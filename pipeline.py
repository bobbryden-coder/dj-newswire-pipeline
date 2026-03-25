"""
Dow Jones Newswire Pipeline
Pulls NML files from sFTP, parses DJNML XML, upserts articles to Supabase,
and scores each article with Claude immediately on ingest.

Feeds:
    EWP3 (primary):  ftpnews1.dowjones.com
    EWP4 (failover): ftpnews2.dowjones.com
    Login: ewplwc / $SFTP_PASSWORD

Environment variables required:
    SFTP_PASSWORD       - Dow Jones sFTP password
    SUPABASE_URL        - Supabase project URL
    SUPABASE_KEY        - Supabase service role key
    ANTHROPIC_API_KEY   - Anthropic API key (optional — skips scoring if missing)
"""

import os
import re
import gzip
import time
import hashlib
import logging
from io import BytesIO
from html import unescape
from datetime import datetime, timedelta, timezone

import anthropic
import paramiko
from supabase import create_client, Client

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────────────────────
SFTP_HOSTS = [
    "ftpnews1.dowjones.com",   # EWP3 primary
    "ftpnews2.dowjones.com",   # EWP4 failover
]
SFTP_PORT     = 22
SFTP_USER     = "ewplwc"
SFTP_PASSWORD = os.environ["SFTP_PASSWORD"]
SFTP_DIR      = "."

SUPABASE_URL      = os.environ["SUPABASE_URL"]
SUPABASE_KEY      = os.environ["SUPABASE_KEY"]
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")

# ── Commodity Code Sets ───────────────────────────────────────────────────────
PM_CODES = {
    # Gold & precious metals core
    "I/GLD", "N/GPC", "N/PCS", "N/SVR", "N/PLM", "I/PPM",
    # Mining & metals broader coverage
    "I/MNG", "I/ONF", "N/MET", "N/OSME", "N/NMX",
}

OIL_CODES = {
    "N/PET",    # Crude Oil & Petroleum Products
    "N/CMKT",   # Crude Market Commentary
    "N/EGY",    # Energy Commentary
    "N/OPC",    # OPEC & OPEC+
    "N/PRD",    # Oil & Natural Gas Production
    "N/RMKT",   # Refined Products Spot Market Commentary
    "I/OIL",    # Major Oil & Natural Gas Companies
    "I/OIS",    # Upstream Oil & Gas
    "N/NRG",    # Dow Jones Energy Service
}

ALL_CODES = PM_CODES | OIL_CODES

RETRY_DELAY = 1
MAX_RETRIES = 3

# ── Sentiment Prompts ─────────────────────────────────────────────────────────
GOLD_PROMPT = """You are a financial sentiment analyst specialising in precious metals markets including gold, silver, platinum and palladium.

You will be given a news article headline and body. Score the sentiment of the article from the perspective of a precious metals trader on a scale of 0 to 100 where:

0-20   = Very bearish (strong negative price pressure, major sell-off, severe negative news)
21-40  = Bearish (negative tone, headwinds, mild selling pressure)
41-59  = Neutral (balanced, factual, no clear directional bias)
60-79  = Bullish (positive tone, tailwinds, mild buying pressure)
80-100 = Very bullish (strong positive price pressure, major rally, severe positive news)

Consider:
- Price movements mentioned (rising = bullish, falling = bearish)
- Demand signals (ETF inflows, central bank buying = bullish)
- Supply signals (mine output increases = bearish)
- Macro context (dollar weakness, inflation = bullish for gold)
- Risk sentiment (risk-off = bullish for gold)
- Company news (positive earnings, new discoveries = bullish)

Respond with ONLY a single integer between 0 and 100. No explanation, no text, just the number."""

OIL_PROMPT = """You are a financial sentiment analyst specialising in crude oil and energy markets.

You will be given a news article headline and body. Score the sentiment of the article from the perspective of a crude oil trader on a scale of 0 to 100 where:

0-20   = Very bearish (strong negative price pressure, major sell-off, severe negative news)
21-40  = Bearish (negative tone, headwinds, oversupply, demand destruction)
41-59  = Neutral (balanced, factual, no clear directional bias)
60-79  = Bullish (positive tone, supply cuts, demand growth, geopolitical risk premium)
80-100 = Very bullish (strong positive price pressure, major supply shock, severe positive news)

Consider:
- Price movements mentioned (rising = bullish, falling = bearish)
- OPEC+ production decisions (cuts = bullish, increases = bearish)
- Inventory data (draws = bullish, builds = bearish)
- Demand signals (economic growth, travel demand = bullish)
- Geopolitical risk (Middle East tensions, sanctions = bullish)
- Supply disruptions (pipeline outages, hurricanes = bullish)
- Refinery margins and product demand

Respond with ONLY a single integer between 0 and 100. No explanation, no text, just the number."""


# ── sFTP Connection ───────────────────────────────────────────────────────────
def get_sftp_client():
    for host in SFTP_HOSTS:
        try:
            log.info(f"Connecting to {host}...")
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(host, port=SFTP_PORT, username=SFTP_USER, password=SFTP_PASSWORD, timeout=30)
            sftp = ssh.open_sftp()
            log.info(f"Connected to {host}")
            return sftp, ssh
        except Exception as e:
            log.warning(f"Failed to connect to {host}: {e}")
    raise ConnectionError("All sFTP hosts failed")


def list_remote_files(sftp, remote_dir):
    try:
        files = sftp.listdir(remote_dir)
        return [f for f in files if f.endswith(".nml") or f.endswith(".nml.gz") or f.endswith(".NML")]
    except Exception as e:
        log.error(f"Could not list {remote_dir}: {e}")
        return []


def get_processed_filenames(supabase, remote_files):
    """Only check if the remote files we care about have been processed."""
    try:
        if not remote_files:
            return set()
        processed = set()
        # Check in batches of 500
        batch_size = 500
        for i in range(0, len(remote_files), batch_size):
            batch = remote_files[i:i + batch_size]
            result = supabase.table("dj_processed_files").select("filename").in_("filename", batch).execute()
            if result.data:
                processed.update(row["filename"] for row in result.data)
        return processed
    except Exception as e:
        log.warning(f"Could not fetch processed files: {e}")
        return set()


def mark_file_processed(supabase, filename):
    supabase.table("dj_processed_files").upsert(
        {"filename": filename, "processed_at": datetime.now(timezone.utc).isoformat()}
    ).execute()


def download_file(sftp, remote_path):
    buf = BytesIO()
    sftp.getfo(remote_path, buf)
    return buf.getvalue()


# ── DJNML Parser ─────────────────────────────────────────────────────────────
def clean_text(raw):
    text = re.sub(r'<!\[CDATA\[', '', raw)
    text = re.sub(r'\]\]>', '', text)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = unescape(text)
    return re.sub(r'\s+', ' ', text).strip()


def extract_articles(nml_content):
    """Extract articles and classify as gold, oil, or both."""
    gold_articles = []
    oil_articles = []

    docs = re.findall(r'<doc\b[^>]*>.*?</doc>', nml_content, re.DOTALL)

    for doc in docs:
        codes = set(re.findall(r'<c>([^<]+)</c>', doc))
        
        is_gold = bool(codes & PM_CODES)
        is_oil = bool(codes & OIL_CODES)
        
        if not is_gold and not is_oil:
            continue

        hl_match = re.search(r'<headline[^>]*>(.*?)</headline>', doc, re.DOTALL)
        headline = clean_text(hl_match.group(1)) if hl_match else None
        if not headline:
            continue

        date_match = re.search(r'display-date="([^"]+)"', doc)
        if not date_match:
            date_match = re.search(r'<publication-date>([^<]+)</publication-date>', doc)
        pub_date = date_match.group(1).strip() if date_match else None

        acc_match = re.search(r'accession-number="([^"]+)"', doc)
        article_id = acc_match.group(1) if acc_match else hashlib.md5(f"{headline}{pub_date}".encode()).hexdigest()

        seq_match = re.search(r'\bseq="(\d+)"', doc)
        sequence_number = int(seq_match.group(1)) if seq_match else None

        body_match = re.search(r'<text>(.*?)</text>', doc, re.DOTALL)
        if not body_match:
            body_match = re.search(r'<body>(.*?)</body>', doc, re.DOTALL)
        body = clean_text(body_match.group(1)) if body_match else None

        source_match = re.search(r'<source[^>]*>([^<]+)</source>', doc)
        source = source_match.group(1).strip() if source_match else "DJN"

        base = {
            "id": article_id,
            "accession_number": article_id,
            "sequence_number": sequence_number,
            "headline": headline,
            "body": body,
            "pub_date": pub_date,
            "source": source,
            "sentiment": None,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }

        if is_gold:
            gold_articles.append({**base, "codes": list(codes & PM_CODES)})

        if is_oil:
            oil_articles.append({**base, "codes": list(codes & OIL_CODES)})

    return gold_articles, oil_articles


# ── Sentiment Scoring ─────────────────────────────────────────────────────────
def score_article(client, headline, body, system_prompt):
    content = f"Headline: {headline}\n\nBody: {body or ''}"
    for attempt in range(MAX_RETRIES):
        try:
            message = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=10,
                system=system_prompt,
                messages=[{"role": "user", "content": content}]
            )
            score_text = message.content[0].text.strip()
            score = int(score_text)
            if 0 <= score <= 100:
                return score
            log.warning(f"Score out of range: {score}")
            return None
        except ValueError:
            log.warning(f"Non-integer response: {score_text}")
            return None
        except Exception as e:
            log.warning(f"API error (attempt {attempt + 1}): {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
    return None


def score_and_upsert(supabase, client, articles, table, system_prompt):
    """Score articles and upsert with sentiment to the correct table."""
    if not articles:
        return 0, 0

    # First upsert without sentiment
    try:
        supabase.table(table).upsert(articles, on_conflict="id").execute()
    except Exception as e:
        log.error(f"Upsert failed for {table}: {e}")
        return 0, 0

    upserted = len(articles)
    scored = 0

    if client:
        for article in articles:
            score = score_article(client, article["headline"], article.get("body"), system_prompt)
            if score is not None:
                try:
                    supabase.table(table).update({"sentiment": score}).eq("id", article["id"]).execute()
                    scored += 1
                    log.info(f"  [{table[:8]}][{score:3d}] {article['headline'][:70]}")
                except Exception as e:
                    log.error(f"Sentiment update failed for {article['id']}: {e}")
            time.sleep(RETRY_DELAY)

    return upserted, scored


# ── Main ─────────────────────────────────────────────────────────────────────
def run():
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    sftp, ssh = get_sftp_client()

    claude = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY) if ANTHROPIC_API_KEY else None
    if not claude:
        log.warning("ANTHROPIC_API_KEY not set — articles will be upserted without sentiment scores")

    try:
        remote_files = list_remote_files(sftp, SFTP_DIR)
        processed    = get_processed_filenames(supabase, remote_files)
        new_files    = [f for f in remote_files if f not in processed]

        log.info(f"Remote files: {len(remote_files)} | Already processed: {len(processed)} | New: {len(new_files)}")

        total_gold = total_oil = total_scored = 0

        for filename in new_files:
            remote_path = f"{SFTP_DIR}/{filename}"
            log.info(f"Downloading {filename}...")

            try:
                raw = download_file(sftp, remote_path)
                if filename.endswith(".gz"):
                    raw = gzip.decompress(raw)

                nml_content = raw.decode("utf-8", errors="replace")
                gold_articles, oil_articles = extract_articles(nml_content)

                g_upserted, g_scored = score_and_upsert(supabase, claude, gold_articles, "dj_articles", GOLD_PROMPT)
                o_upserted, o_scored = score_and_upsert(supabase, claude, oil_articles, "dj_oil_articles", OIL_PROMPT)

                total_gold += g_upserted
                total_oil += o_upserted
                total_scored += g_scored + o_scored

                mark_file_processed(supabase, filename)
                log.info(f"  → gold: {g_upserted} | oil: {o_upserted} | scored: {g_scored + o_scored}")

            except Exception as e:
                log.error(f"Error processing {filename}: {e}")
                continue

        log.info(f"Done. Gold: {total_gold} | Oil: {total_oil} | Scored: {total_scored}")

    finally:
        sftp.close()
        ssh.close()


if __name__ == "__main__":
    run()
