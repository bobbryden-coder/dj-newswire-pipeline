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
from datetime import datetime, timezone

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

PM_CODES = {
    # Gold & precious metals core
    "I/GLD", "N/GPC", "N/PCS", "N/SVR", "N/PLM", "I/PPM",
    # Mining & metals broader coverage
    "I/MNG", "I/ONF", "N/MET", "N/OSME", "N/NMX",
}
TABLE        = "dj_articles"
RETRY_DELAY  = 1
MAX_RETRIES  = 3

SYSTEM_PROMPT = """You are a financial sentiment analyst specialising in precious metals markets including gold, silver, platinum and palladium.

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


def get_processed_filenames(supabase):
    try:
        all_files = set()
        page = 0
        while True:
            result = supabase.table("dj_processed_files").select("filename").range(page * 1000, (page + 1) * 1000 - 1).execute()
            if not result.data:
                break
            all_files.update(row["filename"] for row in result.data)
            if len(result.data) < 1000:
                break
            page += 1
        return all_files
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
    articles = []
    docs = re.findall(r'<doc\b[^>]*>.*?</doc>', nml_content, re.DOTALL)

    for doc in docs:
        codes = set(re.findall(r'<c>([^<]+)</c>', doc))
        if not codes & PM_CODES:
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

        articles.append({
            "id": article_id,
            "accession_number": article_id,
            "sequence_number": sequence_number,
            "headline": headline,
            "body": body,
            "pub_date": pub_date,
            "source": source,
            "codes": list(codes & PM_CODES),
            "sentiment": None,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        })

    return articles


# ── Sentiment Scoring ─────────────────────────────────────────────────────────
def score_article(client, headline, body):
    content = f"Headline: {headline}\n\nBody: {body or ''}"
    for attempt in range(MAX_RETRIES):
        try:
            message = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=10,
                system=SYSTEM_PROMPT,
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


def score_and_update(supabase, client, articles):
    scored = 0
    for article in articles:
        score = score_article(client, article["headline"], article.get("body"))
        if score is not None:
            try:
                supabase.table(TABLE).update({"sentiment": score}).eq("id", article["id"]).execute()
                scored += 1
                log.info(f"  [{score:3d}] {article['headline'][:80]}")
            except Exception as e:
                log.error(f"Sentiment update failed for {article['id']}: {e}")
        time.sleep(RETRY_DELAY)
    return scored


# ── Supabase Upsert ───────────────────────────────────────────────────────────
def upsert_articles(supabase, articles):
    if not articles:
        return 0
    try:
        supabase.table(TABLE).upsert(articles, on_conflict="id").execute()
        return len(articles)
    except Exception as e:
        log.error(f"Upsert failed: {e}")
        return 0


# ── Main ─────────────────────────────────────────────────────────────────────
def run():
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    sftp, ssh = get_sftp_client()

    claude = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY) if ANTHROPIC_API_KEY else None
    if not claude:
        log.warning("ANTHROPIC_API_KEY not set — articles will be upserted without sentiment scores")

    try:
        remote_files = list_remote_files(sftp, SFTP_DIR)
        processed    = get_processed_filenames(supabase)
        new_files    = [f for f in remote_files if f not in processed]

        log.info(f"Remote files: {len(remote_files)} | Already processed: {len(processed)} | New: {len(new_files)}")

        total_articles = 0
        total_scored   = 0

        for filename in new_files:
            remote_path = f"{SFTP_DIR}/{filename}"
            log.info(f"Downloading {filename}...")

            try:
                raw = download_file(sftp, remote_path)

                if filename.endswith(".gz"):
                    raw = gzip.decompress(raw)

                nml_content = raw.decode("utf-8", errors="replace")
                articles = extract_articles(nml_content)
                n = upsert_articles(supabase, articles)
                total_articles += n

                if n > 0 and claude:
                    scored = score_and_update(supabase, claude, articles)
                    total_scored += scored

                mark_file_processed(supabase, filename)
                log.info(f"  → {n} articles upserted, {total_scored} scored so far")

            except Exception as e:
                log.error(f"Error processing {filename}: {e}")
                continue

        log.info(f"Done. Articles upserted: {total_articles} | Scored: {total_scored}")

    finally:
        sftp.close()
        ssh.close()


if __name__ == "__main__":
    run()
