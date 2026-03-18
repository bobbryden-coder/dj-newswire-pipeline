"""
Dow Jones Newswire Pipeline
Pulls NML files from sFTP, parses DJNML XML, upserts articles to Supabase.

Feeds:
    EWP3 (primary):  ftpnews1.dowjones.com
    EWP4 (failover): ftpnews2.dowjones.com
    Login: ewplwc / $SFTP_PASSWORD

Environment variables required:
    SFTP_PASSWORD       - Dow Jones sFTP password
    SUPABASE_URL        - Supabase project URL
    SUPABASE_KEY        - Supabase service role key
"""

import os
import re
import gzip
import hashlib
import logging
from io import BytesIO
from html import unescape
from datetime import datetime, timezone

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

SUPABASE_URL  = os.environ["SUPABASE_URL"]
SUPABASE_KEY  = os.environ["SUPABASE_KEY"]

PM_CODES = {"I/GLD", "N/GPC", "N/PCS", "N/SVR", "N/PLM", "I/PPM"}

TABLE = "dj_articles"

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

        body_match = re.search(r'<text>(.*?)</text>', doc, re.DOTALL)
        if not body_match:
            body_match = re.search(r'<body>(.*?)</body>', doc, re.DOTALL)
        body = clean_text(body_match.group(1)) if body_match else None

        source_match = re.search(r'<source[^>]*>([^<]+)</source>', doc)
        source = source_match.group(1).strip() if source_match else "DJN"

        articles.append({
            "id": article_id,
            "headline": headline,
            "body": body,
            "pub_date": pub_date,
            "source": source,
            "codes": list(codes & PM_CODES),
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        })

    return articles


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

    try:
        remote_files = list_remote_files(sftp, SFTP_DIR)
        processed    = get_processed_filenames(supabase)
        new_files    = [f for f in remote_files if f not in processed]

        log.info(f"Remote files: {len(remote_files)} | Already processed: {len(processed)} | New: {len(new_files)}")

        total_articles = 0

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

                mark_file_processed(supabase, filename)
                log.info(f"  → {n} precious metals articles upserted from {filename}")

            except Exception as e:
                log.error(f"Error processing {filename}: {e}")
                continue

        log.info(f"Done. Total articles upserted: {total_articles}")

    finally:
        sftp.close()
        ssh.close()


if __name__ == "__main__":
    run()
