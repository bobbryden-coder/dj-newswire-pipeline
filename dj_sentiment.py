"""
DJ Newswire Sentiment Scorer
Fetches unscored articles from Supabase, scores each using Claude API (0-100),
and updates the sentiment field.

Environment variables required:
    SUPABASE_URL        - Supabase project URL
    SUPABASE_KEY        - Supabase service role key
    ANTHROPIC_API_KEY   - Anthropic API key
"""

import os
import time
import logging
import anthropic
from datetime import datetime, timezone
from supabase import create_client

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────────────────────
SUPABASE_URL      = os.environ["SUPABASE_URL"]
SUPABASE_KEY      = os.environ["SUPABASE_KEY"]
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]

TABLE             = "dj_articles"
BATCH_SIZE        = 50        # articles per run
RETRY_DELAY       = 2         # seconds between API calls
MAX_RETRIES       = 3

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


def get_unscored_articles(supabase):
    try:
        result = (
            supabase.table(TABLE)
            .select("id, headline, body")
            .is_("sentiment", "null")
            .limit(BATCH_SIZE)
            .execute()
        )
        return result.data
    except Exception as e:
        log.error(f"Failed to fetch unscored articles: {e}")
        return []


def update_sentiment(supabase, article_id, score):
    try:
        supabase.table(TABLE).update({"sentiment": score}).eq("id", article_id).execute()
    except Exception as e:
        log.error(f"Failed to update sentiment for {article_id}: {e}")


def run():
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    client   = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    articles = get_unscored_articles(supabase)
    log.info(f"Found {len(articles)} unscored articles")

    if not articles:
        log.info("Nothing to score.")
        return

    scored = 0
    for article in articles:
        score = score_article(client, article["headline"], article.get("body"))
        if score is not None:
            update_sentiment(supabase, article["id"], score)
            scored += 1
            log.info(f"  [{score:3d}] {article['headline'][:80]}")
        else:
            log.warning(f"  [---] Failed to score: {article['headline'][:80]}")
        time.sleep(RETRY_DELAY)

    log.info(f"Done. Scored {scored}/{len(articles)} articles.")


if __name__ == "__main__":
    run()
