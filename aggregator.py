import os
import sys
from datetime import datetime
from urllib.parse import urlparse
import feedparser
from bs4 import BeautifulSoup
from dateutil import parser as date_parser
from dotenv import load_dotenv
from supabase import create_client, Client

# Bootstrap Environment Config
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("Critical Failure: Core database connection parameters missing from environmental settings.")
    sys.exit(1)

# Initialize Supabase Admin Client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Custom fashion and luxury category keywords dictionaries matching your UI requirements
CATEGORY_KEYWORDS = {
    "diamond_jewelry": ["diamond", "grillz", "iced out", "ring", "pendant", "fine jewelry", "cartier", "vancleef"],
    "branded_clothing": ["saint laurent", "ysl", "balenciaga", "chanel", "runway", "couture", "designer collection", "apparel"],
    "fashion_fame": ["vogue", "editor", "stylist", "break into fashion", "modeling", "bof", "anna wintour"],
    "luxury_investment": ["investment piece", "resale value", "birkin", "hermes", "rolex", "patek", "auction", "sotheby"],
    "affordable_luxury": ["beauty", "fragrance", "lipstick", "luxury entry", "eyewear", "sunglasses", "cardholder", "perfume"]
}

# Industry operational high-end feeds
TARGET_RSS_FEEDS = [
    "https://www.businessoffashion.com/feed",
    "https://wwd.com/feed",
    "https://fashionista.com/.rss/excerpt",
    "https://theindustry.fashion/feed"
]

def clean_and_parse_timestamp(feed_entry) -> str:
    """Safely normalizes incoming text timestamps into valid ISO configurations."""
    raw_time = feed_entry.get("published") or feed_entry.get("updated")
    if raw_time:
        try:
            return date_parser.parse(raw_time).isoformat()
        except Exception:
            pass
    return datetime.utcnow().isoformat()

def identify_target_category(title_string: str, body_string: str) -> str:
    """Scans content across categorization vectors to select exact segment keys."""
    combined_body = f"{title_string} {body_string}".lower()
    for category_key, word_list in CATEGORY_KEYWORDS.items():
        if any(keyword in combined_body for keyword in word_list):
            return category_key
    return None

def extract_all_media_assets(feed_entry, body_html: str) -> list:
    """Extracts images, attachment enclosures, and video paths to store in an array."""
    extracted_urls = []
    if "media_content" in feed_entry:
        for content_item in feed_entry.media_content:
            if "url" in content_item:
                extracted_urls.append(content_item["url"])
    if "enclosures" in feed_entry:
        for enclosure in feed_entry.enclosures:
            if "href" in enclosure:
                extracted_urls.append(enclosure["href"])
    if body_html:
        soup = BeautifulSoup(body_html, "html.parser")
        for img in soup.find_all("img"):
            src_link = img.get("src") or img.get("data-src")
            if src_link and src_link not in extracted_urls:
                extracted_urls.append(src_link)
    if not extracted_urls:
        extracted_urls.append("https://images.unsplash.com/photo-1543872084-c7bd3822856f?auto=format&fit=crop&q=80&w=1000")
    return list(dict.fromkeys(extracted_urls))

def execute_ingestion_pipeline():
    print(f"[{datetime.now()}] Activating pipeline aggregation cycle...")
    for current_url in TARGET_RSS_FEEDS:
        origin_domain = urlparse(current_url).netloc
        try:
            parsed_feed = feedparser.parse(current_url)
            for entry in parsed_feed.entries:
                headline = entry.get("title", "").strip()
                raw_summary = entry.get("summary", "") or entry.get("description", "")
                selected_cat = identify_target_category(headline, raw_summary)
                if not selected_cat:
                    continue
                article_url = entry.get("link")
                if not article_url:
                    continue
                clean_preview_text = BeautifulSoup(raw_summary, "html.parser").get_text()
                if len(clean_preview_text) > 260:
                    clean_preview_text = clean_preview_text[:257] + "..."
                
                payload = {
                    "source": origin_domain,
                    "title": headline,
                    "url": article_url,
                    "excerpt": clean_preview_text,
                    "body": raw_summary,
                    "media_assets": extract_all_media_assets(entry, raw_summary),
                    "category": selected_cat,
                    "is_original": False,
                    "is_advertisement": False,
                    "published_at": clean_and_parse_timestamp(entry)
                }
                try:
                    supabase.table("articles").insert(payload).execute()
                    print(f" -> Committed live trend entry: {headline[:35]}...")
                except Exception as insert_error:
                    if "duplicate key" in str(insert_error).lower():
                        continue
                    print(f"Database sync error: {insert_error}")
        except Exception as execution_failure:
            print(f"Failed parsing feed source {current_url}: {execution_failure}")

if __name__ == "__main__":
    execute_ingestion_pipeline()