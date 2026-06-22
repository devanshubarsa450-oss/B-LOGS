import os
import sys
import re
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

# Custom fashion and luxury category keywords dictionaries
CATEGORY_KEYWORDS = {
    # UPDATE 1: Moved super_cars to the very top so it filters cars BEFORE jewelry
    "super_cars": [
        "supercar", "hypercar", "ferrari", "lamborghini", "mclaren", "bugatti", "pagani", 
        "koenigsegg", "aston martin", "porsche 911", "v12", "exotic car", "gt3"
    ],
    "diamond_jewelry": [
        "diamond", "grillz", "iced out", "ring", "pendant", "fine jewelry", "cartier", 
        "vancleef", "jewelry", "jewellery", "watch", "watches", "gold", "silver", "patek", "rolex", "gem"
    ],
    "branded_clothing": [
        "saint laurent", "ysl", "balenciaga", "chanel", "runway", "couture", "designer collection", 
        "apparel", "clothing", "dress", "jacket", "coat", "suit", "brand", "label", "ready-to-wear", 
        "rtw", "fashion", "collection", "show", "week", "luxury", "haute", "garment", "louis vuitton", "prada"
    ],
    "fashion_fame": [
        "vogue", "editor", "stylist", "break into fashion", "modeling", "bof", "anna wintour", 
        "industry", "career", "model", "magazine", "celebrity", "red carpet", "creative director", "appointment"
    ],
    "luxury_investment": [
        "investment piece", "resale value", "birkin", "hermes", "auction", "sotheby", "market", 
        "sales", "revenue", "retail", "business", "growth", "report", "stocks", "investment", "quarter", "earnings"
    ],
    "affordable_luxury": [
        "beauty", "fragrance", "lipstick", "luxury entry", "eyewear", "sunglasses", "cardholder", 
        "perfume", "makeup", "skincare", "bag", "wallet", "shoes", "sneakers", "streetwear", "accessories"
    ]
}

# Industry operational high-end feeds
TARGET_RSS_FEEDS = [
    # Fashion & Luxury Business Feeds
    "https://www.businessoffashion.com/feed",
    "https://wwd.com/feed",
    "https://fashionista.com/.rss/excerpt",
    "https://theindustry.fashion/feed",
    
    # Supercar Feeds
    "https://www.thesupercarblog.com/feed/",
    "https://gtspirit.com/feed/",
    "https://www.supercars.net/blog/feed/"
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

# UPDATE 2: Upgraded function to use Regex word boundaries (\b)
def identify_target_category(title_string: str, body_string: str) -> str:
    """Scans content across categorization vectors using EXACT word matching to prevent substring errors."""
    combined_body = f"{title_string} {body_string}".lower()
    
    for category_key, word_list in CATEGORY_KEYWORDS.items():
        for keyword in word_list:
            # \b ensures it only matches the exact isolated word (e.g. "ring" but not "steering")
            if re.search(r'\b' + re.escape(keyword) + r'\b', combined_body):
                return category_key
                
    return None

def extract_best_image(entry) -> str:
    """Hunts down images hidden in messy RSS feeds, bypassing lazy-loaders and tracking pixels."""
    
    # 1. Try standard 'media_content'
    if 'media_content' in entry and len(entry.media_content) > 0:
        return entry.media_content[0].get('url')
    
    # 2. Try 'media_thumbnail'
    if 'media_thumbnail' in entry and len(entry.media_thumbnail) > 0:
        return entry.media_thumbnail[0].get('url')
        
    # 3. Try 'enclosures' and 'links' (Standard for many blogs)
    if 'enclosures' in entry and len(entry.enclosures) > 0:
        for enc in entry.enclosures:
            if 'image' in enc.get('type', '') or enc.get('href', '').endswith(('.jpg', '.png', '.jpeg', '.webp')):
                return enc.get('href')

    if 'links' in entry:
        for link in entry.links:
            if 'image' in link.get('type', '') or link.get('rel') == 'enclosure':
                return link.get('href')
                
    # 4. Search deep inside both the summary AND the full article content
    html_blocks = []
    if 'summary' in entry:
        html_blocks.append(entry.summary)
    if 'content' in entry and len(entry.content) > 0:
        html_blocks.append(entry.content[0].value)

    for html_content in html_blocks:
        if not html_content:
            continue
            
        soup = BeautifulSoup(html_content, "html.parser")
        
        # Find all images
        for img in soup.find_all("img"):
            # Publishers like WWD use 'lazy loading' which hides the real image in data attributes.
            # We must check data-src and data-lazy-src BEFORE standard src!
            src_link = img.get("data-src") or img.get("data-lazy-src") or img.get("src")
            
            # Reject 1x1 tracking pixels or empty links
            if src_link and "1x1" not in src_link and "pixel" not in src_link.lower():
                return src_link
                
        # 5. Ultimate Fallback: Raw Regex on the text block looking for lazy loaded attributes
        img_match = re.search(r'<img[^>]+(?:data-src|data-lazy-src|src)=["\'](.*?)["\']', html_content)
        if img_match:
            # Quick check to avoid picking up a tracking pixel from regex
            if "1x1" not in img_match.group(1):
                return img_match.group(1)
        
    # Return None if absolutely no image exists; frontend will apply premium Unsplash fallback
    return None

def execute_ingestion_pipeline():
    print(f"\n[{datetime.now()}] Activating pipeline aggregation cycle...")
    total_committed = 0
    total_skipped = 0
    total_duplicates = 0

    for current_url in TARGET_RSS_FEEDS:
        origin_domain = urlparse(current_url).netloc
        # Clean domain names for a premium aesthetic
        if origin_domain.startswith("www."):
            origin_domain = origin_domain[4:]

        print(f"\n📡 Connecting to feed domain stream: {origin_domain}")
        try:
            parsed_feed = feedparser.parse(current_url)
            print(f" -> Found {len(parsed_feed.entries)} entries to evaluate.")
            
            for entry in parsed_feed.entries:
                headline = entry.get("title", "").strip()
                
                # Fetch both summary and the deeper content chunk for logic matching
                raw_summary = entry.get("summary", "") or entry.get("description", "")
                deep_content = entry.get('content', [{'value': ''}])[0].get('value')
                logic_text = raw_summary + " " + deep_content
                
                # Run content through our intelligent matching filter matrix
                selected_cat = identify_target_category(headline, logic_text)
                if not selected_cat:
                    total_skipped += 1
                    continue
                
                article_url = entry.get("link")
                if not article_url:
                    continue
                
                clean_preview_text = BeautifulSoup(raw_summary, "html.parser").get_text().strip()
                if len(clean_preview_text) > 260:
                    clean_preview_text = clean_preview_text[:257] + "..."
                if not clean_preview_text:
                    clean_preview_text = "Explore the full coverage detailing this season's latest design shift directly at the official publisher link."

                # Find the best possible image URL using the new ultra-smart function
                found_image_url = extract_best_image(entry)

                # PERFECTLY MATCHED PAYLOAD FOR SUPABASE & FRONTEND
                payload = {
                    "source": origin_domain,
                    "title": headline,
                    "url": article_url,
                    "excerpt": clean_preview_text,
                    "summary": clean_preview_text, 
                    "body": raw_summary,
                    "image_url": found_image_url, 
                    "category": selected_cat,
                    "is_original": False,
                    "is_advertisement": False,
                    "published_at": clean_and_parse_timestamp(entry)
                }
                
                try:
                    supabase.table("articles").insert(payload).execute()
                    print(f"   ✅ Committed [{selected_cat}]: {headline[:45]}...")
                    total_committed += 1
                except Exception as insert_error:
                    if "duplicate key" in str(insert_error).lower():
                        total_duplicates += 1
                        continue
                    print(f"   ❌ Database sync error: {insert_error}")
        except Exception as execution_failure:
            print(f" 💥 Failed parsing feed source {current_url}: {execution_failure}")

    print(f"\n┌──────────────────────────────────────────────┐")
    print(f"│         AGGREGATION LOOP COMPLETE SUMMARY     │")
    print(f"├──────────────────────────────────────────────┤")
    print(f"│  • New Trends Injected: {total_committed:<20} │")
    print(f"│  • Existing Duplicates: {total_duplicates:<20} │")
    print(f"│  • Unrelated/Skipped:  {total_skipped:<20} │")
    print(f"└──────────────────────────────────────────────┘\n")

if __name__ == "__main__":
    execute_ingestion_pipeline()