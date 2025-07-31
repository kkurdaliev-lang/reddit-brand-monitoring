import asyncio
# import aiohttp  # Commented out due to Python 3.13 compatibility issues
import sqlite3
import sys
import praw
import prawcore
import requests
import feedparser
import time
import re
import json
import logging
import threading
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Set, Optional
from dataclasses import dataclass, asdict
from flask import Flask, render_template_string, jsonify, request, send_file
import os
from contextlib import contextmanager
import tempfile
import csv
import io
import random
from collections import deque, defaultdict
import random
from collections import deque, defaultdict

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
CONFIG = {
    'database_file': os.getenv('DATABASE_PATH', '/app/data/reddit_monitor.db'),
    'reddit': {
        'client_id': os.getenv('REDDIT_CLIENT_ID', ''),
        'client_secret': os.getenv('REDDIT_CLIENT_SECRET', ''),
        'user_agent': 'AllInOneBrandMonitor/1.0'
    },
    'groq_api_token': os.getenv('GROQ_API_TOKEN', ''),
    'brands': {
        'badinka': r'[@#]?badinka(?:\.com)?',
        'devilwalking': r'[@#]?devil\s*walking(?:\.com)?',  # Matches both "devilwalking" and "devil walking"
        # Add more brands here if needed:
        # 'rave_fashion': r'[@#]?rave\s*fashion',
        # 'festival_outfit': r'[@#]?festival\s*outfit',
    },
    'monitor_all_reddit': True,  # Monitor all of Reddit, not just specific subreddits
    'focused_subreddits': [
        # High-priority subreddits for extra coverage
        "Rezz", "aves", "ElectricForest", "sewing", "avesfashion",
        "cyber_fashion", "aveoutfits", "RitaFourEssenceSystem", "SoftDramatics", "Shein",
        "avesNYC", "veld", "BADINKA", "PlusSize",
        "LostLandsMusicFest", "festivals", "avefashion", "avesafe", "EDCOrlando",
        "findfashion", "BassCanyon", "Aerials", "electricdaisycarnival", "bonnaroo",
        "Tomorrowland", "femalefashion", "Soundhaven", "warpedtour", "Shambhala",
        "Lollapalooza", "EDM", "BeyondWonderland", "kandi"
    ],
    'subreddits': [
        # Fallback subreddits
        "all", "popular", "announcements"
    ],
    'port': int(os.getenv('PORT', 5000))
}

@dataclass
class Mention:
    id: str
    type: str  # 'post', 'comment'
    title: Optional[str]
    body: Optional[str]
    permalink: str
    created: str
    subreddit: str
    author: str
    score: int
    sentiment: Optional[str]
    brand: str
    source: str

class DatabaseManager:
    def __init__(self, db_file: str):
        self.db_file = db_file
        self._init_db()
    
    def _init_db(self):
        """Initialize SQLite database with tables"""
        with self.get_connection() as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS mentions (
                    id TEXT PRIMARY KEY,
                    type TEXT NOT NULL,
                    title TEXT,
                    body TEXT,
                    permalink TEXT NOT NULL,
                    created TIMESTAMP NOT NULL,
                    subreddit TEXT NOT NULL,
                    author TEXT NOT NULL,
                    score INTEGER,
                    sentiment TEXT,
                    brand TEXT NOT NULL,
                    source TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create indexes for better performance
            conn.execute('CREATE INDEX IF NOT EXISTS idx_brand ON mentions(brand)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_created ON mentions(created)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_subreddit ON mentions(subreddit)')
            conn.commit()
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = sqlite3.connect(self.db_file)
        try:
            yield conn
        finally:
            conn.close()
    
    def insert_mentions(self, mentions: List[Mention]):
        """Insert multiple mentions, handling duplicates"""
        if not mentions:
            return
        
        with self.get_connection() as conn:
            for mention in mentions:
                conn.execute('''
                    INSERT OR REPLACE INTO mentions 
                    (id, type, title, body, permalink, created, subreddit, author, score, sentiment, brand, source)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    mention.id, mention.type, mention.title, mention.body,
                    mention.permalink, mention.created, mention.subreddit,
                    mention.author, mention.score, mention.sentiment,
                    mention.brand, mention.source
                ))
            conn.commit()
            logger.info(f"Inserted {len(mentions)} mentions into database")
    
    def get_existing_ids(self) -> Set[str]:
        """Get set of existing mention IDs to avoid duplicates"""
        with self.get_connection() as conn:
            cursor = conn.execute("SELECT id FROM mentions")
            return set(row[0] for row in cursor.fetchall())
    
    def get_core_reddit_ids(self) -> Set[str]:
        """Extract core Reddit IDs from various formats to prevent duplicates"""
        with self.get_connection() as conn:
            cursor = conn.execute("SELECT id FROM mentions")
            core_ids = set()
            for row in cursor.fetchall():
                raw_id = row[0]
                # Extract core Reddit ID from different formats
                if raw_id.startswith('json_'):
                    # Handle old format: json_n3z93h6_badinka -> n3z93h6
                    parts = raw_id.split('_')
                    if len(parts) >= 2:
                        core_id = parts[1]  # Extract the Reddit ID part
                        core_ids.add(core_id)
                else:
                    # Handle direct Reddit IDs: n3z93h6 -> n3z93h6
                    core_ids.add(raw_id)
            return core_ids
    
    def get_existing_content_hashes(self) -> Set[str]:
        """Get set of content hashes to detect duplicates by content"""
        with self.get_connection() as conn:
            cursor = conn.execute("SELECT body, brand, subreddit FROM mentions")
            hashes = set()
            for row in cursor.fetchall():
                body, brand, subreddit = row
                # Create a content-based hash for duplicate detection
                content_hash = f"{brand}_{subreddit}_{hash(body[:100])}"
                hashes.add(content_hash)
            return hashes

class ErrorTracker:
    """Track API errors and implement smart backoff strategies"""
    def __init__(self):
        self.error_counts = defaultdict(int)
        self.last_error_time = defaultdict(float)
        self.circuit_breaker_until = defaultdict(float)
        self.backoff_delays = defaultdict(float)
    
    def record_error(self, source: str, error_type: str = "500"):
        """Record an error for exponential backoff calculation"""
        current_time = time.time()
        self.error_counts[source] += 1
        self.last_error_time[source] = current_time
        
        # Exponential backoff: 2^errors * base_delay (max 300 seconds)
        base_delay = 30
        self.backoff_delays[source] = min(300, base_delay * (2 ** min(self.error_counts[source] - 1, 4)))
        
        # Circuit breaker: if too many errors in short time, break circuit
        if self.error_counts[source] >= 5:
            self.circuit_breaker_until[source] = current_time + 600  # 10 minutes
            logger.warning(f"🚨 Circuit breaker activated for {source} - cooling down for 10 minutes")
        
        logger.warning(f"📊 Error recorded for {source}: count={self.error_counts[source]}, backoff={self.backoff_delays[source]}s")
    
    def record_success(self, source: str):
        """Record a successful request to reset error tracking"""
        if source in self.error_counts and self.error_counts[source] > 0:
            logger.info(f"✅ Success recorded for {source} - resetting error count from {self.error_counts[source]} to 0")
            self.error_counts[source] = 0
            self.backoff_delays[source] = 0
    
    def should_skip_request(self, source: str) -> tuple[bool, float]:
        """Check if we should skip a request due to errors"""
        current_time = time.time()
        
        # Check circuit breaker
        if current_time < self.circuit_breaker_until.get(source, 0):
            remaining = self.circuit_breaker_until[source] - current_time
            return True, remaining
        
        # Check backoff delay
        if current_time < (self.last_error_time.get(source, 0) + self.backoff_delays.get(source, 0)):
            remaining = (self.last_error_time[source] + self.backoff_delays[source]) - current_time
            return True, remaining
        
        return False, 0
    
    def get_delay_with_jitter(self, source: str) -> float:
        """Get delay with jitter to avoid thundering herd"""
        base_delay = self.backoff_delays.get(source, 0)
        if base_delay == 0:
            return 0
        # Add 20% jitter
        jitter = base_delay * 0.2 * random.random()
        return base_delay + jitter

class FailedRequestQueue:
    """Queue and retry failed requests"""
    def __init__(self, max_size: int = 1000):
        self.queue = deque(maxlen=max_size)
        self.processing = False
    
    def add_failed_request(self, request_info: dict):
        """Add a failed request to retry queue"""
        request_info['retry_count'] = request_info.get('retry_count', 0) + 1
        request_info['queued_at'] = time.time()
        
        # Only retry up to 3 times
        if request_info['retry_count'] <= 3:
            self.queue.append(request_info)
            logger.info(f"📝 Queued failed request: {request_info['type']} (attempt {request_info['retry_count']})")
        else:
            logger.warning(f"❌ Dropping request after 3 failed attempts: {request_info['type']}")
    
    def get_next_request(self) -> Optional[dict]:
        """Get next request to retry"""
        if not self.queue:
            return None
        
        # Get oldest request
        request = self.queue.popleft()
        
        # Check if request is too old (older than 1 hour)
        if time.time() - request['queued_at'] > 3600:
            logger.warning(f"⏰ Dropping stale request: {request['type']}")
            return self.get_next_request()  # Try next request
        
        return request
    
    def size(self) -> int:
        return len(self.queue)

class RSSBackupMonitor:
    """RSS-based backup monitoring that activates during API failures"""
    def __init__(self, brands: dict, db: DatabaseManager, sentiment: 'SentimentAnalyzer'):
        self.brands = brands
        self.db = db
        self.sentiment = sentiment
        self.running = False
        self.seen_rss_ids = set()
        self.active = False  # Only active when primary systems fail
    
    def activate(self):
        """Activate RSS backup monitoring"""
        if not self.active:
            self.active = True
            logger.info("🔄 RSS backup monitoring ACTIVATED")
    
    def deactivate(self):
        """Deactivate RSS backup monitoring"""
        if self.active:
            self.active = False
            logger.info("✅ RSS backup monitoring DEACTIVATED - primary systems recovered")
    
    def monitor_rss_feeds(self):
        """Monitor RSS feeds as backup"""
        logger.info("📡 RSS backup monitor started")
        
        while self.running:
            try:
                if not self.active:
                    time.sleep(30)  # Check every 30 seconds if we should activate
                    continue
                
                # Monitor key RSS endpoints
                rss_urls = [
                    "https://www.reddit.com/r/all/new/.rss?limit=100",
                    "https://www.reddit.com/r/all/comments/.rss?limit=100"
                ]
                
                for url in rss_urls:
                    if not self.running or not self.active:
                        break
                    
                    try:
                        response = requests.get(url, timeout=15)
                        if response.status_code == 200:
                            self._process_rss_content(response.text, url)
                            logger.debug(f"✅ RSS backup processed: {url}")
                        else:
                            logger.warning(f"❌ RSS backup failed: {url} - status {response.status_code}")
                    except Exception as e:
                        logger.error(f"❌ RSS backup error for {url}: {e}")
                    
                    time.sleep(10)  # Delay between RSS feeds
                
                time.sleep(60)  # Check RSS every minute when active
                
            except Exception as e:
                logger.error(f"❌ RSS backup monitoring error: {e}")
                time.sleep(60)
    
    def _process_rss_content(self, rss_text: str, source_url: str):
        """Process RSS content for brand mentions"""
        try:
            feed = feedparser.parse(rss_text)
            mentions_found = 0
            
            for entry in feed.entries:
                if not self.running or not self.active:
                    break
                
                # Extract ID from entry
                entry_id = entry.get('id', entry.get('link', ''))
                if entry_id in self.seen_rss_ids:
                    continue
                
                # Check for brand mentions
                title = entry.get('title', '')
                summary = entry.get('summary', '')
                full_text = f"{title} {summary}"
                
                for brand_name, brand_pattern in self.brands.items():
                    if brand_pattern.search(full_text):
                        # Found a brand mention via RSS backup!
                        try:
                            sentiment = self.sentiment.analyze(full_text[:400], brand_name)
                        except:
                            sentiment = "neutral"
                        
                        mention = Mention(
                            id=f"rss_backup_{hash(entry_id)}_{brand_name}",
                            type="rss_backup",
                            title=title,
                            body=summary,
                            permalink=entry.get('link', ''),
                            created=datetime.now(timezone.utc).isoformat(),
                            subreddit="unknown",
                            author=entry.get('author', 'unknown'),
                            score=0,
                            sentiment=sentiment,
                            brand=brand_name,
                            source="rss_backup"
                        )
                        
                        self.db.insert_mentions([mention])
                        mentions_found += 1
                        logger.info(f"🔄 RSS backup found: {brand_name} mention via {source_url}")
                
                self.seen_rss_ids.add(entry_id)
            
            if mentions_found > 0:
                logger.info(f"✅ RSS backup processed {mentions_found} mentions from {source_url}")
                
        except Exception as e:
            logger.error(f"❌ RSS content processing error: {e}")

class SentimentAnalyzer:
    def __init__(self, api_token: str):
        self.api_token = api_token
        self.api_url = "https://api.groq.com/openai/v1/chat/completions"
        logger.info(f"🤖 Groq sentiment analyzer initialized with URL: {self.api_url}")
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_token}"
        } if api_token else {}
    
    def analyze(self, context_text: str, brand_name: str = "the brand") -> str:
        """Analyze sentiment specifically toward a brand using Groq"""
        logger.info(f"🔍 Starting Groq brand sentiment analysis for '{brand_name}' in text: '{context_text[:100]}...'")
        
        if not self.api_token:
            logger.warning("❌ No Groq API token provided")
            return "neutral"
            
        if not context_text.strip():
            logger.warning("❌ Empty text provided for sentiment analysis")
            return "neutral"
        
        try:
            # Create brand-focused prompt
            prompt = f"""Analyze the sentiment specifically about the brand '{brand_name}' in this text:

"{context_text[:500]}"

Focus ONLY on opinions, experiences, or feelings about this specific brand. Ignore general complaints or negative words that don't relate to the brand itself.

For example:
- "I ordered from {brand_name} and had zero issues, great quality" = positive (customer satisfied with brand)
- "Shipping took forever but {brand_name} quality is amazing" = positive (complaint about shipping, not brand)
- "{brand_name} clothes are overpriced and poor quality" = negative (directly about brand)

Respond with ONLY ONE WORD: positive, negative, or neutral"""

            payload = {
                "model": "llama-3.1-8b-instant",
                "messages": [
                    {
                        "role": "user", 
                        "content": prompt
                    }
                ],
                "max_tokens": 10,
                "temperature": 0.1
            }
            
            logger.info(f"📡 Sending request to Groq API")
            logger.info(f"🎯 Brand focus: {brand_name}")
            
            response = requests.post(self.api_url, headers=self.headers, json=payload, timeout=15)
            logger.info(f"📊 Response status: {response.status_code}")
            
            if response.status_code == 200:
                result = response.json()
                sentiment_text = result['choices'][0]['message']['content'].strip().lower()
                logger.info(f"🤖 Groq response: '{sentiment_text}'")
                
                # Clean and validate response
                if 'positive' in sentiment_text:
                    sentiment = 'positive'
                elif 'negative' in sentiment_text:
                    sentiment = 'negative'
                else:
                    sentiment = 'neutral'
                
                logger.info(f"🎯 Final brand sentiment for '{brand_name}': {sentiment}")
                return sentiment
            else:
                logger.error(f"❌ Groq API returned status {response.status_code}: {response.text}")
        except Exception as e:
            logger.error(f"💥 Groq sentiment analysis error: {e}")
            import traceback
            logger.error(f"📚 Traceback: {traceback.format_exc()}")
        
        logger.warning("⚠️ Falling back to neutral sentiment")
        return "neutral"

class RedditMonitor:
    def __init__(self, config: dict, db: DatabaseManager):
        self.config = config
        self.db = db
        self.brands = {
            name: re.compile(pattern, re.IGNORECASE)
            for name, pattern in config['brands'].items()
        }
        self.sentiment = SentimentAnalyzer(config['groq_api_token'])
        self.seen_ids = self.db.get_existing_ids()
        self.seen_core_ids = self.db.get_core_reddit_ids()  # For handling mixed ID formats
        self.seen_content = self.db.get_existing_content_hashes()
        logger.info(f"🔍 Loaded {len(self.seen_ids)} existing IDs to prevent duplicates")
        logger.info(f"🔍 Loaded {len(self.seen_core_ids)} core Reddit IDs to prevent duplicates")
        logger.info(f"🔍 Loaded {len(self.seen_content)} existing content hashes to prevent duplicates")
        if self.seen_ids:
            logger.info(f"🔍 Sample existing IDs: {list(self.seen_ids)[:3]}")  # Show first 3 IDs
        if self.seen_core_ids:
            logger.info(f"🔍 Sample core IDs: {list(self.seen_core_ids)[:3]}")  # Show first 3 core IDs
        self.mention_buffer: List[Mention] = []  # For PRAW mention objects
        self.json_mention_buffer = []  # For JSON mention dictionaries
        self.running = False
        
        # Backfill tracking for intelligent gap filling
        self.last_comment_rate_limit = None
        self.last_post_rate_limit = None
        self.comment_rate_limit_start = None
        self.post_rate_limit_start = None
        
        # 🛡️ NEW RESILIENCE SYSTEMS
        self.error_tracker = ErrorTracker()
        self.failed_request_queue = FailedRequestQueue()
        self.rss_backup = RSSBackupMonitor(self.brands, self.db, self.sentiment)
        self.system_health = {
            'praw_comments': True,
            'praw_posts': True, 
            'json_api': True,
            'rss_backup': True  # Healthy when in standby mode
        }
        logger.info("🛡️ Resilience systems initialized: error tracking, request queue, RSS backup")
        
        # Initialize Reddit client
        if config['reddit']['client_id'] and config['reddit']['client_secret']:
            try:
                self.reddit = praw.Reddit(
                    client_id=config['reddit']['client_id'],
                    client_secret=config['reddit']['client_secret'],
                    user_agent=config['reddit']['user_agent']
                )
                # Don't test connection during initialization to avoid blocking startup
                logger.info("✅ PRAW Reddit client initialized (authentication will be tested during first use)")
            except Exception as e:
                logger.error(f"❌ PRAW initialization failed: {e}")
                self.reddit = None
        else:
            self.reddit = None
            logger.warning("❌ Reddit credentials not provided - PRAW monitoring disabled")
    
    def find_brands(self, text: str) -> List[str]:
        """Find brand mentions in text"""
        brands_found = []
        for brand, pattern in self.brands.items():
            if pattern.search(text):
                brands_found.append(brand)
        return brands_found
    
    def test_brand_patterns(self):
        """Test brand patterns with sample text"""
        test_texts = [
            "I love badinka clothing!",
            "Check out @badinka.com",
            "#badinka is awesome",
            "Devil walking is cool",
            "devilwalking brand",
            "devil walking fashion",
            "This is a random text with no brands"
        ]
        
        logger.info("🧪 Testing brand pattern recognition:")
        for text in test_texts:
            brands = self.find_brands(text)
            if brands:
                logger.info(f"✅ Found {brands} in: '{text}'")
            else:
                logger.info(f"❌ No brands in: '{text}'")
    
    async def process_mention_buffer(self):
        """Process and save mentions from buffer"""
        if not self.mention_buffer:
            return
        
        # Add focused sentiment analysis for brand mentions only
        for mention in self.mention_buffer:
            if mention.sentiment is None:
                # Extract context around the brand mention for focused sentiment analysis
                context_text = self._extract_brand_context(mention)
                if context_text:
                    mention.sentiment = self.sentiment.analyze(context_text, mention.brand)
                    logger.debug(f"💭 Analyzed sentiment for '{mention.brand}': {mention.sentiment}")
                else:
                    mention.sentiment = "neutral"  # Fallback if no context found
        
        # Save to database
        self.db.insert_mentions(self.mention_buffer)
        self.mention_buffer.clear()
    
    async def monitor_rss_feeds(self):
        """Monitor RSS feeds for new posts"""
        logger.info("Starting RSS monitoring...")
        
        while self.running:
            try:
                # Monitor all Reddit posts or specific subreddits
                if self.config.get('monitor_all_reddit', False):
                    subreddits_to_monitor = ["all", "popular"]
                else:
                    subreddits_to_monitor = self.config['subreddits']
                
                for subreddit in subreddits_to_monitor:
                    if not self.running:
                        break
                    
                    url = f"https://www.reddit.com/r/{subreddit}/new/.rss"
                    
                    try:
                        response = requests.get(url, timeout=10)
                        if response.status_code == 200:
                            rss_text = response.text
                            logger.debug(f"📡 Fetched RSS for r/{subreddit} - {len(rss_text)} bytes")
                            await self._process_rss_feed(rss_text, subreddit)
                        else:
                            logger.warning(f"RSS r/{subreddit} returned status {response.status_code}")
                    except Exception as e:
                        logger.error(f"RSS error for r/{subreddit}: {e}")
                    
                    await asyncio.sleep(2)  # Delay between subreddits
                
                # Flush buffer if needed
                if self.mention_buffer:
                    await self.process_mention_buffer()
                
                await asyncio.sleep(300)  # Check RSS every 5 minutes
                
            except Exception as e:
                logger.error(f"RSS monitoring error: {e}")
                await asyncio.sleep(60)
    
    async def _process_rss_feed(self, rss_text: str, subreddit: str):
        """Process RSS feed content"""
        try:
            feed = feedparser.parse(rss_text)
            logger.debug(f"📊 RSS r/{subreddit}: {len(feed.entries)} entries")
            
            for entry in feed.entries:
                if not self.running:
                    break
                
                # Extract Reddit ID from link
                reddit_id = entry.link.split('/')[-2] if entry.link else entry.id
                
                if reddit_id in self.seen_ids:
                    continue
                
                # Check title and description for brand mentions
                title = getattr(entry, 'title', '')
                description = getattr(entry, 'summary', '')
                full_text = f"{title} {description}"
                
                brands = self.find_brands(full_text)
                if brands:
                    for brand in brands:
                        mention = Mention(
                            id=reddit_id,
                            type="post",
                            title=title,
                            body=description,
                            permalink=entry.link,
                            created=datetime.fromtimestamp(time.mktime(entry.published_parsed), tz=timezone.utc).isoformat(),
                            subreddit=subreddit,
                            author=getattr(entry, 'author', 'unknown'),
                            score=0,  # RSS doesn't provide scores
                            sentiment=None,
                            brand=brand,
                            source="rss"
                        )
                        
                        self.mention_buffer.append(mention)
                        self.seen_ids.add(reddit_id)
                        logger.info(f"Found RSS mention: {brand} in r/{subreddit}")
                        
        except Exception as e:
            logger.error(f"RSS processing error: {e}")
    
    async def monitor_json_api(self):
        """Monitor Reddit JSON API for comments"""
        logger.info("Starting JSON API monitoring...")
        
        while self.running:
            try:
                # Monitor all Reddit or specific subreddits
                if self.config.get('monitor_all_reddit', False):
                    # Monitor all Reddit comments
                    url = "https://www.reddit.com/r/all/comments.json?limit=100"
                    
                    try:
                        response = requests.get(url, timeout=10)
                        if response.status_code == 200:
                            data = response.json()
                            comment_count = len(data.get('data', {}).get('children', []))
                            logger.debug(f"📊 JSON API r/all: {comment_count} comments")
                            await self._process_json_comments(data)
                        elif response.status_code == 429:
                            logger.warning("JSON API rate limited")
                            await asyncio.sleep(60)
                        else:
                            logger.warning(f"JSON API returned status {response.status_code}")
                    except Exception as e:
                        logger.error(f"JSON API error for r/all: {e}")
                    
                    await asyncio.sleep(10)  # Slightly longer delay for r/all
                else:
                    # Monitor specific subreddits (original behavior)
                    for subreddit_chunk in self._chunk_subreddits():
                        if not self.running:
                            break
                        
                        chunk_str = "+".join(subreddit_chunk)
                        url = f"https://www.reddit.com/r/{chunk_str}/comments.json?limit=25"
                        
                        try:
                            response = requests.get(url, timeout=10)
                            if response.status_code == 200:
                                data = response.json()
                                await self._process_json_comments(data)
                            elif response.status_code == 429:
                                logger.warning("JSON API rate limited")
                                await asyncio.sleep(60)
                        except Exception as e:
                            logger.error(f"JSON API error for {chunk_str}: {e}")
                        
                        await asyncio.sleep(5)  # Delay between chunks
                
                # Flush buffer if needed
                if self.mention_buffer:
                    await self.process_mention_buffer()
                
                await asyncio.sleep(30)  # Wait before next cycle
                
            except Exception as e:
                logger.error(f"JSON API monitoring error: {e}")
                await asyncio.sleep(60)
    
    def _chunk_subreddits(self, chunk_size: int = 10):
        """Split subreddits into chunks for API efficiency"""
        subreddits = self.config['subreddits']
        for i in range(0, len(subreddits), chunk_size):
            yield subreddits[i:i + chunk_size]
    
    async def _process_json_comments(self, data: dict):
        """Process JSON API comment data"""
        try:
            if 'data' in data and 'children' in data['data']:
                for item in data['data']['children']:
                    if not self.running:
                        break
                    
                    comment_data = item['data']
                    comment_id = comment_data.get('id')
                    
                    if comment_id in self.seen_ids:
                        continue
                    
                    body = comment_data.get('body', '')
                    brands = self.find_brands(body)
                    
                    if brands:
                        for brand in brands:
                            mention = Mention(
                                id=comment_id,
                                type="comment",
                                title=None,
                                body=body,
                                permalink=f"https://reddit.com{comment_data.get('permalink', '')}",
                                created=datetime.fromtimestamp(comment_data.get('created_utc', 0), tz=timezone.utc).isoformat(),
                                subreddit=comment_data.get('subreddit', 'unknown'),
                                author=comment_data.get('author', 'unknown'),
                                score=comment_data.get('score', 0),
                                sentiment=None,
                                brand=brand,
                                source="json_api"
                            )
                            
                            self.mention_buffer.append(mention)
                            self.seen_ids.add(comment_id)
                            logger.info(f"Found JSON mention: {brand} in r/{comment_data['subreddit']}")
                            
        except Exception as e:
            logger.error(f"JSON processing error: {e}")
    
    async def start_monitoring(self):
        """Start all monitoring tasks"""
        self.running = True
        logger.info("Starting Reddit monitoring...")
        
        # Test brand patterns
        self.test_brand_patterns()
        
        tasks = [
            # RSS and JSON API are getting 403 errors, but PRAW is working perfectly
            # self.monitor_rss_feeds(),
            # self.monitor_json_api(),
            self.monitor_focused_subreddits()  # Additional focused monitoring
        ]
        
        # Start PRAW monitoring in separate thread if available
        if self.reddit:
            praw_thread = threading.Thread(target=self._monitor_praw_sync, daemon=True)
            praw_thread.start()
        
        await asyncio.gather(*tasks, return_exceptions=True)
    
    def _monitor_praw_sync(self):
        """Monitor comments and posts using PRAW in sync mode (separate thread)"""
        if not self.reddit:
            return
        
        logger.info("🚀 Starting PRAW monitoring with staggered timing and resilience systems...")
        
        # 🛡️ Start RSS backup monitor (always running, activates when needed)
        self.rss_backup.running = True
        rss_thread = threading.Thread(target=self.rss_backup.monitor_rss_feeds, daemon=True)
        rss_thread.start()
        logger.info("✅ Started RSS backup monitor (standby mode)")
        
        # 🛡️ Start failed request processor
        retry_thread = threading.Thread(target=self._process_failed_requests, daemon=True)
        retry_thread.start()
        logger.info("✅ Started failed request processor")
        
        # 🛡️ Start system health monitor
        health_thread = threading.Thread(target=self._monitor_system_health, daemon=True)
        health_thread.start()
        logger.info("✅ Started system health monitor")
        
        # Start comment monitoring immediately
        comment_thread = threading.Thread(target=self._monitor_praw_comments, daemon=True)
        comment_thread.start()
        logger.info("✅ Started PRAW comment monitoring")
        
        # Start JSON monitoring immediately (different endpoint, less conflicts)
        json_thread = threading.Thread(target=self._monitor_json_comments_chunked, daemon=True)
        json_thread.start()
        logger.info("✅ Started enhanced JSON comment monitoring with chunking")
        
        # STAGGERED: Wait 30 seconds before starting post monitoring to offset rate limits
        logger.info("⏳ Waiting 30 seconds before starting post monitoring (staggered approach)...")
        time.sleep(30)
        
        post_thread = threading.Thread(target=self._monitor_praw_posts, daemon=True)
        post_thread.start()
        logger.info("✅ Started PRAW post monitoring (staggered)")
        
        # Wait for threads to complete
        comment_thread.join()
        post_thread.join()
        json_thread.join()
        rss_thread.join()
        retry_thread.join()
        health_thread.join()
    
    def _monitor_praw_comments(self):
        """Monitor comments using PRAW"""
        logger.info("Starting PRAW comment monitoring...")
        
        # Add startup delay to avoid processing recent comments that might be in database
        logger.info("⏳ Waiting 30 seconds to avoid processing recent comments...")
        time.sleep(30)
        
        while self.running:
            try:
                # Monitor all Reddit comments
                if self.config.get('monitor_all_reddit', False):
                    subreddit = self.reddit.subreddit("all")
                else:
                    subreddit = self.reddit.subreddit("+".join(self.config['subreddits']))
                # Use skip_existing=True but also manually track to ensure no duplicates
                comment_stream = subreddit.stream.comments(skip_existing=True, pause_after=10)
                logger.info(f"🎯 Starting comment stream for {subreddit} (skip_existing=True)")
                
                for comment in comment_stream:
                    if not self.running:
                        break
                    
                    if comment is None:
                        time.sleep(1.5)  # Slightly longer pause to reduce rate limiting
                        continue
                    
                    if comment.id in self.seen_ids:
                        logger.debug(f"⏭️ Skipping already processed comment: {comment.id}")
                        continue
                    
                    # Debug: Log every 100th comment to see what we're processing
                    if hasattr(self, '_comment_count'):
                        self._comment_count += 1
                    else:
                        self._comment_count = 1
                    
                    if self._comment_count % 100 == 0:
                        logger.debug(f"🔍 Processed {self._comment_count} comments, checking: '{comment.body[:50]}...'")
                        # 🛡️ Record successful processing every 100 comments
                        self.error_tracker.record_success("praw_comments")
                        self.system_health['praw_comments'] = True
                        self._check_backup_deactivation()
                    
                    brands = self.find_brands(comment.body)
                    if brands:
                        for brand in brands:
                            # Analyze sentiment IMMEDIATELY
                            context_text = comment.body[:400]  # Focus on relevant content
                            try:
                                sentiment = self.sentiment.analyze(context_text, brand)
                                logger.info(f"💭 Analyzed sentiment for '{brand}': {sentiment}")
                            except Exception as e:
                                sentiment = "neutral"
                                logger.warning(f"Sentiment analysis failed for {brand}: {e}")
                            
                            mention = Mention(
                                id=comment.id,  # Use actual Reddit comment ID (e.g., n3z93h6)
                                type="comment",
                                title=None,
                                body=comment.body,
                                permalink=f"https://reddit.com{comment.permalink}",
                                created=datetime.fromtimestamp(comment.created_utc, tz=timezone.utc).isoformat(),
                                subreddit=str(comment.subreddit),
                                author=str(comment.author),
                                score=comment.score,
                                sentiment=sentiment,  # Set analyzed sentiment
                                brand=brand,
                                source="praw"
                            )
                            
                            # Check for duplicates using ID, core ID, and content
                            content_hash = f"{brand}_{str(comment.subreddit)}_{hash(comment.body[:100])}"
                            
                            is_duplicate = (comment.id in self.seen_ids or 
                                          comment.id in self.seen_core_ids or 
                                          content_hash in self.seen_content)
                            
                            if not is_duplicate:
                                # Save to database IMMEDIATELY
                                self.db.insert_mentions([mention])
                                self.seen_ids.add(comment.id)
                                self.seen_core_ids.add(comment.id)
                                self.seen_content.add(content_hash)
                                logger.info(f"✅ Saved PRAW mention: {brand} in r/{comment.subreddit} with sentiment: {sentiment} (ID: {comment.id})")
                            else:
                                logger.info(f"⏭️ Skipped duplicate: ID={comment.id in self.seen_ids}, CoreID={comment.id in self.seen_core_ids}, Content={content_hash in self.seen_content} for brand {brand}")
                    
                    # Flush buffer more frequently for immediate processing
                    if len(self.mention_buffer) >= 1:  # Process immediately
                        # Process buffer with sentiment analysis
                        if self.mention_buffer:
                            logger.info(f"💾 Processing {len(self.mention_buffer)} PRAW mentions from buffer...")
                            self._process_praw_buffer_with_sentiment()
                            self.mention_buffer.clear()
                
            except prawcore.exceptions.TooManyRequests:
                # Track rate limit period for backfill
                self.comment_rate_limit_start = datetime.now(timezone.utc)
                logger.warning("PRAW rate limited, sleeping 60 seconds")
                
                # 🛡️ Activate RSS backup during rate limit
                self.system_health['praw_comments'] = False
                self._check_backup_activation()
                
                time.sleep(60)
                
                # Mark system as healthy after rate limit
                self.system_health['praw_comments'] = True
                self._check_backup_deactivation()
                
                # After rate limit ends, trigger backfill
                rate_limit_end = datetime.now(timezone.utc)
                if self.comment_rate_limit_start:
                    logger.info("🔄 Rate limit ended, starting comment backfill...")
                    threading.Thread(
                        target=self._backfill_missed_content,
                        args=("comments", self.comment_rate_limit_start, rate_limit_end),
                        daemon=True
                    ).start()
            except prawcore.exceptions.ServerError as e:
                # 🛡️ Handle 500 errors with smart backoff
                logger.warning(f"PRAW server error (500): {e}")
                self.error_tracker.record_error("praw_comments", "500")
                self.system_health['praw_comments'] = False
                self._check_backup_activation()
                
                # Add failed request to retry queue
                self.failed_request_queue.add_failed_request({
                    'type': 'praw_comments',
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'error': str(e)
                })
                
                # Smart backoff delay
                delay = self.error_tracker.get_delay_with_jitter("praw_comments")
                logger.warning(f"⏳ Backing off for {delay:.1f} seconds due to 500 errors")
                time.sleep(delay)
            except Exception as e:
                logger.error(f"PRAW comment monitoring error: {e}")
                self.error_tracker.record_error("praw_comments", "other")
                time.sleep(30)
    
    def _monitor_praw_posts(self):
        """Monitor posts using PRAW"""
        logger.info("Starting PRAW post monitoring...")
        
        # Add startup delay to avoid processing recent posts that might be in database  
        logger.info("⏳ Waiting 30 seconds to avoid processing recent posts...")
        time.sleep(30)
        
        while self.running:
            try:
                # Monitor all Reddit posts
                if self.config.get('monitor_all_reddit', False):
                    subreddit = self.reddit.subreddit("all")
                else:
                    subreddit = self.reddit.subreddit("+".join(self.config['subreddits']))
                
                post_stream = subreddit.stream.submissions(skip_existing=True, pause_after=10)
                
                for post in post_stream:
                    if not self.running:
                        break
                    
                    if post is None:
                        time.sleep(1.5)  # Slightly longer pause to reduce rate limiting
                        continue
                    
                    if post.id in self.seen_ids:
                        continue
                    
                    # Debug: Log every 50th post to see what we're processing
                    if hasattr(self, '_post_count'):
                        self._post_count += 1
                    else:
                        self._post_count = 1
                    
                    if self._post_count % 50 == 0:
                        logger.debug(f"📝 Processed {self._post_count} posts, checking: '{post.title[:50]}...'")
                        # 🛡️ Record successful processing every 50 posts
                        self.error_tracker.record_success("praw_posts")
                        self.system_health['praw_posts'] = True
                        self._check_backup_deactivation()
                    
                    # Check both title and selftext for brand mentions
                    full_text = f"{post.title} {post.selftext}"
                    brands = self.find_brands(full_text)
                    
                    if brands:
                        for brand in brands:
                            # Analyze sentiment IMMEDIATELY
                            context_text = full_text[:400]  # Focus on relevant content
                            try:
                                sentiment = self.sentiment.analyze(context_text, brand)
                                logger.info(f"💭 Analyzed post sentiment for '{brand}': {sentiment}")
                            except Exception as e:
                                sentiment = "neutral"
                                logger.warning(f"Post sentiment analysis failed for {brand}: {e}")
                            
                            mention = Mention(
                                id=post.id,  # Use actual Reddit post ID
                                type="post",
                                title=post.title,
                                body=post.selftext,
                                permalink=f"https://reddit.com{post.permalink}",
                                created=datetime.fromtimestamp(post.created_utc, tz=timezone.utc).isoformat(),
                                subreddit=str(post.subreddit),
                                author=str(post.author),
                                score=post.score,
                                sentiment=sentiment,  # Set analyzed sentiment
                                brand=brand,
                                source="praw"
                            )
                            
                            # Check for duplicates using ID, core ID, and content
                            content_hash = f"{brand}_{str(post.subreddit)}_{hash(full_text[:100])}"
                            
                            is_duplicate = (post.id in self.seen_ids or 
                                          post.id in self.seen_core_ids or 
                                          content_hash in self.seen_content)
                            
                            if not is_duplicate:
                                # Save to database IMMEDIATELY
                                self.db.insert_mentions([mention])
                                self.seen_ids.add(post.id)
                                self.seen_core_ids.add(post.id)
                                self.seen_content.add(content_hash)
                                logger.info(f"✅ Saved PRAW post mention: {brand} in r/{post.subreddit} with sentiment: {sentiment}")
                            else:
                                logger.info(f"⏭️ Skipped duplicate post: ID={post.id in self.seen_ids}, CoreID={post.id in self.seen_core_ids}, Content={content_hash in self.seen_content} for brand {brand}")
                    
                    # Flush buffer more frequently for immediate processing
                    if len(self.mention_buffer) >= 1:  # Process immediately
                        # Process buffer with sentiment analysis
                        if self.mention_buffer:
                            logger.info(f"💾 Processing {len(self.mention_buffer)} PRAW post mentions from buffer...")
                            self._process_praw_buffer_with_sentiment()
                            self.mention_buffer.clear()
                
            except prawcore.exceptions.TooManyRequests:
                # Track rate limit period for backfill
                self.post_rate_limit_start = datetime.now(timezone.utc)
                logger.warning("PRAW posts rate limited, sleeping 60 seconds")
                
                # 🛡️ Activate RSS backup during rate limit
                self.system_health['praw_posts'] = False
                self._check_backup_activation()
                
                time.sleep(60)
                
                # Mark system as healthy after rate limit
                self.system_health['praw_posts'] = True
                self._check_backup_deactivation()
                
                # After rate limit ends, trigger backfill
                rate_limit_end = datetime.now(timezone.utc)
                if self.post_rate_limit_start:
                    logger.info("🔄 Rate limit ended, starting post backfill...")
                    threading.Thread(
                        target=self._backfill_missed_content,
                        args=("posts", self.post_rate_limit_start, rate_limit_end),
                        daemon=True
                    ).start()
            except prawcore.exceptions.ServerError as e:
                # 🛡️ Handle 500 errors with smart backoff
                logger.warning(f"PRAW posts server error (500): {e}")
                self.error_tracker.record_error("praw_posts", "500")
                self.system_health['praw_posts'] = False
                self._check_backup_activation()
                
                # Add failed request to retry queue
                self.failed_request_queue.add_failed_request({
                    'type': 'praw_posts',
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'error': str(e)
                })
                
                # Smart backoff delay
                delay = self.error_tracker.get_delay_with_jitter("praw_posts")
                logger.warning(f"⏳ Posts backing off for {delay:.1f} seconds due to 500 errors")
                time.sleep(delay)
            except Exception as e:
                logger.error(f"PRAW post monitoring error: {e}")
                self.error_tracker.record_error("praw_posts", "other")
                time.sleep(30)
    
    def _process_json_buffer_with_sentiment(self):
        """Process JSON mention buffer with sentiment analysis (synchronous)"""
        if not self.json_mention_buffer:
            return
        
        try:
            for mention_dict in self.json_mention_buffer:
                # Extract context and analyze sentiment
                context_text = mention_dict.get('context', mention_dict.get('content', ''))
                if context_text and len(context_text) > 10:
                    sentiment = self.sentiment.analyze(context_text, mention_dict['brand'])
                    logger.debug(f"💭 Analyzed sentiment for '{mention_dict['brand']}': {sentiment}")
                else:
                    sentiment = "neutral"
                
                # Create Mention object and save to database
                mention_obj = Mention(
                    id=f"json_{mention_dict['brand']}_{int(mention_dict['created'])}_{hash(mention_dict['content'][:50])}",
                    type="comment",
                    title=mention_dict['title'],
                    body=mention_dict['content'],
                    permalink=mention_dict['url'],
                    created=datetime.fromtimestamp(mention_dict['created']).isoformat(),
                    subreddit=mention_dict['location'],
                    author=mention_dict.get('author', 'unknown'),
                    score=mention_dict.get('score', 0),
                    sentiment=sentiment,
                    brand=mention_dict['brand'],
                    source="json_chunked_monitoring"
                )
                
                self.db.insert_mentions([mention_obj])
                logger.info(f"✅ Saved {mention_dict['brand']} mention to database")
                
        except Exception as e:
            logger.error(f"Error processing JSON buffer: {e}")
    
    def _process_praw_buffer_with_sentiment(self):
        """Process PRAW mention buffer with sentiment analysis (synchronous)"""
        if not self.mention_buffer:
            return
        
        # Add sentiment analysis for each mention
        for mention in self.mention_buffer:
            if mention.sentiment is None:
                # Extract context around the brand mention for focused sentiment analysis
                context_text = self._extract_brand_context(mention)
                if context_text:
                    # Run sentiment analysis on brand-focused context
                    mention.sentiment = self.sentiment.analyze(context_text, mention.brand)
                    logger.debug(f"💭 Analyzed sentiment for '{mention.brand}': {mention.sentiment}")
                else:
                    mention.sentiment = "neutral"  # Fallback if no context found
        
        # Save to database
        self.db.insert_mentions(self.mention_buffer)
        logger.info(f"💭 Processed {len(self.mention_buffer)} brand mentions with sentiment analysis")
    
    def _extract_simple_context(self, text, brand_name):
        """Simple context extraction for JSON monitoring"""
        text_lower = text.lower()
        brand_lower = brand_name.lower()
        
        # Find brand position
        pos = text_lower.find(brand_lower)
        if pos == -1:
            return text[:200]  # Return first 200 chars if brand not found
        
        # Extract 100 chars before and after the brand
        start = max(0, pos - 100)
        end = min(len(text), pos + len(brand_lower) + 100)
        
        return text[start:end].strip()
    
    def _extract_brand_context(self, mention):
        """Extract context around brand mention for focused sentiment analysis"""
        full_text = f"{mention.title or ''} {mention.body or ''}".lower()
        brand_name = mention.brand.lower()
        
        # Handle multi-word brands like "devil walking"
        if ' ' in brand_name:
            brand_patterns = [brand_name, brand_name.replace(' ', '')]
        else:
            brand_patterns = [brand_name, brand_name.replace(' ', '')]
        
        # Find the brand in the text
        brand_position = -1
        matched_pattern = None
        
        for pattern in brand_patterns:
            pos = full_text.find(pattern)
            if pos != -1:
                brand_position = pos
                matched_pattern = pattern
                break
        
        if brand_position == -1:
            logger.warning(f"Brand '{brand_name}' not found in text for sentiment analysis")
            return None
        
        # Extract context: 100 characters before and after the brand mention
        context_start = max(0, brand_position - 100)
        context_end = min(len(full_text), brand_position + len(matched_pattern) + 100)
        
        context = full_text[context_start:context_end].strip()
        
        # Ensure we have meaningful context (at least the brand + some words)
        if len(context) < len(matched_pattern) + 10:
            # If context is too short, use a bit more
            context_start = max(0, brand_position - 200)
            context_end = min(len(full_text), brand_position + len(matched_pattern) + 200)
            context = full_text[context_start:context_end].strip()
        
        logger.debug(f"📝 Brand context for '{brand_name}': '{context[:100]}...'")
        return context
    
    def _backfill_missed_content(self, content_type: str, gap_start: datetime, gap_end: datetime):
        """Intelligent backfill to catch content missed during rate limit periods"""
        try:
            logger.info(f"🔄 Starting backfill for {content_type} from {gap_start} to {gap_end}")
            
            # Calculate gap duration
            gap_duration = (gap_end - gap_start).total_seconds()
            if gap_duration < 30:  # Don't backfill very short gaps
                logger.info(f"⏭️ Skipping backfill - gap too short ({gap_duration}s)")
                return
            
            # Use JSON API for backfill (more reliable than PRAW for historical data)
            if content_type == "comments":
                self._backfill_comments_json(gap_start, gap_end)
            elif content_type == "posts":
                self._backfill_posts_json(gap_start, gap_end)
                
        except Exception as e:
            logger.error(f"❌ Backfill error for {content_type}: {e}")
    
    def _backfill_comments_json(self, gap_start: datetime, gap_end: datetime):
        """Backfill comments using JSON API"""
        try:
            # Use focused subreddits for backfill to reduce load
            subreddits_to_check = self.config.get('focused_subreddits', [])[:5]  # Limit to top 5
            
            for subreddit in subreddits_to_check:
                url = f"https://www.reddit.com/r/{subreddit}/comments.json?limit=100&sort=new"
                
                try:
                    response = requests.get(url, timeout=10)
                    if response.status_code == 200:
                        data = response.json()
                        comments_found = 0
                        
                        for item in data.get('data', {}).get('children', []):
                            comment_data = item.get('data', {})
                            created_time = datetime.fromtimestamp(comment_data.get('created_utc', 0), tz=timezone.utc)
                            
                            # Check if comment falls within gap period
                            if gap_start <= created_time <= gap_end:
                                comment_body = comment_data.get('body', '')
                                brands = self.find_brands(comment_body)
                                
                                if brands:
                                    comments_found += 1
                                    # Process as normal mention
                                    for brand in brands:
                                        logger.info(f"🔄 Backfilled comment mention: {brand} in r/{subreddit}")
                        
                        if comments_found > 0:
                            logger.info(f"✅ Backfilled {comments_found} comments from r/{subreddit}")
                            
                except Exception as e:
                    logger.error(f"❌ Backfill error for r/{subreddit} comments: {e}")
                    
                time.sleep(1)  # Rate limiting for backfill
                
        except Exception as e:
            logger.error(f"❌ Comment backfill error: {e}")
    
    def _backfill_posts_json(self, gap_start: datetime, gap_end: datetime):
        """Backfill posts using JSON API"""
        try:
            # Use focused subreddits for backfill
            subreddits_to_check = self.config.get('focused_subreddits', [])[:5]  # Limit to top 5
            
            for subreddit in subreddits_to_check:
                url = f"https://www.reddit.com/r/{subreddit}/new.json?limit=100"
                
                try:
                    response = requests.get(url, timeout=10)
                    if response.status_code == 200:
                        data = response.json()
                        posts_found = 0
                        
                        for item in data.get('data', {}).get('children', []):
                            post_data = item.get('data', {})
                            created_time = datetime.fromtimestamp(post_data.get('created_utc', 0), tz=timezone.utc)
                            
                            # Check if post falls within gap period
                            if gap_start <= created_time <= gap_end:
                                full_text = f"{post_data.get('title', '')} {post_data.get('selftext', '')}"
                                brands = self.find_brands(full_text)
                                
                                if brands:
                                    posts_found += 1
                                    # Process as normal mention
                                    for brand in brands:
                                        logger.info(f"🔄 Backfilled post mention: {brand} in r/{subreddit}")
                        
                        if posts_found > 0:
                            logger.info(f"✅ Backfilled {posts_found} posts from r/{subreddit}")
                            
                except Exception as e:
                    logger.error(f"❌ Backfill error for r/{subreddit} posts: {e}")
                    
                time.sleep(1)  # Rate limiting for backfill
                
        except Exception as e:
            logger.error(f"❌ Post backfill error: {e}")
     
    async def monitor_focused_subreddits(self):
        """Monitor high-priority subreddits for extra coverage"""
        logger.info("Starting focused subreddits monitoring...")
        
        while self.running:
            try:
                for subreddit in self.config.get('focused_subreddits', []):
                    if not self.running:
                        break
                    
                    # Monitor posts for each focused subreddit (comments handled by JSON chunked monitoring)
                    await self._monitor_subreddit_posts(subreddit)
                    
                    # Small delay between monitoring different content types
                    await asyncio.sleep(1)
                    
                    await asyncio.sleep(3)  # Delay between subreddits
                
                # Flush buffer if needed
                if self.mention_buffer:
                    await self.process_mention_buffer()
                
                await asyncio.sleep(120)  # Check focused subreddits every 2 minutes
                
            except Exception as e:
                logger.error(f"Focused subreddits monitoring error: {e}")
                await asyncio.sleep(60)
    
    async def _monitor_subreddit_posts(self, subreddit: str):
        """Monitor posts in a specific subreddit"""
        try:
            url = f"https://www.reddit.com/r/{subreddit}/new.json?limit=25"
            
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                await self._process_subreddit_posts(data, subreddit)
            elif response.status_code == 429:
                logger.warning(f"Rate limited for r/{subreddit}")
                await asyncio.sleep(30)
                        
        except Exception as e:
            logger.error(f"Error monitoring r/{subreddit} posts: {e}")
    
    def _monitor_json_comments_chunked(self):
        """Enhanced JSON comment monitoring with chunking and robust error handling"""
        logger.info("📡 Enhanced JSON comment poller started...")
        import requests
        import random
        
        session = requests.Session()
        session.headers.update({"User-Agent": "BrandMentionMonitor/1.0 by AllInOneRedditMonitor"})
        seen_json_ids = set()
        chunk_size = 3  # Smaller chunks to reduce rate limiting
        base_delay = 25  # Longer delay to reduce rate limiting
        
        subreddits = self.config.get('focused_subreddits', [])
        
        while self.running:
            try:
                for i in range(0, len(subreddits), chunk_size):
                    if not self.running:
                        break
                        
                    chunk = subreddits[i:i + chunk_size]
                    chunk_str = "+".join(chunk)
                    url = f"https://www.reddit.com/r/{chunk_str}/comments.json?limit=100"
                    
                    try:
                        response = session.get(url, timeout=10)
                        if response.status_code == 429:
                            logger.warning(f"❌ 429 Too Many Requests on chunk: {chunk_str}")
                            time.sleep(30)
                            continue
                        response.raise_for_status()
                        
                        data = response.json()
                        children = data.get("data", {}).get("children", [])
                        
                        for item in children:
                            if not self.running:
                                break
                                
                            c = item.get("data", {})
                            cid = c.get("id")
                            if not c or cid in seen_json_ids:
                                continue
                                
                            body = c.get("body", "")
                            if len(body) < 20:
                                continue
                            
                            # Check for brand mentions
                            for brand_name, brand_pattern in self.brands.items():
                                if brand_pattern.search(body):
                                    # Analyze sentiment IMMEDIATELY
                                    context = self._extract_simple_context(body, brand_name)
                                    try:
                                        sentiment = self.sentiment.analyze(context, brand_name)
                                        logger.info(f"💭 Analyzed JSON sentiment for '{brand_name}': {sentiment}")
                                    except Exception as e:
                                        sentiment = "neutral"
                                        logger.warning(f"JSON sentiment analysis failed for {brand_name}: {e}")
                                    
                                    # Use actual Reddit comment ID (e.g., n3z93h6)
                                    reddit_id = c.get('id')
                                    if not reddit_id:
                                        logger.warning("❌ No Reddit ID found in JSON data")
                                        continue
                                    mention_id = reddit_id  # Use actual Reddit ID directly
                                    
                                    # Check for duplicates using both ID and content  
                                    content_hash = f"{brand_name}_{c.get('subreddit', 'unknown')}_{hash(body[:100])}"
                                    
                                    is_duplicate = (mention_id in self.seen_ids or 
                                                   mention_id in self.seen_core_ids or 
                                                   content_hash in self.seen_content)
                                    
                                    if is_duplicate:
                                        logger.debug(f"⏭️ Skipped duplicate JSON mention: ID={mention_id in self.seen_ids}, CoreID={mention_id in self.seen_core_ids}, Content={content_hash in self.seen_content}")
                                        continue
                                    
                                    # Create Mention object and save IMMEDIATELY
                                    mention_obj = Mention(
                                        id=mention_id,
                                        type="comment",
                                        title=f"Comment in r/{c.get('subreddit', 'unknown')}",
                                        body=body,
                                        permalink=f"https://reddit.com{c.get('permalink', '')}",
                                        created=datetime.fromtimestamp(c.get('created_utc', time.time())).isoformat(),
                                        subreddit=f"r/{c.get('subreddit', 'unknown')}",
                                        author=c.get('author', 'unknown'),
                                        score=c.get('score', 0),
                                        sentiment=sentiment,  # Set analyzed sentiment
                                        brand=brand_name,
                                        source="json_chunked_monitoring"
                                    )
                                    
                                    # Save to database IMMEDIATELY
                                    self.db.insert_mentions([mention_obj])
                                    self.seen_ids.add(mention_id)  # Add to seen_ids to prevent duplicates
                                    self.seen_core_ids.add(mention_id)  # Add to seen_core_ids to prevent duplicates
                                    self.seen_content.add(content_hash)  # Add to seen_content to prevent duplicates
                                    seen_json_ids.add(cid)
                                    logger.info(f"✅ Saved JSON mention: {brand_name} in r/{c.get('subreddit')} with sentiment: {sentiment}")
                    
                    except requests.exceptions.ConnectionError as e:
                        logger.warning(f"❌ Connection error for chunk {chunk_str}: {e}")
                        backoff = random.randint(20, 40)
                        logger.info(f"⏳ Backing off for {backoff} seconds...")
                        time.sleep(backoff)
                    except requests.exceptions.HTTPError as e:
                        if response.status_code >= 500:
                            logger.warning(f"❌ 5xx server error for chunk {chunk_str}: {e}")
                            backoff = random.randint(25, 60)
                            logger.info(f"⏳ Backing off for {backoff} seconds...")
                            time.sleep(backoff)
                        else:
                            logger.error(f"❌ HTTP error on chunk {chunk_str}: {e}")
                    except Exception as e:
                        logger.error(f"❌ Unknown error on chunk {chunk_str}: {e}")
                    
                    time.sleep(base_delay)
                    
                    # Process JSON mention buffer periodically
                    if self.json_mention_buffer:
                        logger.info(f"💾 Processing {len(self.json_mention_buffer)} JSON mentions from buffer...")
                        self._process_json_buffer_with_sentiment()
                        self.json_mention_buffer.clear()
                    
            except Exception as e:
                logger.error(f"JSON comment monitoring error: {e}")
                time.sleep(60)  # Wait before retrying
                
            # Process any remaining JSON mentions in buffer at end of cycle
            if self.json_mention_buffer:
                logger.info(f"💾 End-of-cycle: Processing {len(self.json_mention_buffer)} JSON mentions from buffer...")
                self._process_json_buffer_with_sentiment()
                self.json_mention_buffer.clear()
    
    async def _process_subreddit_posts(self, data: dict, subreddit: str):
        """Process posts from focused subreddit monitoring"""
        try:
            if 'data' in data and 'children' in data['data']:
                for item in data['data']['children']:
                    if not self.running:
                        break
                    
                    post_data = item['data']
                    post_id = post_data.get('id')
                    
                    if post_id in self.seen_ids:
                        continue
                    
                    title = post_data.get('title', '')
                    selftext = post_data.get('selftext', '')
                    full_text = f"{title} {selftext}"
                    
                    brands = self.find_brands(full_text)
                    if brands:
                        for brand in brands:
                            mention = Mention(
                                id=post_id,
                                type="post",
                                title=title,
                                body=selftext,
                                permalink=f"https://reddit.com{post_data.get('permalink', '')}",
                                created=datetime.fromtimestamp(post_data.get('created_utc', 0), tz=timezone.utc).isoformat(),
                                subreddit=subreddit,
                                author=post_data.get('author', 'unknown'),
                                score=post_data.get('score', 0),
                                sentiment=None,
                                brand=brand,
                                source="focused_subreddit"
                            )
                            
                            self.mention_buffer.append(mention)
                            self.seen_ids.add(post_id)
                            logger.info(f"Found focused mention: {brand} in r/{subreddit} (post)")
                            
        except Exception as e:
            logger.error(f"Focused subreddit posts processing error: {e}")

    def _check_backup_activation(self):
        """Check if RSS backup should be activated"""
        primary_systems_down = (
            not self.system_health['praw_comments'] or 
            not self.system_health['praw_posts'] or 
            not self.system_health['json_api']
        )
        
        if primary_systems_down and not self.rss_backup.active:
            logger.warning("🚨 Primary systems failing - activating RSS backup!")
            self.rss_backup.activate()
            # RSS backup health remains True - it's working as designed
    
    def _check_backup_deactivation(self):
        """Check if RSS backup can be deactivated"""
        primary_systems_healthy = (
            self.system_health['praw_comments'] and 
            self.system_health['praw_posts'] and 
            self.system_health['json_api']
        )
        
        if primary_systems_healthy and self.rss_backup.active:
            logger.info("✅ Primary systems recovered - deactivating RSS backup")
            self.rss_backup.deactivate()
            # RSS backup health remains True - it's healthy in standby mode

    def _process_failed_requests(self):
        """Continuously process and retry requests from the failed_request_queue"""
        logger.info("🔄 Failed request processor started")
        
        while self.running:
            try:
                request = self.failed_request_queue.get_next_request()
                if not request:
                    time.sleep(30)  # No requests to retry, wait
                    continue
                
                # Check if we should skip this request due to circuit breaker
                should_skip, delay = self.error_tracker.should_skip_request(request['type'])
                if should_skip:
                    logger.info(f"⏳ Skipping retry for {request['type']} - circuit breaker active ({delay:.0f}s remaining)")
                    time.sleep(min(delay, 60))  # Wait but not more than 1 minute
                    continue
                
                # Attempt to retry the request
                logger.info(f"🔄 Retrying failed request: {request['type']} (attempt {request['retry_count']})")
                success = self._retry_failed_request(request)
                
                if success:
                    self.error_tracker.record_success(request['type'])
                    logger.info(f"✅ Successfully retried: {request['type']}")
                else:
                    self.error_tracker.record_error(request['type'], "retry_failed")
                    logger.warning(f"❌ Retry failed for: {request['type']}")
                
                time.sleep(10)  # Small delay between retries
                
            except Exception as e:
                logger.error(f"❌ Error in failed request processor: {e}")
                time.sleep(60)
    
    def _retry_failed_request(self, request_info: dict) -> bool:
        """Retry a specific failed request"""
        try:
            request_type = request_info['type']
            
            if request_type == 'praw_comments':
                # Retry PRAW comment monitoring for a short burst
                if self.reddit:
                    subreddit = self.reddit.subreddit("all" if self.config.get('monitor_all_reddit') else "+".join(self.config['subreddits']))
                    comment_stream = subreddit.stream.comments(skip_existing=True, pause_after=5)
                    
                    for i, comment in enumerate(comment_stream):
                        if i >= 10 or comment is None:  # Process max 10 comments in retry
                            break
                        
                        brands = self.find_brands(comment.body)
                        if brands:
                            for brand in brands:
                                # Process the mention
                                logger.info(f"🔄 Retry found: {brand} mention in PRAW comments")
                            return True
                    
            elif request_type == 'praw_posts':
                # Retry PRAW post monitoring for a short burst
                if self.reddit:
                    subreddit = self.reddit.subreddit("all" if self.config.get('monitor_all_reddit') else "+".join(self.config['subreddits']))
                    post_stream = subreddit.stream.submissions(skip_existing=True, pause_after=5)
                    
                    for i, post in enumerate(post_stream):
                        if i >= 5 or post is None:  # Process max 5 posts in retry
                            break
                        
                        full_text = f"{post.title} {post.selftext}"
                        brands = self.find_brands(full_text)
                        if brands:
                            for brand in brands:
                                logger.info(f"🔄 Retry found: {brand} mention in PRAW posts")
                            return True
                            
            elif request_type == 'json_api':
                # Retry JSON API call
                url = "https://www.reddit.com/r/all/comments.json?limit=25"
                response = requests.get(url, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    children = data.get("data", {}).get("children", [])
                    for item in children[:5]:  # Process max 5 comments in retry
                        comment_data = item.get("data", {})
                        body = comment_data.get("body", "")
                        brands = self.find_brands(body)
                        if brands:
                            logger.info(f"🔄 Retry found: brand mention in JSON API")
                            return True
            
            return False
            
        except Exception as e:
            logger.error(f"❌ Error retrying {request_info['type']}: {e}")
            return False
    
    def _monitor_system_health(self):
        """Monitor overall system health and log status"""
        logger.info("🏥 System health monitor started")
        
        while self.running:
            try:
                # Calculate overall health
                healthy_systems = sum(self.system_health.values())
                total_systems = len(self.system_health)
                health_percentage = (healthy_systems / total_systems) * 100
                
                # Log system status every 5 minutes
                logger.info(f"🏥 System Health: {health_percentage:.0f}% ({healthy_systems}/{total_systems} systems healthy)")
                
                # Log individual system status
                for system, status in self.system_health.items():
                    status_emoji = "✅" if status else "❌"
                    logger.info(f"   {status_emoji} {system}: {'Healthy' if status else 'Unhealthy'}")
                
                # Log error statistics
                total_errors = sum(self.error_tracker.error_counts.values())
                if total_errors > 0:
                    logger.info(f"📊 Total errors across all systems: {total_errors}")
                    for source, count in self.error_tracker.error_counts.items():
                        if count > 0:
                            logger.info(f"   ❌ {source}: {count} errors")
                
                # Log queue status
                queue_size = self.failed_request_queue.size()
                if queue_size > 0:
                    logger.info(f"📝 Failed request queue size: {queue_size}")
                
                # Log RSS backup status
                if self.rss_backup.active:
                    logger.info("🔄 RSS backup system: ACTIVE")
                else:
                    logger.info("💤 RSS backup system: STANDBY")
                
                # Emergency RSS activation if health is critically low
                if health_percentage < 30 and not self.rss_backup.active:
                    logger.warning("🚨 CRITICAL: System health below 30% - Emergency RSS backup activation!")
                    self.rss_backup.activate()
                    self.system_health['rss_backup'] = True
                
                time.sleep(300)  # Check every 5 minutes
                
            except Exception as e:
                logger.error(f"❌ System health monitor error: {e}")
                time.sleep(300)

    def stop_monitoring(self):
        """Stop monitoring"""
        self.running = False
        self.rss_backup.running = False
        logger.info("Stopping Reddit monitoring...")

# Flask Web Interface
app = Flask(__name__)
db_manager = None
reddit_monitor = None

@app.route('/debug-info')
def debug_info():
    """Debug route to test if new routes are working"""
    return jsonify({
        "status": "SUCCESS - New routes are working!",
        "timestamp": datetime.utcnow().isoformat(),
        "message": "If you can see this, the deployment picked up our changes",
        "backfill_available": True
    })

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/health')
def health():
    try:
        return jsonify({
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "monitoring": reddit_monitor.running if reddit_monitor else False,
            "port": CONFIG['port']
        })
    except Exception as e:
        # Return basic health check even if reddit_monitor isn't ready
        return jsonify({
            "status": "starting",
            "timestamp": datetime.utcnow().isoformat(),
            "error": str(e)
        })

@app.route('/data')
def get_mentions():
    brand = request.args.get('brand', list(CONFIG['brands'].keys())[0])
    page = int(request.args.get('page', 1))
    per_page = min(int(request.args.get('per_page', 50)), 100)
    
    # Build query
    offset = (page - 1) * per_page
    
    with db_manager.get_connection() as conn:
        if brand:
            query = '''
                SELECT id, type, title, body, permalink, created, subreddit, author, score, sentiment, brand, source
                FROM mentions 
                WHERE brand = ?
                ORDER BY created DESC 
                LIMIT ? OFFSET ?
            '''
            cursor = conn.execute(query, (brand, per_page, offset))
        else:
            query = '''
                SELECT id, type, title, body, permalink, created, subreddit, author, score, sentiment, brand, source
                FROM mentions 
                ORDER BY created DESC 
                LIMIT ? OFFSET ?
            '''
            cursor = conn.execute(query, (per_page, offset))
        
        mentions = cursor.fetchall()
    
    # Convert to list of dicts
    results = []
    for mention in mentions:
        results.append({
            'id': mention[0],
            'type': mention[1],
            'title': mention[2],
            'body': mention[3],
            'permalink': mention[4],
            'created': mention[5],
            'subreddit': mention[6],
            'author': mention[7],
            'score': mention[8],
            'sentiment': mention[9],
            'brand': mention[10],
            'source': mention[11]
        })
    
    return jsonify(results)

@app.route('/stats')
def get_stats():
    brand = request.args.get('brand', list(CONFIG['brands'].keys())[0])
    tz_offset = int(request.args.get('tz_offset', 0))
    
    with db_manager.get_connection() as conn:
        # Daily stats
        daily_query = '''
            SELECT type, COUNT(*) 
            FROM mentions 
            WHERE brand = ? AND DATE(created) = DATE('now') 
            GROUP BY type
        '''
        daily_cursor = conn.execute(daily_query, (brand,))
        daily_results = dict(daily_cursor.fetchall())
        
        # Total stats
        total_query = '''
            SELECT type, COUNT(*) 
            FROM mentions 
            WHERE brand = ? 
            GROUP BY type
        '''
        total_cursor = conn.execute(total_query, (brand,))
        total_results = dict(total_cursor.fetchall())
        
        # Sentiment stats
        sentiment_query = '''
            SELECT sentiment, COUNT(*) 
            FROM mentions 
            WHERE brand = ? AND sentiment IS NOT NULL 
            GROUP BY sentiment
        '''
        sentiment_cursor = conn.execute(sentiment_query, (brand,))
        sentiment_results = dict(sentiment_cursor.fetchall())
    
    # Calculate score (simple sentiment-based scoring)
    total_sentiment = sum(sentiment_results.values())
    if total_sentiment > 0:
        positive_ratio = sentiment_results.get('positive', 0) / total_sentiment
        negative_ratio = sentiment_results.get('negative', 0) / total_sentiment
        score = max(0, min(100, int((positive_ratio - negative_ratio + 1) * 50)))
    else:
        score = 50  # Neutral when no sentiment data
    
    return jsonify({
        'brand': brand,
        'daily': {
            'posts': daily_results.get('post', 0),
            'comments': daily_results.get('comment', 0)
        },
        'total': {
            'posts': total_results.get('post', 0),
            'comments': total_results.get('comment', 0)
        },
        'sentiment': {
            'positive': sentiment_results.get('positive', 0),
            'negative': sentiment_results.get('negative', 0),
            'neutral': sentiment_results.get('neutral', 0)
        },
        'score': score
    })

@app.route('/trending_subreddits')
def trending_subreddits():
    brand = request.args.get('brand')
    
    with db_manager.get_connection() as conn:
        if brand:
            query = '''
                SELECT subreddit, COUNT(*) as mention_count, GROUP_CONCAT(DISTINCT brand) as brands
                FROM mentions 
                WHERE brand = ?
                GROUP BY subreddit 
                ORDER BY mention_count DESC 
                LIMIT 20
            '''
            cursor = conn.execute(query, (brand,))
        else:
            query = '''
                SELECT subreddit, COUNT(*) as mention_count, GROUP_CONCAT(DISTINCT brand) as brands
                FROM mentions 
                GROUP BY subreddit 
                ORDER BY mention_count DESC 
                LIMIT 20
            '''
            cursor = conn.execute(query)
        
        results = cursor.fetchall()
    
    trending = []
    for subreddit, count, brands in results:
        trending.append({
            'subreddit': subreddit,
            'mention_count': count,
            'brands': brands.split(',') if brands else []
        })
    
    return jsonify(trending)

@app.route('/export')
def export_mentions():
    brand = request.args.get('brand')
    
    with db_manager.get_connection() as conn:
        if brand:
            query = "SELECT * FROM mentions WHERE brand = ? ORDER BY created DESC"
            cursor = conn.execute(query, (brand,))
        else:
            query = "SELECT * FROM mentions ORDER BY created DESC"
            cursor = conn.execute(query)
        
        mentions = cursor.fetchall()
    
    # Create CSV
    output = io.StringIO()
    writer = csv.writer(output)
    
    # Write header
    writer.writerow(['ID', 'Type', 'Title', 'Body', 'Permalink', 'Created', 'Subreddit', 'Author', 'Score', 'Sentiment', 'Brand', 'Source'])
    
    # Write data
    for mention in mentions:
        writer.writerow(mention)
    
    # Create file response
    csv_output = output.getvalue()
    output.close()
    
    return send_file(
        io.BytesIO(csv_output.encode('utf-8')),
        as_attachment=True,
        download_name=f'reddit_mentions_{brand or "all"}_{datetime.now().strftime("%Y%m%d")}.csv',
        mimetype='text/csv'
    )

@app.route('/system_status')
def system_status():
    with db_manager.get_connection() as conn:
        # Get total mentions
        total_cursor = conn.execute("SELECT COUNT(*) FROM mentions")
        total_mentions = total_cursor.fetchone()[0]
        
        # Get total brands
        brands_cursor = conn.execute("SELECT COUNT(DISTINCT brand) FROM mentions")
        total_brands = brands_cursor.fetchone()[0]
        
        # Get data sources
        source_query = '''
            SELECT source, COUNT(*) as count 
            FROM mentions 
            GROUP BY source
        '''
        source_cursor = conn.execute(source_query)
        sources = dict(source_cursor.fetchall())
    
    return jsonify({
        "total_mentions": total_mentions,
        "total_brands": total_brands,
        "sources": sources,
        "monitoring_active": reddit_monitor.running if reddit_monitor else False,
        "system_time": datetime.utcnow().isoformat()
    })

@app.route('/delete', methods=['POST'])
def delete_mention():
    data = request.get_json()
    mention_id = data.get('id')
    
    if not mention_id:
        return jsonify({"error": "Missing id"}), 400
    
    with db_manager.get_connection() as conn:
        cursor = conn.execute("DELETE FROM mentions WHERE id = ?", (mention_id,))
        conn.commit()
        
        if cursor.rowcount == 0:
            return jsonify({"error": "Mention not found"}), 404
    
    return jsonify({"status": "deleted", "id": mention_id})

@app.route('/favicon.ico')
def favicon():
    return '', 204  # No content, prevents 404 errors

@app.route('/download')
def download_csv():
    """Download mentions for current brand as CSV with specific format"""
    brand = request.args.get('brand')
    
    with db_manager.get_connection() as conn:
        if brand:
            query = "SELECT type, subreddit, author, permalink, created, title, body, sentiment FROM mentions WHERE brand = ? ORDER BY created DESC"
            cursor = conn.execute(query, (brand,))
        else:
            query = "SELECT type, subreddit, author, permalink, created, title, body, sentiment FROM mentions ORDER BY created DESC"
            cursor = conn.execute(query)
        
        mentions = cursor.fetchall()
    
    # Create CSV output
    output = io.StringIO()
    writer = csv.writer(output, delimiter='\t')  # Using tab delimiter as requested
    
    # Write header with requested format
    writer.writerow(['Type', 'Subreddit', 'Author', 'Link', 'Created', 'Preview', 'Sentiment'])
    
    # Write data
    for mention in mentions:
        type_val, subreddit, author, permalink, created, title, body, sentiment = mention
        # Create preview from body (actual content) or title
        preview = body if body else (title if title else "")
        # Truncate preview if too long
        if len(preview) > 200:
            preview = preview[:200] + "..."
        
        writer.writerow([
            type_val,
            subreddit,
            author,
            f"https://reddit.com{permalink}" if permalink and not permalink.startswith('http') else permalink,
            created,
            preview,
            sentiment or "neutral"
        ])
    
    # Create response
    csv_output = output.getvalue()
    output.close()
    
    response = app.response_class(
        csv_output,
        mimetype='text/csv',
        headers={
            'Content-Disposition': f'attachment; filename=reddit_mentions_{brand or "all"}_{datetime.now().strftime("%Y%m%d")}.csv'
        }
    )
    
    return response

@app.route('/test-route')
def test_route():
    """Simple test route to verify Flask routing is working"""
    return jsonify({"status": "success", "message": "Flask routing is working!", "timestamp": datetime.utcnow().isoformat()})

@app.route('/backfill/<subreddit>')
def backfill_subreddit(subreddit):
    """Manually backfill recent mentions from a specific subreddit"""
    print(f"🔄 Backfill requested for subreddit: {subreddit}")
    try:
        # Get recent posts from the subreddit (last 25 posts)
        import praw
        
        reddit = praw.Reddit(
            client_id=CONFIG['reddit']['client_id'],
            client_secret=CONFIG['reddit']['client_secret'],
            user_agent=CONFIG['reddit']['user_agent']
        )
        
        found_mentions = 0
        processed = 0
        
        # Check recent posts
        for submission in reddit.subreddit(subreddit).new(limit=25):
            processed += 1
            # Check if this post contains brand mentions
            for brand_name, brand_pattern_str in CONFIG['brands'].items():
                title_text = f"{submission.title} {submission.selftext}"
                brand_pattern = re.compile(brand_pattern_str, re.IGNORECASE)
                if brand_pattern.search(title_text):
                    # This is a brand mention - save it
                    db_manager.add_mention(
                        brand_name, submission.title, title_text[:500], 
                        f"r/{subreddit}", f"https://reddit.com{submission.permalink}",
                        "neutral", submission.created_utc
                    )
                    found_mentions += 1
                    
            # Check recent comments on this post
            submission.comments.replace_more(limit=0)
            for comment in submission.comments.list()[:10]:  # Latest 10 comments
                for brand_name, brand_pattern_str in CONFIG['brands'].items():
                    brand_pattern = re.compile(brand_pattern_str, re.IGNORECASE)
                    if brand_pattern.search(comment.body):
                        db_manager.add_mention(
                            brand_name, f"Comment on: {submission.title}", 
                            comment.body[:500], f"r/{subreddit}",
                            f"https://reddit.com{comment.permalink}",
                            "neutral", comment.created_utc
                        )
                        found_mentions += 1
        
        return jsonify({
            "status": "success",
            "processed": processed,
            "found_mentions": found_mentions,
            "subreddit": subreddit
        })
        
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

@app.route('/weekly_mentions')
def weekly_mentions():
    """Get weekly mention counts for charts"""
    brand = request.args.get('brand', 'badinka')
    tz = request.args.get('tz', 'UTC')
    week_offset = int(request.args.get('week_offset', 0))
    
    # Calculate the start of the week (Monday)
    today = datetime.now()
    days_since_monday = today.weekday()
    monday = today - timedelta(days=days_since_monday) + timedelta(weeks=week_offset)
    monday = monday.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Get mentions for the week
    with db_manager.get_connection() as conn:
        query = '''
            SELECT DATE(created) as date, COUNT(*) as count
            FROM mentions 
            WHERE brand = ? 
            AND created >= ? 
            AND created < ?
            GROUP BY DATE(created)
            ORDER BY date
        '''
        
        start_date = monday.isoformat()
        end_date = (monday + timedelta(days=7)).isoformat()
        
        cursor = conn.execute(query, (brand, start_date, end_date))
        results = cursor.fetchall()
    
    # Convert to dictionary format expected by frontend
    weekly_data = {}
    for date_str, count in results:
        # Convert to frontend format (YYYY-MM-DD)
        weekly_data[date_str] = count
    
    return jsonify(weekly_data)

@app.route('/system-health')
def system_health_status():
    """Get detailed system health information"""
    if not reddit_monitor:
        return jsonify({"error": "Reddit monitor not initialized"}), 500
    
    try:
        # Get system health
        healthy_systems = sum(reddit_monitor.system_health.values())
        total_systems = len(reddit_monitor.system_health)
        health_percentage = (healthy_systems / total_systems) * 100
        
        # Get error statistics
        error_stats = {}
        for source, count in reddit_monitor.error_tracker.error_counts.items():
            if count > 0:
                error_stats[source] = {
                    'error_count': count,
                    'last_error': reddit_monitor.error_tracker.last_error_time.get(source, 0),
                    'backoff_delay': reddit_monitor.error_tracker.backoff_delays.get(source, 0),
                    'circuit_breaker_until': reddit_monitor.error_tracker.circuit_breaker_until.get(source, 0)
                }
        
        # Get queue status
        queue_size = reddit_monitor.failed_request_queue.size()
        
        return jsonify({
            "overall_health": f"{health_percentage:.0f}%",
            "healthy_systems": healthy_systems,
            "total_systems": total_systems,
            "system_status": reddit_monitor.system_health,
            "error_statistics": error_stats,
            "failed_request_queue_size": queue_size,
            "rss_backup_active": reddit_monitor.rss_backup.active if reddit_monitor.rss_backup else False,
            "timestamp": datetime.utcnow().isoformat()
        })
        
    except Exception as e:
        return jsonify({"error": f"Health check failed: {str(e)}"}), 500



# HTML Template (embedded) - User's Preferred Version
HTML_TEMPLATE = '''
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <title>Reddit Brand Monitoring</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/jspdf/2.5.1/jspdf.umd.min.js"></script>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js"></script>
  <style>
    body { font-family: sans-serif; margin: 20px; background-color: #f9f9f9; color: #333; max-width: 100%; overflow-x: hidden; }
    h1, h2, h3 { color: #222; }
    table { width: 100%; border-collapse: collapse; margin-top: 20px; background-color: #fff; table-layout: fixed; }
    th, td { border: 1px solid #ccc; padding: 8px; text-align: left; word-wrap: break-word; overflow: hidden; }
    th { background-color: #f0f0f0; }
    button { padding: 6px 12px; font-size: 14px; cursor: pointer; border-radius: 4px; border: none; background-color: #007bff; color: white; }
    button:disabled { background-color: #aaa; cursor: not-allowed; }
    .badge { padding: 2px 6px; border-radius: 4px; color: white; font-size: 12px; text-transform: capitalize; }
    .positive { background-color: limegreen; }
    .neutral  { background-color: gray; }
    .negative { background-color: red; }
    #brand-buttons { margin-bottom: 20px; }
    #brand-buttons button { margin-right: 10px; }
    #stats-tab { display: none; }
    .stats-container { display: flex; gap: 30px; align-items: flex-start; margin-top: 20px; }
    .stat-block { flex: 1; background: #fff; padding: 16px; border: 1px solid #ddd; border-radius: 6px; }
    .pdf-btn, .csv-btn { display: inline-block; margin-top: 10px; }
    .score-label { font-weight: bold; color: white; padding: 4px 8px; border-radius: 4px; display: inline-block; }
    .charts-table { width: 100%; table-layout: fixed; margin-top: 20px; }
    .charts-table td { text-align: center; vertical-align: top; }
    .charts-table canvas { width: 300px; height: 300px; }
    #data-table th:nth-child(1) { width: 8%; } /* Type */
    #data-table th:nth-child(2) { width: 12%; } /* Subreddit */
    #data-table th:nth-child(3) { width: 12%; } /* Author */
    #data-table th:nth-child(4) { width: 8%; } /* Link */
    #data-table th:nth-child(5) { width: 15%; } /* Created */
    #data-table th:nth-child(6) { width: 30%; } /* Preview */
    #data-table th:nth-child(7) { width: 10%; } /* Sentiment */
    #data-table th:nth-child(8) { width: 5%; } /* Action */
  </style>
</head>
<body>
  <h1>Reddit Brand Monitoring</h1>
     <div id="brand-buttons">
     <button id="btn-badinka" onclick="switchBrand('badinka')">Badinka</button>
     <button id="btn-devilwalking" onclick="switchBrand('devilwalking')">Devil Walking</button>
     <button id="btn-stats" onclick="showStats()">Stats</button>
   </div>
  <p class="csv-btn">
    <button id="csv-btn" onclick="downloadCurrentBrandCSV()">📥 Download CSV</button>
    <button id="pdf-btn" style="display:none;" onclick="downloadPDF()">📄 Download as PDF</button>
  </p>

  <div id="mentions-tab">
    <table id="data-table">
      <thead>
        <tr>
          <th>Type</th>
          <th>Subreddit</th>
          <th>Author</th>
          <th>Link</th>
          <th>Created</th>
          <th>Preview</th>
          <th>Sentiment</th>
          <th>Action</th>
        </tr>
      </thead>
      <tbody></tbody>
    </table>
  </div>

  <div id="stats-tab">
    <h2>Head to head stats</h2>
    <div class="stats-container">
      <div id="stats-left" class="stat-block"></div>
      <div id="stats-right" class="stat-block"></div>
    </div>
    <table class="charts-table">
      <tr>
        <td><canvas id="pie-left"></canvas></td>
        <td><canvas id="pie-right"></canvas></td>
      </tr>
      <tr>
        <td>
          <div style="text-align: center; margin-top: 30px;">
            <button onclick="changeWeek(-1)">⬅️ Previous Week</button>
            <span id="week-label-left" style="margin: 0 20px; font-weight: bold;">This Week</span>
            <button onclick="changeWeek(1)">Next Week ➡️</button>
          </div>
          <canvas id="bar-left" style="margin-top: 20px; height: 300px;"></canvas>
        </td>
        <td>
          <div style="text-align: center; margin-top: 30px;">
            <button onclick="changeWeek(-1)">⬅️ Previous Week</button>
            <span id="week-label-right" style="margin: 0 20px; font-weight: bold;">This Week</span>
            <button onclick="changeWeek(1)">Next Week ➡️</button>
          </div>
          <canvas id="bar-right" style="margin-top: 20px; height: 300px;"></canvas>
        </td>
      </tr>
    </table>
  </div>

  <script>
    let currentBrand = "badinka";
    let charts = {};
    let barCharts = { left: null, right: null };
    let weekOffset = 0;

         function switchBrand(brand) {
       currentBrand = brand;
       document.getElementById("mentions-tab").style.display = "block";
       document.getElementById("stats-tab").style.display = "none";
       document.getElementById("btn-badinka").disabled = (brand === "badinka");
       document.getElementById("btn-devilwalking").disabled = (brand === "devilwalking");
       document.getElementById("btn-stats").disabled = false;
       document.getElementById("csv-btn").style.display = 'inline-block';
       document.getElementById("pdf-btn").style.display = 'none';
       loadData();
     }

         function showStats() {
       document.getElementById("mentions-tab").style.display = "none";
       document.getElementById("stats-tab").style.display = "block";
       document.getElementById("btn-badinka").disabled = false;
       document.getElementById("btn-devilwalking").disabled = false;
       document.getElementById("btn-stats").disabled = true;
       document.getElementById("csv-btn").style.display = 'none';
       document.getElementById("pdf-btn").style.display = 'inline-block';
       loadStats();
     }

    function changeWeek(offset) {
      weekOffset += offset;
      loadWeeklyCharts();
    }

    function getMonday(offset) {
      const today = new Date();
      today.setHours(0, 0, 0, 0);
      const monday = new Date(today);
      monday.setDate(monday.getDate() - ((monday.getDay() + 6) % 7) + 7 * offset);
      return monday;
    }

    function updateWeekLabel(monday) {
      const sunday = new Date(monday);
      sunday.setDate(monday.getDate() + 6);
      const label = `${monday.toLocaleDateString()} - ${sunday.toLocaleDateString()}`;
      document.getElementById("week-label-left").textContent = label;
      document.getElementById("week-label-right").textContent = label;
    }

    function loadData() {
      fetch(`/data?brand=${currentBrand}`)
        .then(res => res.json())
        .then(data => {
          const tbody = document.querySelector("#data-table tbody");
          tbody.innerHTML = "";
          data.sort((a, b) => new Date(b.created) - new Date(a.created));
          data.forEach(item => {
            const sentiment = item.sentiment || "neutral";
            const badge = `<span class="badge ${sentiment}">${sentiment}</span>`;
            const row = document.createElement("tr");
            row.innerHTML = `
              <td>${item.type}</td>
              <td>${item.subreddit}</td>
              <td>${item.author}</td>
              <td><a href="${item.permalink}" target="_blank">View</a></td>
              <td>${new Date(item.created).toLocaleString()}</td>
              <td>${item.body || item.title || ""}</td>
              <td>${badge}</td>
              <td><button onclick="deleteEntry('${item.id}')">🗑️ Delete</button></td>`;
            tbody.appendChild(row);
          });
        });
    }

    function deleteEntry(id) {
      if (!confirm("Are you sure you want to delete this entry?")) return;
      fetch("/delete", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ id })
      }).then(() => loadData());
    }

    function downloadCurrentBrandCSV() {
      window.location.href = `/download?brand=${currentBrand}`;
    }

         function loadStats() {
       const tzOffset = new Date().getTimezoneOffset();
       fetch(`/stats?brand=badinka&tz_offset=${tzOffset}`).then(res => res.json()).then(data => renderStats(data, "left"));
       fetch(`/stats?brand=devilwalking&tz_offset=${tzOffset}`).then(res => res.json()).then(data => renderStats(data, "right"));
       loadWeeklyCharts();
     }

    function renderStats(data, side) {
      const totalToday = data.daily.posts + data.daily.comments;
      const totalAll = data.total.posts + data.total.comments;
      const perception = data.score > 60 ? 'positive' : data.score >= 40 ? 'neutral' : 'negative';

      const container = document.getElementById(`stats-${side}`);
      container.innerHTML = `
        <h3>${data.brand}</h3>
        <table>
          <tr><th colspan="2">Today</th></tr>
          <tr><td>Posts</td><td>${data.daily.posts}</td></tr>
          <tr><td>Comments</td><td>${data.daily.comments}</td></tr>
          <tr><td><strong>Total Today</strong></td><td><strong>${totalToday}</strong></td></tr>
          <tr><th colspan="2">All Time</th></tr>
          <tr><td>Posts</td><td>${data.total.posts}</td></tr>
          <tr><td>Comments</td><td>${data.total.comments}</td></tr>
          <tr><td><strong>Total</strong></td><td><strong>${totalAll}</strong></td></tr>
          <tr><th>Brand Perception Score</th><td><span class="score-label ${perception}"><strong>${data.score}/100</strong></span></td></tr>
        </table>`;

      const ctx = document.getElementById(`pie-${side}`).getContext("2d");
      if (charts[side]) charts[side].destroy();
      charts[side] = new Chart(ctx, {
        type: "pie",
        data: {
          labels: ["Positive", "Neutral", "Negative"],
          datasets: [{
            data: [data.sentiment.positive, data.sentiment.neutral, data.sentiment.negative],
            backgroundColor: ["limegreen", "gray", "red"]
          }]
        },
        options: { plugins: { legend: { position: "bottom" } }, responsive: true }
      });
    }

    function loadWeeklyCharts() {
      const tz = Intl.DateTimeFormat().resolvedOptions().timeZone;
      const monday = getMonday(weekOffset);
      updateWeekLabel(monday);

      const days = Array.from({ length: 7 }, (_, i) => {
        const d = new Date(monday);
        d.setDate(monday.getDate() + i);
        d.setHours(0, 0, 0, 0);
        return d;
      });

      const labels = days.map(d => d.toLocaleDateString());
      const keys = days.map(d => d.toLocaleDateString('en-CA'));  // en-CA = YYYY-MM-DD

             Promise.all([
         fetch(`/weekly_mentions?brand=badinka&tz=${tz}&week_offset=${weekOffset}`).then(res => res.json()),
         fetch(`/weekly_mentions?brand=devilwalking&tz=${tz}&week_offset=${weekOffset}`).then(res => res.json())
       ]).then(([leftData, rightData]) => {
        const leftValues = keys.map(key => leftData[key] || 0);
        const rightValues = keys.map(key => rightData[key] || 0);
        const maxY = Math.max(...leftValues, ...rightValues, 1);
        drawBarChart("left", labels, leftValues, maxY);
        drawBarChart("right", labels, rightValues, maxY);
      });
    }

    function drawBarChart(side, labels, values, maxY) {
      const ctx = document.getElementById(`bar-${side}`).getContext("2d");
      if (barCharts[side]) barCharts[side].destroy();
      barCharts[side] = new Chart(ctx, {
        type: "bar",
        data: {
          labels: labels,
          datasets: [{
            label: "Mentions",
            data: values,
            backgroundColor: "#007bff"
          }]
        },
        options: {
          responsive: true,
          scales: {
            y: { beginAtZero: true, max: maxY, title: { display: true, text: "Mentions Count" } },
            x: { title: { display: true, text: "Date" } }
          }
        }
      });
    }

    function downloadPDF() {
      const { jsPDF } = window.jspdf;
      html2canvas(document.querySelector("#stats-tab")).then(canvas => {
        const doc = new jsPDF();
        const img = canvas.toDataURL("image/png");
        doc.addImage(img, "PNG", 0, 0, doc.internal.pageSize.getWidth(), doc.internal.pageSize.getHeight());
        doc.save("brand-stats.pdf");
      });
    }

    switchBrand(currentBrand);
    setInterval(loadData, 30000);
  </script>
</body>
</html>
'''

def run_monitoring_thread():
    """Run monitoring in a separate thread"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(reddit_monitor.start_monitoring())

def main():
    global db_manager, reddit_monitor
    
    print("🚀 Starting All-in-One Reddit Brand Monitor v2.0 - WITH BACKFILL FEATURE...")
    print(f"🔧 Python version: {os.sys.version}")
    print(f"🔧 Working directory: {os.getcwd()}")
    print(f"🔧 PORT environment variable: {os.getenv('PORT', 'Not set')}")
    
    try:
        # Start with just Flask to ensure basic functionality
        print("🔄 Starting Flask server first...")
        port = int(os.getenv('PORT', CONFIG['port']))  # CRITICAL: Use Railway's PORT
        print(f"🌐 Web interface will start on port {port}")
        
        # Initialize minimal components
        print("🔄 Initializing database...")
        
        # Ensure data directory exists for persistent storage
        data_dir = os.path.dirname(CONFIG['database_file'])
        
        # Create data directory with multiple fallback strategies
        if not os.path.exists(data_dir):
            try:
                os.makedirs(data_dir, mode=0o755, exist_ok=True)
                print(f"📁 Created data directory: {data_dir}")
            except Exception as e:
                print(f"⚠️ Could not create data directory {data_dir}: {e}")
                # Fallback to current directory
                CONFIG['database_file'] = 'reddit_monitor.db'
                print(f"🔄 Falling back to current directory: {CONFIG['database_file']}")
        
        # Debug: Check volume mounting and permissions
        print(f"🔍 Data directory: {data_dir}")
        print(f"🔍 Data directory exists: {os.path.exists(data_dir)}")
        if os.path.exists(data_dir):
            print(f"🔍 Data directory permissions: {oct(os.stat(data_dir).st_mode)[-3:]}")
            print(f"🔍 Data directory writable: {os.access(data_dir, os.W_OK)}")
        
        print(f"🔍 Database file path: {CONFIG['database_file']}")
        print(f"🔍 Database file exists: {os.path.exists(CONFIG['database_file'])}")
        if os.path.exists(CONFIG['database_file']):
            print(f"🔍 Database file size: {os.path.getsize(CONFIG['database_file'])} bytes")
            # Count existing mentions to verify persistence
            try:
                import sqlite3
                conn = sqlite3.connect(CONFIG['database_file'])
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM mentions")
                count = cursor.fetchone()[0]
                conn.close()
                print(f"🔍 Existing mentions in database: {count}")
            except Exception as e:
                print(f"🔍 Could not count existing mentions: {e}")
        else:
            print(f"🔍 Database file will be created at: {CONFIG['database_file']}")
        
        # Initialize database
        db_manager = DatabaseManager(CONFIG['database_file'])
        print(f"✅ Database initialized: {CONFIG['database_file']}")
        
        # Initialize Reddit monitor but don't start monitoring yet
        print("🔄 Initializing Reddit monitor...")
        reddit_monitor = RedditMonitor(CONFIG, db_manager)
        print("✅ Reddit monitor initialized (monitoring will start after Flask)")
        
        # Start Flask first, then monitoring
        print("✅ Starting Flask server...")
        
        # Start monitoring in background after a delay
        def delayed_monitoring_start():
            import time
            time.sleep(10)  # Wait 10 seconds for Flask to fully start
            try:
                print("🔄 Starting background monitoring...")
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(reddit_monitor.start_monitoring())
            except Exception as e:
                print(f"⚠️ Background monitoring failed to start: {e}")
        
        monitoring_thread = threading.Thread(target=delayed_monitoring_start, daemon=True)
        monitoring_thread.start()
        
        # Start Flask - this should work since our test worked
        print(f"🔍 Health check: http://localhost:{port}/health")
        
        # Log all registered routes for debugging
        print("📋 Registered Flask routes:")
        for rule in app.url_map.iter_rules():
            print(f"   {rule.rule} -> {rule.endpoint}")
        
        app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
        
    except Exception as e:
        print(f"❌ Critical error during startup: {e}")
        import traceback
        traceback.print_exc()
        # Try to start Flask anyway with minimal functionality
        try:
            print(f"🚨 Attempting emergency Flask start on port {port}...")
            app.run(host='0.0.0.0', port=port, debug=False)
        except:
            print("💥 Emergency Flask start also failed")
            raise
    except KeyboardInterrupt:
        print("\n⏹️  Shutting down...")
        if 'reddit_monitor' in globals():
            reddit_monitor.stop_monitoring()

if __name__ == "__main__":
    main()
