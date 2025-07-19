#!/usr/bin/env python3
"""
All-in-One Reddit Brand Monitor
- Single file deployment
- Embedded SQLite database
- Built-in web interface
- Real-time monitoring
- Perfect for commercial deployment
- Now monitoring ALL of Reddit!
"""

import asyncio
import aiohttp
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

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
CONFIG = {
    'database_file': '/app/data/reddit_monitor.db',
    'reddit': {
        'client_id': os.getenv('REDDIT_CLIENT_ID', ''),
        'client_secret': os.getenv('REDDIT_CLIENT_SECRET', ''),
        'user_agent': 'AllInOneBrandMonitor/1.0'
    },
    'hf_api_token': os.getenv('HF_API_TOKEN', ''),
    'brands': {
        'badinka': r'[@#]?badinka(?:\.com)?',
        'devilwalking': r'[@#]?devil\s*walking(?:\.com)?'  # Matches both "devilwalking" and "devil walking"
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

class SentimentAnalyzer:
    def __init__(self, api_token: str):
        self.api_token = api_token
        self.api_url = "https://api-inference.huggingface.co/models/cardiffnlp/twitter-roberta-base-sentiment"
        self.headers = {"Authorization": f"Bearer {api_token}"} if api_token else {}
    
    async def analyze(self, context_text: str) -> str:
        """Analyze sentiment of brand-focused context text"""
        if not self.api_token or not context_text.strip():
            return "neutral"
        
        try:
            async with aiohttp.ClientSession() as session:
                # Limit text length for API efficiency (focus on most relevant part)
                focused_text = context_text[:400]  # Slightly shorter for better focus
                payload = {"inputs": focused_text}
                
                async with session.post(self.api_url, headers=self.headers, json=payload) as response:
                    if response.status == 200:
                        result = await response.json()
                        if result and isinstance(result, list) and result[0]:
                            scores = result[0]
                            best_score = max(scores, key=lambda x: x['score'])
                            label = best_score['label'].lower()
                            confidence = best_score['score']
                            
                            # Map labels to our sentiment system
                            if 'positive' in label:
                                sentiment = 'positive'
                            elif 'negative' in label:
                                sentiment = 'negative'
                            else:
                                sentiment = 'neutral'
                            
                            logger.debug(f"🎯 Sentiment: {sentiment} (confidence: {confidence:.2f})")
                            return sentiment
                    else:
                        logger.warning(f"HuggingFace API returned status {response.status}")
        except Exception as e:
            logger.error(f"Sentiment analysis error: {e}")
        
        return "neutral"

class RedditMonitor:
    def __init__(self, config: dict, db: DatabaseManager):
        self.config = config
        self.db = db
        self.brands = {
            name: re.compile(pattern, re.IGNORECASE)
            for name, pattern in config['brands'].items()
        }
        self.sentiment = SentimentAnalyzer(config['hf_api_token'])
        self.seen_ids = self.db.get_existing_ids()
        self.mention_buffer: List[Mention] = []
        self.running = False
        
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
                    mention.sentiment = await self.sentiment.analyze(context_text)
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
                        async with aiohttp.ClientSession() as session:
                            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                                if response.status == 200:
                                    rss_text = await response.text()
                                    logger.debug(f"📡 Fetched RSS for r/{subreddit} - {len(rss_text)} bytes")
                                    await self._process_rss_feed(rss_text, subreddit)
                                else:
                                    logger.warning(f"RSS r/{subreddit} returned status {response.status}")
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
                        async with aiohttp.ClientSession() as session:
                            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                                if response.status == 200:
                                    data = await response.json()
                                    comment_count = len(data.get('data', {}).get('children', []))
                                    logger.debug(f"📊 JSON API r/all: {comment_count} comments")
                                    await self._process_json_comments(data)
                                elif response.status == 429:
                                    logger.warning("JSON API rate limited")
                                    await asyncio.sleep(60)
                                else:
                                    logger.warning(f"JSON API returned status {response.status}")
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
                            async with aiohttp.ClientSession() as session:
                                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                                    if response.status == 200:
                                        data = await response.json()
                                        await self._process_json_comments(data)
                                    elif response.status == 429:
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
        
        logger.info("Starting PRAW monitoring...")
        
        # Start both comment and post monitoring in separate threads
        comment_thread = threading.Thread(target=self._monitor_praw_comments, daemon=True)
        post_thread = threading.Thread(target=self._monitor_praw_posts, daemon=True)
        
        comment_thread.start()
        post_thread.start()
        
        # Wait for threads to complete
        comment_thread.join()
        post_thread.join()
    
    def _monitor_praw_comments(self):
        """Monitor comments using PRAW"""
        logger.info("Starting PRAW comment monitoring...")
        
        while self.running:
            try:
                # Monitor all Reddit comments
                if self.config.get('monitor_all_reddit', False):
                    subreddit = self.reddit.subreddit("all")
                else:
                    subreddit = self.reddit.subreddit("+".join(self.config['subreddits']))
                comment_stream = subreddit.stream.comments(skip_existing=True, pause_after=5)
                
                for comment in comment_stream:
                    if not self.running:
                        break
                    
                    if comment is None:
                        time.sleep(1)
                        continue
                    
                    if comment.id in self.seen_ids:
                        continue
                    
                    # Debug: Log every 100th comment to see what we're processing
                    if hasattr(self, '_comment_count'):
                        self._comment_count += 1
                    else:
                        self._comment_count = 1
                    
                    if self._comment_count % 100 == 0:
                        logger.info(f"🔍 Processed {self._comment_count} comments, checking: '{comment.body[:50]}...'")
                    
                    brands = self.find_brands(comment.body)
                    if brands:
                        for brand in brands:
                            mention = Mention(
                                id=comment.id,
                                type="comment",
                                title=None,
                                body=comment.body,
                                permalink=f"https://reddit.com{comment.permalink}",
                                created=datetime.fromtimestamp(comment.created_utc, tz=timezone.utc).isoformat(),
                                subreddit=str(comment.subreddit),
                                author=str(comment.author),
                                score=comment.score,
                                sentiment=None,
                                brand=brand,
                                source="praw"
                            )
                            
                            self.mention_buffer.append(mention)
                            self.seen_ids.add(comment.id)
                            logger.info(f"Found PRAW mention: {brand} in r/{comment.subreddit}")
                    
                    # Flush buffer periodically
                    if len(self.mention_buffer) >= 10:
                        # Process buffer with sentiment analysis
                        if self.mention_buffer:
                            self._process_praw_buffer_with_sentiment()
                            self.mention_buffer.clear()
                
            except prawcore.exceptions.TooManyRequests:
                logger.warning("PRAW rate limited, sleeping 60 seconds")
                time.sleep(60)
            except Exception as e:
                logger.error(f"PRAW comment monitoring error: {e}")
                time.sleep(30)
    
    def _monitor_praw_posts(self):
        """Monitor posts using PRAW"""
        logger.info("Starting PRAW post monitoring...")
        
        while self.running:
            try:
                # Monitor all Reddit posts
                if self.config.get('monitor_all_reddit', False):
                    subreddit = self.reddit.subreddit("all")
                else:
                    subreddit = self.reddit.subreddit("+".join(self.config['subreddits']))
                
                post_stream = subreddit.stream.submissions(skip_existing=True, pause_after=5)
                
                for post in post_stream:
                    if not self.running:
                        break
                    
                    if post is None:
                        time.sleep(1)
                        continue
                    
                    if post.id in self.seen_ids:
                        continue
                    
                    # Debug: Log every 50th post to see what we're processing
                    if hasattr(self, '_post_count'):
                        self._post_count += 1
                    else:
                        self._post_count = 1
                    
                    if self._post_count % 50 == 0:
                        logger.info(f"📝 Processed {self._post_count} posts, checking: '{post.title[:50]}...'")
                    
                    # Check both title and selftext for brand mentions
                    full_text = f"{post.title} {post.selftext}"
                    brands = self.find_brands(full_text)
                    
                    if brands:
                        for brand in brands:
                            mention = Mention(
                                id=post.id,
                                type="post",
                                title=post.title,
                                body=post.selftext,
                                permalink=f"https://reddit.com{post.permalink}",
                                created=datetime.fromtimestamp(post.created_utc, tz=timezone.utc).isoformat(),
                                subreddit=str(post.subreddit),
                                author=str(post.author),
                                score=post.score,
                                sentiment=None,
                                brand=brand,
                                source="praw"
                            )
                            
                            self.mention_buffer.append(mention)
                            self.seen_ids.add(post.id)
                            logger.info(f"🎉 Found PRAW post mention: {brand} in r/{post.subreddit}")
                    
                    # Flush buffer periodically
                    if len(self.mention_buffer) >= 10:
                        # Process buffer with sentiment analysis
                        if self.mention_buffer:
                            self._process_praw_buffer_with_sentiment()
                            self.mention_buffer.clear()
                
            except prawcore.exceptions.TooManyRequests:
                logger.warning("PRAW posts rate limited, sleeping 60 seconds")
                time.sleep(60)
            except Exception as e:
                logger.error(f"PRAW post monitoring error: {e}")
                time.sleep(30)
    
    def _process_praw_buffer_with_sentiment(self):
        """Process PRAW mention buffer with sentiment analysis (synchronous)"""
        if not self.mention_buffer:
            return
        
        # Add sentiment analysis for each mention
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            for mention in self.mention_buffer:
                if mention.sentiment is None:
                    # Extract context around the brand mention for focused sentiment analysis
                    context_text = self._extract_brand_context(mention)
                    if context_text:
                        # Run sentiment analysis on brand-focused context
                        mention.sentiment = loop.run_until_complete(self.sentiment.analyze(context_text))
                        logger.debug(f"💭 Analyzed sentiment for '{mention.brand}': {mention.sentiment}")
                    else:
                        mention.sentiment = "neutral"  # Fallback if no context found
            
            # Save to database
            self.db.insert_mentions(self.mention_buffer)
            logger.info(f"💭 Processed {len(self.mention_buffer)} brand mentions with sentiment analysis")
        
        finally:
            loop.close()
    
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
     
    async def monitor_focused_subreddits(self):
        """Monitor high-priority subreddits for extra coverage"""
        logger.info("Starting focused subreddits monitoring...")
        
        while self.running:
            try:
                for subreddit in self.config.get('focused_subreddits', []):
                    if not self.running:
                        break
                    
                    # Monitor both posts and comments for each focused subreddit
                    await self._monitor_subreddit_posts(subreddit)
                    await self._monitor_subreddit_comments(subreddit)
                    
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
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status == 200:
                        data = await response.json()
                        await self._process_subreddit_posts(data, subreddit)
                    elif response.status == 429:
                        logger.warning(f"Rate limited for r/{subreddit}")
                        await asyncio.sleep(30)
                        
        except Exception as e:
            logger.error(f"Error monitoring r/{subreddit} posts: {e}")
    
    async def _monitor_subreddit_comments(self, subreddit: str):
        """Monitor comments in a specific subreddit"""
        try:
            url = f"https://www.reddit.com/r/{subreddit}/comments.json?limit=25"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status == 200:
                        data = await response.json()
                        await self._process_json_comments(data)
                    elif response.status == 429:
                        logger.warning(f"Rate limited for r/{subreddit}")
                        await asyncio.sleep(30)
                        
        except Exception as e:
            logger.error(f"Error monitoring r/{subreddit} comments: {e}")
    
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

    def stop_monitoring(self):
        """Stop monitoring"""
        self.running = False
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
    """Download all mentions as CSV"""
    brand = request.args.get('brand')
    
    with db_manager.get_connection() as conn:
        if brand:
            query = "SELECT * FROM mentions WHERE brand = ? ORDER BY created DESC"
            cursor = conn.execute(query, (brand,))
        else:
            query = "SELECT * FROM mentions ORDER BY created DESC"
            cursor = conn.execute(query)
        
        mentions = cursor.fetchall()
    
    # Create CSV output
    output = io.StringIO()
    writer = csv.writer(output)
    
    # Write header
    writer.writerow(['ID', 'Type', 'Brand', 'Subreddit', 'Author', 'Title', 'Body', 
                     'Permalink', 'Created', 'Score', 'Sentiment', 'Source'])
    
    # Write data
    for mention in mentions:
        writer.writerow(mention)
    
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
            for brand_name, brand_pattern in CONFIG['brands'].items():
                title_text = f"{submission.title} {submission.selftext}"
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
                for brand_name, brand_pattern in CONFIG['brands'].items():
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
    body { font-family: sans-serif; margin: 20px; background-color: #f9f9f9; color: #333; }
    h1, h2, h3 { color: #222; }
    table { width: 100%; border-collapse: collapse; margin-top: 20px; background-color: #fff; }
    th, td { border: 1px solid #ccc; padding: 8px; text-align: left; }
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
  </style>
</head>
<body>
  <h1>Reddit Brand Monitoring - ALL OF REDDIT 🌍</h1>
     <div id="brand-buttons">
     <button id="btn-badinka" onclick="switchBrand('badinka')">Badinka</button>
     <button id="btn-devilwalking" onclick="switchBrand('devilwalking')">Devil Walking</button>
     <button id="btn-stats" onclick="showStats()">Stats</button>
   </div>
  <p class="csv-btn">
    <button id="csv-btn" onclick="window.location.href='/download'">📥 Download CSV</button>
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
        data_dir = '/app/data'
        if not os.path.exists(data_dir):
            try:
                os.makedirs(data_dir, exist_ok=True)
                print(f"📁 Created data directory: {data_dir}")
            except Exception as e:
                print(f"⚠️ Could not create data directory: {e}")
        
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