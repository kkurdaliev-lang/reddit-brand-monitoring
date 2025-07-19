#!/usr/bin/env python3
"""
All-in-One Reddit Brand Monitor
- Single file deployment
- Embedded SQLite database
- Built-in web interface
- Real-time monitoring
- Perfect for commercial deployment
"""

import asyncio
import aiohttp
import sqlite3
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
from flask import Flask, render_template, render_template_string, jsonify, request, send_file
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
    'database_file': 'reddit_monitor.db',
    'reddit': {
        'client_id': os.getenv('REDDIT_CLIENT_ID', ''),
        'client_secret': os.getenv('REDDIT_CLIENT_SECRET', ''),
        'user_agent': 'AllInOneBrandMonitor/1.0'
    },
    'hf_api_token': os.getenv('HF_API_TOKEN', ''),
    'brands': {
        'badinka': r'[@#]?badinka(?:\.com)?',
        'iheartraves': r'[@#]?iheartraves(?:\.com)?'
    },
    'monitor_all_reddit': True,  # Monitor all of Reddit, not just specific subreddits
    'subreddits': [
        # These are fallback/priority subreddits if needed
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
            conn.execute('CREATE INDEX IF NOT EXISTS idx_type ON mentions(type)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_subreddit ON mentions(subreddit)')
            
            conn.commit()
            logger.info("Database initialized successfully")
    
    @contextmanager
    def get_connection(self):
        """Get database connection with context manager"""
        conn = sqlite3.connect(self.db_file, timeout=30.0)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()
    
    def insert_mentions(self, mentions: List[Mention]):
        """Insert mentions into database"""
        if not mentions:
            return
        
        with self.get_connection() as conn:
            for mention in mentions:
                conn.execute('''
                    INSERT OR REPLACE INTO mentions 
                    (id, type, title, body, permalink, created, subreddit, author, 
                     score, sentiment, brand, source)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    mention.id, mention.type, mention.title, mention.body,
                    mention.permalink, mention.created, mention.subreddit,
                    mention.author, mention.score, mention.sentiment,
                    mention.brand, mention.source
                ))
            
            conn.commit()
            logger.info(f"Inserted {len(mentions)} mentions")
    
    def get_existing_ids(self) -> Set[str]:
        """Get all existing mention IDs"""
        with self.get_connection() as conn:
            cursor = conn.execute('SELECT id FROM mentions')
            return {row[0] for row in cursor.fetchall()}
    
    def execute_query(self, query: str, params: tuple = ()) -> List[sqlite3.Row]:
        """Execute a query and return results"""
        with self.get_connection() as conn:
            cursor = conn.execute(query, params)
            return cursor.fetchall()

class SentimentAnalyzer:
    def __init__(self, api_token: str):
        self.api_url = "https://api-inference.huggingface.co/models/tabularisai/multilingual-sentiment-analysis"
        self.headers = {"Authorization": f"Bearer {api_token}"}
        self.last_request = 0
        self.min_delay = 1.0  # Minimum delay between requests
    
    async def analyze(self, text: str) -> str:
        """Analyze sentiment of text"""
        if not text or len(text.strip()) == 0 or not self.headers.get("Authorization") or "Bearer " not in self.headers.get("Authorization"):
            return "neutral"
        
        # Rate limiting
        now = time.time()
        if now - self.last_request < self.min_delay:
            await asyncio.sleep(self.min_delay - (now - self.last_request))
        
        try:
            async with aiohttp.ClientSession() as session:
                payload = {"inputs": text[:1000]}
                async with session.post(
                    self.api_url,
                    headers=self.headers,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    self.last_request = time.time()
                    
                    if response.status == 200:
                        result = await response.json()
                        scores = result[0]
                        top_label = max(scores, key=lambda x: x['score'])['label'].lower()
                        
                        if 'positive' in top_label:
                            return "positive"
                        elif 'negative' in top_label:
                            return "negative"
                        else:
                            return "neutral"
                    else:
                        logger.warning(f"Sentiment API returned {response.status}")
                        return "neutral"
        except Exception as e:
            logger.error(f"Sentiment analysis failed: {e}")
            return "neutral"

class RedditMonitor:
    def __init__(self, config: Dict, db: DatabaseManager):
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
            self.reddit = praw.Reddit(
                client_id=config['reddit']['client_id'],
                client_secret=config['reddit']['client_secret'],
                user_agent=config['reddit']['user_agent']
            )
        else:
            self.reddit = None
            logger.warning("Reddit credentials not provided - PRAW monitoring disabled")
    
    def find_brands(self, text: str) -> List[str]:
        """Find brand mentions in text"""
        brands_found = []
        for brand, pattern in self.brands.items():
            if pattern.search(text):
                brands_found.append(brand)
        return brands_found
    
    async def process_mention_buffer(self):
        """Process and save mentions from buffer"""
        if not self.mention_buffer:
            return
        
        # Add sentiment analysis
        for mention in self.mention_buffer:
            if mention.sentiment is None:
                text = f"{mention.title or ''} {mention.body or ''}"
                mention.sentiment = await self.sentiment.analyze(text)
        
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
                                    await self._process_rss_feed(rss_text, subreddit)
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
            
            for entry in feed.entries:
                post_id = entry.link.split('/')[-2] if '/' in entry.link else entry.link.split('/')[-1]
                
                if post_id in self.seen_ids:
                    continue
                
                title = entry.title
                content = getattr(entry, 'summary', '')
                text = f"{title} {content}"
                
                brands = self.find_brands(text)
                if brands:
                    for brand in brands:
                        mention = Mention(
                            id=post_id,
                            type="post",
                            title=title,
                            body=content,
                            permalink=entry.link,
                            created=datetime.now(timezone.utc).isoformat(),
                            subreddit=subreddit,
                            author="unknown",
                            score=0,
                            sentiment=None,
                            brand=brand,
                            source="rss"
                        )
                        
                        self.mention_buffer.append(mention)
                        self.seen_ids.add(post_id)
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
                                    await self._process_json_comments(data)
                                elif response.status == 429:
                                    logger.warning("JSON API rate limited")
                                    await asyncio.sleep(60)
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
                logger.error(f"JSON monitoring error: {e}")
                await asyncio.sleep(60)
    
    def _chunk_subreddits(self, chunk_size: int = 3):
        """Split subreddits into chunks"""
        subreddits = self.config['subreddits']
        for i in range(0, len(subreddits), chunk_size):
            yield subreddits[i:i + chunk_size]
    
    async def _process_json_comments(self, data: Dict):
        """Process comments from JSON API"""
        children = data.get("data", {}).get("children", [])
        
        for item in children:
            comment_data = item.get("data", {})
            comment_id = comment_data.get("id")
            
            if not comment_id or comment_id in self.seen_ids:
                continue
            
            body = comment_data.get("body", "")
            if len(body) < 10:  # Skip very short comments
                continue
            
            brands = self.find_brands(body)
            if brands:
                for brand in brands:
                    mention = Mention(
                        id=comment_id,
                        type="comment",
                        title=None,
                        body=body,
                        permalink=f"https://reddit.com{comment_data['permalink']}",
                        created=datetime.fromtimestamp(comment_data["created_utc"], tz=timezone.utc).isoformat(),
                        subreddit=comment_data["subreddit"],
                        author=comment_data["author"],
                        score=comment_data["score"],
                        sentiment=None,
                        brand=brand,
                        source="json"
                    )
                    
                    self.mention_buffer.append(mention)
                    self.seen_ids.add(comment_id)
                    logger.info(f"Found JSON mention: {brand} in r/{comment_data['subreddit']}")
    
    async def start_monitoring(self):
        """Start all monitoring tasks"""
        self.running = True
        logger.info("Starting Reddit monitoring...")
        
        tasks = [
            self.monitor_rss_feeds(),
            self.monitor_json_api()
        ]
        
        # Start PRAW monitoring in separate thread if available
        if self.reddit:
            praw_thread = threading.Thread(target=self._monitor_praw_sync, daemon=True)
            praw_thread.start()
        
        await asyncio.gather(*tasks, return_exceptions=True)
    
    def _monitor_praw_sync(self):
        """Monitor comments using PRAW in sync mode (separate thread)"""
        if not self.reddit:
            return
        
        logger.info("Starting PRAW monitoring...")
        
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
                        # Process buffer synchronously for thread safety
                        if self.mention_buffer:
                            self.db.insert_mentions(self.mention_buffer)
                            self.mention_buffer.clear()
                
            except prawcore.exceptions.TooManyRequests:
                logger.warning("PRAW rate limited, sleeping 60 seconds")
                time.sleep(60)
            except Exception as e:
                logger.error(f"PRAW monitoring error: {e}")
                time.sleep(30)
    
    def stop_monitoring(self):
        """Stop monitoring"""
        self.running = False
        logger.info("Stopping Reddit monitoring...")

# Flask Web Interface
app = Flask(__name__)
db_manager = None
reddit_monitor = None

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/health')
def health():
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "monitoring": reddit_monitor.running if reddit_monitor else False
    })

@app.route('/data')
def get_mentions():
    brand = request.args.get('brand', list(CONFIG['brands'].keys())[0])
    page = int(request.args.get('page', 1))
    per_page = min(int(request.args.get('per_page', 50)), 100)
    
    # Build query
    offset = (page - 1) * per_page
    query = '''
        SELECT * FROM mentions 
        WHERE brand = ? 
        ORDER BY created DESC 
        LIMIT ? OFFSET ?
    '''
    
    mentions = db_manager.execute_query(query, (brand, per_page, offset))
    
    return jsonify({
        "results": [dict(row) for row in mentions],
        "pagination": {"page": page, "per_page": per_page}
    })

@app.route('/stats')
def get_stats():
    brand = request.args.get('brand', list(CONFIG['brands'].keys())[0])
    
    # Today's stats
    today = datetime.now().strftime('%Y-%m-%d')
    daily_query = '''
        SELECT 
            COUNT(CASE WHEN type = 'post' THEN 1 END) as posts,
            COUNT(CASE WHEN type = 'comment' THEN 1 END) as comments
        FROM mentions 
        WHERE brand = ? AND date(created) = ?
    '''
    daily_stats = db_manager.execute_query(daily_query, (brand, today))[0]
    
    # Total stats
    total_query = '''
        SELECT 
            COUNT(CASE WHEN type = 'post' THEN 1 END) as posts,
            COUNT(CASE WHEN type = 'comment' THEN 1 END) as comments
        FROM mentions 
        WHERE brand = ?
    '''
    total_stats = db_manager.execute_query(total_query, (brand,))[0]
    
    # Sentiment stats
    sentiment_query = '''
        SELECT sentiment, COUNT(*) as count
        FROM mentions 
        WHERE brand = ? AND sentiment IS NOT NULL
        GROUP BY sentiment
    '''
    sentiment_stats = {row[0]: row[1] for row in db_manager.execute_query(sentiment_query, (brand,))}
    
    # Calculate sentiment score
    pos = sentiment_stats.get('positive', 0)
    neu = sentiment_stats.get('neutral', 0)
    neg = sentiment_stats.get('negative', 0)
    total = pos + neu + neg
    score = round((pos * 100 + neu * 50) / total) if total > 0 else 0
    
    return jsonify({
        "brand": brand,
        "daily": {"posts": daily_stats[0], "comments": daily_stats[1]},
        "total": {"posts": total_stats[0], "comments": total_stats[1]},
        "sentiment": {"positive": pos, "neutral": neu, "negative": neg},
        "score": score
    })

@app.route('/trending_subreddits')
def trending_subreddits():
    brand = request.args.get('brand', list(CONFIG['brands'].keys())[0])
    days = int(request.args.get('days', 7))
    
    cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    
    query = '''
        SELECT 
            subreddit,
            COUNT(*) as mention_count,
            AVG(score) as avg_score,
            COUNT(CASE WHEN sentiment = 'positive' THEN 1 END) as positive,
            COUNT(CASE WHEN sentiment = 'negative' THEN 1 END) as negative
        FROM mentions 
        WHERE brand = ? AND date(created) >= ?
        GROUP BY subreddit
        ORDER BY mention_count DESC
        LIMIT 10
    '''
    
    results = db_manager.execute_query(query, (brand, cutoff_date))
    
    trending = []
    for row in results:
        subreddit, count, avg_score, pos, neg = row
        trending.append({
            "subreddit": subreddit,
            "mention_count": count,
            "avg_score": float(avg_score) if avg_score else 0,
            "positive_mentions": pos,
            "negative_mentions": neg,
            "sentiment_ratio": pos / (pos + neg) if (pos + neg) > 0 else 0.5
        })
    
    return jsonify(trending)

@app.route('/export')
def export_data():
    brand = request.args.get('brand', list(CONFIG['brands'].keys())[0])
    
    query = '''
        SELECT * FROM mentions 
        WHERE brand = ? 
        ORDER BY created DESC 
        LIMIT 5000
    '''
    
    mentions = db_manager.execute_query(query, (brand,))
    
    # Create CSV
    output = io.StringIO()
    writer = csv.writer(output)
    
    # Header
    if mentions:
        writer.writerow(mentions[0].keys())
        
        # Data
        for mention in mentions:
            writer.writerow(mention)
    
    output.seek(0)
    
    return send_file(
        io.BytesIO(output.getvalue().encode('utf-8')),
        mimetype='text/csv',
        as_attachment=True,
        download_name=f'{brand}_mentions_{datetime.now().strftime("%Y%m%d")}.csv'
    )

@app.route('/system_status')
def system_status():
    # Database stats
    total_mentions = db_manager.execute_query("SELECT COUNT(*) FROM mentions")[0][0]
    total_brands = len(CONFIG['brands'])
    
    # Source breakdown
    source_query = '''
        SELECT source, COUNT(*), MAX(created) as last_update
        FROM mentions 
        GROUP BY source
    '''
    sources = []
    for row in db_manager.execute_query(source_query):
        sources.append({
            "source": row[0],
            "count": row[1],
            "last_update": row[2]
        })
    
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

# HTML Template (embedded)
HTML_TEMPLATE = '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Reddit Brand Monitor - All-in-One</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
            background: #f8fafc; color: #334155; line-height: 1.6;
        }
        .header { 
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white; padding: 2rem; text-align: center;
        }
        .header h1 { font-size: 2.5rem; margin-bottom: 0.5rem; }
        .header p { opacity: 0.9; font-size: 1.1rem; }
        .container { max-width: 1200px; margin: 0 auto; padding: 2rem; }
        .tabs { display: flex; gap: 0.5rem; margin-bottom: 2rem; flex-wrap: wrap; }
        .tab { 
            padding: 0.75rem 1.5rem; background: white; border: none; 
            border-radius: 8px; cursor: pointer; font-weight: 500;
            transition: all 0.2s; box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .tab:hover { transform: translateY(-1px); box-shadow: 0 4px 8px rgba(0,0,0,0.15); }
        .tab.active { background: #667eea; color: white; }
        .stats-grid { 
            display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1rem; margin-bottom: 2rem;
        }
        .stat-card { 
            background: white; padding: 1.5rem; border-radius: 12px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1); text-align: center;
        }
        .stat-number { font-size: 2rem; font-weight: bold; color: #667eea; }
        .stat-label { color: #64748b; margin-top: 0.5rem; font-size: 0.9rem; }
        .content-section { 
            background: white; border-radius: 12px; padding: 1.5rem;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1); margin-bottom: 2rem;
        }
        .mentions-table { width: 100%; border-collapse: collapse; }
        .mentions-table th, .mentions-table td { 
            padding: 0.75rem; text-align: left; border-bottom: 1px solid #e2e8f0;
        }
        .mentions-table th { background: #f8fafc; font-weight: 600; }
        .badge { 
            padding: 0.25rem 0.75rem; border-radius: 20px; font-size: 0.8rem;
            font-weight: 500; text-transform: capitalize;
        }
        .badge.positive { background: #dcfce7; color: #166534; }
        .badge.neutral { background: #f1f5f9; color: #475569; }
        .badge.negative { background: #fecaca; color: #991b1b; }
        .btn { 
            padding: 0.5rem 1rem; border: none; border-radius: 6px;
            cursor: pointer; font-weight: 500; transition: all 0.2s;
            margin: 0.25rem;
        }
        .btn-primary { background: #667eea; color: white; }
        .btn-primary:hover { background: #5a67d8; }
        .btn-danger { background: #ef4444; color: white; font-size: 0.8rem; }
        .btn-danger:hover { background: #dc2626; }
        .filters { display: flex; gap: 1rem; margin-bottom: 1.5rem; flex-wrap: wrap; }
        .filter-input { 
            padding: 0.5rem; border: 1px solid #d1d5db; border-radius: 6px;
            font-size: 0.9rem;
        }
        .loading { text-align: center; padding: 2rem; color: #64748b; }
        .charts-container { display: grid; grid-template-columns: 1fr 1fr; gap: 2rem; }
        .chart-wrapper { text-align: center; }
        .status-indicator { 
            display: inline-block; width: 10px; height: 10px; border-radius: 50%;
            margin-right: 5px;
        }
        .status-active { background: #22c55e; }
        .status-inactive { background: #ef4444; }
        @media (max-width: 768px) {
            .charts-container { grid-template-columns: 1fr; }
            .filters { flex-direction: column; }
            .tabs { flex-direction: column; }
            .stats-grid { grid-template-columns: 1fr; }
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>Reddit Brand Monitor</h1>
        <p>All-in-One Real-time Brand Monitoring Solution</p>
    </div>

    <div class="container">
        <div class="tabs">
            <button class="tab active" onclick="showTab('dashboard')" id="tab-dashboard">Dashboard</button>
            <button class="tab" onclick="showTab('mentions')" id="tab-mentions">Live Mentions</button>
            <button class="tab" onclick="showTab('analytics')" id="tab-analytics">Analytics</button>
            <button class="tab" onclick="showTab('settings')" id="tab-settings">Settings</button>
        </div>

        <!-- Dashboard Tab -->
        <div id="dashboard-tab" class="tab-content">
            <div class="stats-grid" id="stats-container">
                <div class="loading">Loading stats...</div>
            </div>
            
            <div class="content-section">
                <h3>Recent Activity</h3>
                <div class="charts-container">
                    <div class="chart-wrapper">
                        <h4>Sentiment Distribution</h4>
                        <canvas id="sentiment-chart" width="300" height="300"></canvas>
                    </div>
                    <div class="chart-wrapper">
                        <h4>Brand Comparison</h4>
                        <canvas id="brand-chart" width="300" height="300"></canvas>
                    </div>
                </div>
            </div>
        </div>

        <!-- Mentions Tab -->
        <div id="mentions-tab" class="tab-content" style="display: none;">
            <div class="content-section">
                <div class="filters">
                    <select class="filter-input" id="brand-filter">
                        <!-- Populated by JavaScript -->
                    </select>
                    <button class="btn btn-primary" onclick="loadMentions()">Refresh</button>
                    <button class="btn btn-primary" onclick="exportData()">Export CSV</button>
                </div>

                <table class="mentions-table">
                    <thead>
                        <tr>
                            <th>Type</th>
                            <th>Subreddit</th>
                            <th>Author</th>
                            <th>Content</th>
                            <th>Created</th>
                            <th>Score</th>
                            <th>Sentiment</th>
                            <th>Source</th>
                            <th>Actions</th>
                        </tr>
                    </thead>
                    <tbody id="mentions-tbody">
                        <tr><td colspan="9" class="loading">Loading mentions...</td></tr>
                    </tbody>
                </table>
            </div>
        </div>

        <!-- Analytics Tab -->
        <div id="analytics-tab" class="tab-content" style="display: none;">
            <div class="content-section">
                <h3>Trending Subreddits</h3>
                <div id="trending-container" class="loading">Loading trending data...</div>
            </div>
        </div>

        <!-- Settings Tab -->
        <div id="settings-tab" class="tab-content" style="display: none;">
            <div class="content-section">
                <h3>System Status</h3>
                <div id="status-container" class="loading">Loading system status...</div>
            </div>
            
            <div class="content-section">
                <h3>Configuration</h3>
                <p><strong>Brands Tracked:</strong> <span id="brands-list"></span></p>
                <p><strong>Subreddits Monitored:</strong> <span id="subreddits-count"></span></p>
                <p><strong>Database:</strong> SQLite (embedded)</p>
                <p><strong>Monitoring Status:</strong> <span id="monitoring-status"></span></p>
            </div>
        </div>
    </div>

    <script>
        let currentBrand = '';
        let brands = [];
        let charts = {};

        // Initialize
        async function init() {
            try {
                // Load system status to get brands
                const response = await fetch('/system_status');
                const status = await response.json();
                
                // Set up brand filter
                const brandFilter = document.getElementById('brand-filter');
                brandFilter.innerHTML = '';
                
                // Get brands from config (we'll need to expose this)
                brands = ['badinka', 'iheartraves']; // Default brands
                brands.forEach(brand => {
                    const option = document.createElement('option');
                    option.value = brand;
                    option.textContent = brand.charAt(0).toUpperCase() + brand.slice(1);
                    brandFilter.appendChild(option);
                });
                
                currentBrand = brands[0];
                
                // Update settings
                document.getElementById('brands-list').textContent = brands.join(', ');
                document.getElementById('subreddits-count').textContent = '10+ communities';
                
                const monitoringStatus = status.monitoring_active ? 
                    '<span class="status-indicator status-active"></span>Active' :
                    '<span class="status-indicator status-inactive"></span>Inactive';
                document.getElementById('monitoring-status').innerHTML = monitoringStatus;
                
                loadDashboard();
            } catch (error) {
                console.error('Initialization error:', error);
            }
        }

        // Tab management
        function showTab(tabName) {
            document.querySelectorAll('.tab-content').forEach(tab => tab.style.display = 'none');
            document.querySelectorAll('.tab').forEach(tab => tab.classList.remove('active'));
            
            document.getElementById(tabName + '-tab').style.display = 'block';
            document.getElementById('tab-' + tabName).classList.add('active');
            
            if (tabName === 'dashboard') loadDashboard();
            else if (tabName === 'mentions') loadMentions();
            else if (tabName === 'analytics') loadAnalytics();
            else if (tabName === 'settings') loadSystemStatus();
        }

        // Load dashboard
        async function loadDashboard() {
            try {
                const promises = brands.map(brand => 
                    fetch(`/stats?brand=${brand}`).then(r => r.json())
                );
                const statsArray = await Promise.all(promises);
                
                renderStats(statsArray);
                renderCharts(statsArray);
            } catch (error) {
                console.error('Error loading dashboard:', error);
            }
        }

        function renderStats(statsArray) {
            const container = document.getElementById('stats-container');
            
            let totalMentions = 0;
            let totalToday = 0;
            let avgScore = 0;
            
            statsArray.forEach(stats => {
                totalMentions += stats.total.posts + stats.total.comments;
                totalToday += stats.daily.posts + stats.daily.comments;
                avgScore += stats.score;
            });
            
            avgScore = avgScore / statsArray.length;
            
            container.innerHTML = `
                <div class="stat-card">
                    <div class="stat-number">${totalToday}</div>
                    <div class="stat-label">Today's Mentions</div>
                </div>
                <div class="stat-card">
                    <div class="stat-number">${totalMentions}</div>
                    <div class="stat-label">Total Mentions</div>
                </div>
                <div class="stat-card">
                    <div class="stat-number">${Math.round(avgScore)}/100</div>
                    <div class="stat-label">Avg Sentiment</div>
                </div>
                <div class="stat-card">
                    <div class="stat-number">${brands.length}</div>
                    <div class="stat-label">Brands Tracked</div>
                </div>
            `;
        }

        function renderCharts(statsArray) {
            // Sentiment pie chart for current brand
            const currentStats = statsArray[0] || {sentiment: {positive: 0, neutral: 0, negative: 0}};
            
            const sentimentCtx = document.getElementById('sentiment-chart').getContext('2d');
            if (charts.sentiment) charts.sentiment.destroy();
            
            charts.sentiment = new Chart(sentimentCtx, {
                type: 'pie',
                data: {
                    labels: ['Positive', 'Neutral', 'Negative'],
                    datasets: [{
                        data: [
                            currentStats.sentiment.positive, 
                            currentStats.sentiment.neutral, 
                            currentStats.sentiment.negative
                        ],
                        backgroundColor: ['#22c55e', '#64748b', '#ef4444']
                    }]
                },
                options: { 
                    responsive: true, 
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { position: 'bottom' }
                    }
                }
            });

            // Brand comparison chart
            const brandCtx = document.getElementById('brand-chart').getContext('2d');
            if (charts.brand) charts.brand.destroy();
            
            const brandData = statsArray.map((stats, index) => ({
                brand: brands[index] || `Brand ${index + 1}`,
                mentions: stats.total.posts + stats.total.comments
            }));
            
            charts.brand = new Chart(brandCtx, {
                type: 'bar',
                data: {
                    labels: brandData.map(d => d.brand),
                    datasets: [{
                        label: 'Total Mentions',
                        data: brandData.map(d => d.mentions),
                        backgroundColor: '#667eea'
                    }]
                },
                options: { 
                    responsive: true, 
                    maintainAspectRatio: false,
                    scales: {
                        y: { beginAtZero: true }
                    }
                }
            });
        }

        // Load mentions
        async function loadMentions() {
            const brand = document.getElementById('brand-filter').value;
            
            try {
                const response = await fetch(`/data?brand=${brand}&per_page=50`);
                const data = await response.json();
                renderMentions(data.results || []);
            } catch (error) {
                console.error('Error loading mentions:', error);
            }
        }

        function renderMentions(mentions) {
            const tbody = document.getElementById('mentions-tbody');
            
            if (mentions.length === 0) {
                tbody.innerHTML = '<tr><td colspan="9" class="loading">No mentions found</td></tr>';
                return;
            }
            
            tbody.innerHTML = mentions.map(mention => `
                <tr>
                    <td>${mention.type}</td>
                    <td>r/${mention.subreddit}</td>
                    <td>u/${mention.author}</td>
                    <td>
                        <div style="max-width: 250px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">
                            ${mention.title || mention.body || ''}
                        </div>
                        <a href="${mention.permalink}" target="_blank" style="font-size: 0.8rem; color: #667eea;">View →</a>
                    </td>
                    <td>${new Date(mention.created).toLocaleDateString()}</td>
                    <td>${mention.score}</td>
                    <td><span class="badge ${mention.sentiment || 'neutral'}">${mention.sentiment || 'neutral'}</span></td>
                    <td>${mention.source}</td>
                    <td>
                        <button class="btn btn-danger" onclick="deleteMention('${mention.id}')">Delete</button>
                    </td>
                </tr>
            `).join('');
        }

        // Load analytics
        async function loadAnalytics() {
            try {
                const response = await fetch(`/trending_subreddits?brand=${currentBrand}`);
                const trending = await response.json();
                
                const container = document.getElementById('trending-container');
                container.innerHTML = `
                    <table class="mentions-table">
                        <thead>
                            <tr>
                                <th>Subreddit</th>
                                <th>Mentions</th>
                                <th>Avg Score</th>
                                <th>Positive %</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${trending.map(item => `
                                <tr>
                                    <td>r/${item.subreddit}</td>
                                    <td>${item.mention_count}</td>
                                    <td>${item.avg_score.toFixed(1)}</td>
                                    <td>${(item.sentiment_ratio * 100).toFixed(1)}%</td>
                                </tr>
                            `).join('')}
                        </tbody>
                    </table>
                `;
            } catch (error) {
                console.error('Error loading analytics:', error);
            }
        }

        // Load system status
        async function loadSystemStatus() {
            try {
                const response = await fetch('/system_status');
                const status = await response.json();
                
                const container = document.getElementById('status-container');
                container.innerHTML = `
                    <div class="stats-grid">
                        <div class="stat-card">
                            <div class="stat-number">${status.total_mentions}</div>
                            <div class="stat-label">Total Mentions</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-number">${status.total_brands}</div>
                            <div class="stat-label">Brands Tracked</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-number">${status.monitoring_active ? 'Active' : 'Inactive'}</div>
                            <div class="stat-label">Monitoring Status</div>
                        </div>
                    </div>
                    
                    <h4>Data Sources</h4>
                    <table class="mentions-table">
                        <thead>
                            <tr><th>Source</th><th>Total Mentions</th><th>Last Update</th></tr>
                        </thead>
                        <tbody>
                            ${status.sources.map(source => `
                                <tr>
                                    <td>${source.source}</td>
                                    <td>${source.count}</td>
                                    <td>${source.last_update ? new Date(source.last_update).toLocaleString() : 'N/A'}</td>
                                </tr>
                            `).join('')}
                        </tbody>
                    </table>
                `;
            } catch (error) {
                console.error('Error loading system status:', error);
            }
        }

        // Utility functions
        async function deleteMention(id) {
            if (!confirm('Are you sure you want to delete this mention?')) return;
            
            try {
                await fetch('/delete', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ id })
                });
                loadMentions();
            } catch (error) {
                console.error('Error deleting mention:', error);
            }
        }

        function exportData() {
            const brand = document.getElementById('brand-filter').value;
            window.open(`/export?brand=${brand}`, '_blank');
        }

        // Initialize and auto-refresh
        document.addEventListener('DOMContentLoaded', () => {
            init();
            setInterval(() => {
                if (document.getElementById('dashboard-tab').style.display !== 'none') {
                    loadDashboard();
                }
            }, 30000); // Refresh every 30 seconds
        });
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
    
    print("🚀 Starting All-in-One Reddit Brand Monitor...")
    
    # Initialize database
    db_manager = DatabaseManager(CONFIG['database_file'])
    print(f"✅ Database initialized: {CONFIG['database_file']}")
    
    # Initialize Reddit monitor
    reddit_monitor = RedditMonitor(CONFIG, db_manager)
    if CONFIG.get('monitor_all_reddit', False):
        print("✅ Reddit monitor initialized - MONITORING ALL OF REDDIT 🌍")
    else:
        print("✅ Reddit monitor initialized - monitoring specific subreddits")
    
    # Start monitoring in background thread
    monitoring_thread = threading.Thread(target=run_monitoring_thread, daemon=True)
    monitoring_thread.start()
    print("✅ Background monitoring started")
    
    # Start Flask web interface
    print(f"🌐 Starting web interface on port {CONFIG['port']}")
    print(f"📊 Dashboard: http://localhost:{CONFIG['port']}")
    print(f"🔍 Health check: http://localhost:{CONFIG['port']}/health")
    
    try:
        app.run(host='0.0.0.0', port=CONFIG['port'], debug=False)
    except KeyboardInterrupt:
        print("\n⏹️  Shutting down...")
        reddit_monitor.stop_monitoring()

if __name__ == "__main__":
    main()
