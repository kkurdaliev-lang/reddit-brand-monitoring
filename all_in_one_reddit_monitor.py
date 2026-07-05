"""
All-in-One Reddit Brand Monitor
===============================

Monitors ALL of Reddit (every public post and comment) for brand mentions.

Coverage strategy (why mentions are not missed):
  1. PRAW streams of r/all comments and submissions (real-time firehose).
  2. Reddit IDs are sequential (base36). Every ID that the streams skip --
     because of rate limits, restarts, errors, or because the subreddit is
     excluded from r/all -- is detected as a "gap" and fetched explicitly
     via the /api/info endpoint in batches of 100. The last processed IDs
     are persisted, so downtime gaps are backfilled on restart too.
  3. A periodic Reddit search sweep per brand catches any post that still
     slipped through (belt and braces).

Everything runs against the authenticated Reddit API (OAuth via PRAW).
The old unauthenticated .json/.rss polling was removed: Reddit has blocked
those endpoints (403) since the 2023 API changes, so they only wasted
resources without ever finding anything.
"""

import csv
import io
import logging
import os
import re
import sqlite3
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import praw
import prawcore
import requests
from flask import Flask, jsonify, render_template_string, request, send_file

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CONFIG = {
    'database_file': os.getenv('DATABASE_PATH', '/app/data/reddit_monitor.db'),
    'reddit': {
        'client_id': os.getenv('REDDIT_CLIENT_ID', ''),
        'client_secret': os.getenv('REDDIT_CLIENT_SECRET', ''),
        'user_agent': os.getenv('REDDIT_USER_AGENT', 'python:brand-mention-monitor:v3.0 (by /u/brandmonitorbot)'),
    },
    'groq_api_token': os.getenv('GROQ_API_TOKEN', ''),
    'groq_model': os.getenv('GROQ_MODEL', 'llama-3.1-8b-instant'),
    # Brand name -> regex. A leading guard prevents matches inside other
    # words ("abadinka"), the tail stays permissive so "badinka's",
    # "badinka.com", "#badinka" etc. still match.
    'brands': {
        'badinka': r'(?<![a-z0-9])[@#]?badinka(?:\.com)?',
        'candy catz': r'(?<![a-z0-9])[@#]?candy\s*catz(?:\.com)?',
    },
    # Extra search terms per brand for the periodic search sweep.
    'search_terms': {
        'badinka': ['badinka'],
        'candy catz': ['"candy catz"', 'candycatz'],
    },
    # How far the gap backfill is allowed to reach back (number of Reddit
    # IDs). ~250k comment IDs is roughly 1 hour of all of Reddit.
    'gap_limit_comments': int(os.getenv('GAP_LIMIT_COMMENTS', '250000')),
    'gap_limit_posts': int(os.getenv('GAP_LIMIT_POSTS', '50000')),
    'sweep_interval_seconds': int(os.getenv('SWEEP_INTERVAL_SECONDS', '1800')),
    'port': int(os.getenv('PORT', 5000)),
}

BASE36_CHARS = "0123456789abcdefghijklmnopqrstuvwxyz"


def to_base36(number: int) -> str:
    if number == 0:
        return "0"
    out = ""
    while number:
        number, rem = divmod(number, 36)
        out = BASE36_CHARS[rem] + out
    return out


@dataclass
class Mention:
    id: str
    type: str  # 'post' or 'comment'
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


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

class DatabaseManager:
    def __init__(self, db_file: str):
        self.db_file = db_file
        self._init_db()

    def get_connection(self):
        conn = sqlite3.connect(self.db_file, timeout=30)
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA synchronous=NORMAL')
        conn.execute('PRAGMA busy_timeout=30000')
        return conn

    def _init_db(self):
        conn = self.get_connection()
        try:
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
            conn.execute('''
                CREATE TABLE IF NOT EXISTS monitor_state (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            ''')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_brand ON mentions(brand)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_created ON mentions(created)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_subreddit ON mentions(subreddit)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_brand_created ON mentions(brand, created)')
            conn.commit()
        finally:
            conn.close()

    def mention_exists(self, mention_id: str, reddit_id: str, brand: str) -> bool:
        """True if this brand mention is already stored.

        Checks the new-format primary key (<reddit_id>_<brand>) as well as
        legacy rows keyed by the bare Reddit ID.
        """
        conn = self.get_connection()
        try:
            cursor = conn.execute(
                'SELECT 1 FROM mentions WHERE id = ? OR (id = ? AND brand = ?) LIMIT 1',
                (mention_id, reddit_id, brand)
            )
            return cursor.fetchone() is not None
        finally:
            conn.close()

    def insert_mention(self, mention: Mention) -> bool:
        conn = self.get_connection()
        try:
            cursor = conn.execute('''
                INSERT OR IGNORE INTO mentions
                (id, type, title, body, permalink, created, subreddit, author, score, sentiment, brand, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                mention.id, mention.type, mention.title, mention.body,
                mention.permalink, mention.created, mention.subreddit,
                mention.author, mention.score, mention.sentiment,
                mention.brand, mention.source
            ))
            conn.commit()
            return cursor.rowcount > 0
        finally:
            conn.close()

    def get_state(self, key: str) -> Optional[str]:
        conn = self.get_connection()
        try:
            row = conn.execute('SELECT value FROM monitor_state WHERE key = ?', (key,)).fetchone()
            return row[0] if row else None
        finally:
            conn.close()

    def set_state(self, key: str, value: str):
        conn = self.get_connection()
        try:
            conn.execute(
                'INSERT INTO monitor_state (key, value) VALUES (?, ?) '
                'ON CONFLICT(key) DO UPDATE SET value = excluded.value',
                (key, value)
            )
            conn.commit()
        finally:
            conn.close()

    def count_mentions(self) -> int:
        conn = self.get_connection()
        try:
            return conn.execute('SELECT COUNT(*) FROM mentions').fetchone()[0]
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# Sentiment (Groq)
# ---------------------------------------------------------------------------

class SentimentAnalyzer:
    def __init__(self, api_token: str, model: str):
        self.api_token = api_token
        self.model = model
        self.api_url = "https://api.groq.com/openai/v1/chat/completions"
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_token}",
        } if api_token else {}

    def analyze(self, context_text: str, brand_name: str) -> str:
        if not self.api_token or not context_text.strip():
            return "neutral"

        prompt = (
            f"Analyze the sentiment toward the brand '{brand_name}' in this text.\n\n"
            f"Text: \"{context_text[:500]}\"\n\n"
            "If the brand is mentioned positively (good quality, satisfied, recommend) answer positive. "
            "If negatively (bad quality, disappointed, avoid, scam) answer negative. "
            "Otherwise answer neutral.\n"
            "Respond with ONLY ONE WORD: positive, negative, or neutral"
        )
        payload = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 5,
            "temperature": 0.0,
        }
        try:
            response = requests.post(self.api_url, headers=self.headers, json=payload, timeout=15)
            if response.status_code == 200:
                text = response.json()['choices'][0]['message']['content'].strip().lower()
                for label in ('positive', 'negative', 'neutral'):
                    if label in text:
                        return label
                logger.warning(f"Unexpected Groq response: '{text}'")
            else:
                logger.warning(f"Groq API status {response.status_code}: {response.text[:200]}")
        except Exception as e:
            logger.warning(f"Groq sentiment analysis failed: {e}")
        return "neutral"


# ---------------------------------------------------------------------------
# Monitor
# ---------------------------------------------------------------------------

class RedditMonitor:
    STATE_SAVE_INTERVAL = 60          # seconds between persisting stream positions
    HEARTBEAT_INTERVAL = 900          # seconds between status log lines
    INFO_BATCH_SIZE = 100             # /api/info max fullnames per request

    def __init__(self, config: dict, db: DatabaseManager):
        self.config = config
        self.db = db
        self.brands = {
            name: re.compile(pattern, re.IGNORECASE)
            for name, pattern in config['brands'].items()
        }
        self.sentiment = SentimentAnalyzer(config['groq_api_token'], config['groq_model'])
        self.running = False
        self.threads: List[threading.Thread] = []

        # Gap tracking: queues of integer IDs the streams skipped.
        self._gap_lock = threading.Lock()
        self._gaps = {'comment': deque(), 'post': deque()}
        self._last_id = {'comment': None, 'post': None}
        self._gap_limits = {
            'comment': config['gap_limit_comments'],
            'post': config['gap_limit_posts'],
        }

        self._stats_lock = threading.Lock()
        self.stats = {
            'comments_streamed': 0,
            'posts_streamed': 0,
            'gap_items_fetched': 0,
            'gap_ids_dropped': 0,
            'mentions_found': 0,
            'stream_errors': 0,
            'last_comment_seen': None,
            'last_post_seen': None,
            'started_at': None,
        }

    # -- helpers ------------------------------------------------------------

    def has_credentials(self) -> bool:
        return bool(self.config['reddit']['client_id'] and self.config['reddit']['client_secret'])

    def _make_reddit(self) -> praw.Reddit:
        # PRAW is not thread-safe, so each worker thread gets its own
        # instance. They share one OAuth app, and prawcore coordinates the
        # shared rate limit budget via Reddit's response headers.
        return praw.Reddit(
            client_id=self.config['reddit']['client_id'],
            client_secret=self.config['reddit']['client_secret'],
            user_agent=self.config['reddit']['user_agent'],
            check_for_updates=False,
        )

    def find_brands(self, text: str) -> List[str]:
        if not text:
            return []
        return [brand for brand, pattern in self.brands.items() if pattern.search(text)]

    def _bump(self, key: str, amount: int = 1):
        with self._stats_lock:
            self.stats[key] += amount

    # -- gap bookkeeping ------------------------------------------------------

    def _note_id(self, kind: str, id36: str):
        """Track a freshly streamed ID and enqueue any skipped IDs before it."""
        try:
            id_int = int(id36, 36)
        except ValueError:
            return
        with self._gap_lock:
            last = self._last_id[kind]
            if last is None:
                self._last_id[kind] = id_int
                return
            if id_int <= last:
                return  # out-of-order delivery, already covered
            gap = id_int - last - 1
            if gap > 0:
                queue = self._gaps[kind]
                limit = self._gap_limits[kind]
                start = last + 1
                if gap > limit:
                    # Cap huge gaps (e.g. weeks of downtime): only backfill
                    # the most recent `limit` IDs.
                    self._bump_locked_dropped(gap - limit)
                    start = id_int - limit
                queue.extend(range(start, id_int))
                overflow = len(queue) - limit
                if overflow > 0:
                    for _ in range(overflow):
                        queue.popleft()
                    self._bump_locked_dropped(overflow)
            self._last_id[kind] = id_int

    def _bump_locked_dropped(self, amount: int):
        with self._stats_lock:
            self.stats['gap_ids_dropped'] += amount

    def _next_gap_batch(self):
        """Return (kind, [ids]) for the next /api/info batch, preferring the
        larger backlog."""
        with self._gap_lock:
            kind = max(self._gaps, key=lambda k: len(self._gaps[k]))
            queue = self._gaps[kind]
            if not queue:
                return None, []
            ids = [queue.popleft() for _ in range(min(self.INFO_BATCH_SIZE, len(queue)))]
            return kind, ids

    def _requeue_gap_batch(self, kind: str, ids: List[int]):
        with self._gap_lock:
            self._gaps[kind].extendleft(reversed(ids))

    def gap_backlog(self) -> dict:
        with self._gap_lock:
            return {kind: len(queue) for kind, queue in self._gaps.items()}

    # -- mention pipeline -----------------------------------------------------

    def process_text_item(self, kind: str, reddit_id: str, subreddit: str, author: str,
                          created_utc: float, score: int, permalink: str,
                          title: Optional[str], body: Optional[str], source: str) -> int:
        """Scan one post/comment for all brands; store new mentions. Returns
        the number of newly stored mentions."""
        full_text = f"{title or ''} {body or ''}"
        found = self.find_brands(full_text)
        if not found:
            return 0

        stored = 0
        for brand in found:
            mention_id = f"{reddit_id}_{brand}"
            if self.db.mention_exists(mention_id, reddit_id, brand):
                continue
            sentiment = self.sentiment.analyze(self._brand_context(full_text, brand), brand)
            mention = Mention(
                id=mention_id,
                type=kind,
                title=title,
                body=body,
                permalink=permalink,
                created=datetime.fromtimestamp(created_utc, tz=timezone.utc).isoformat(),
                subreddit=subreddit,
                author=author,
                score=score,
                sentiment=sentiment,
                brand=brand,
                source=source,
            )
            if self.db.insert_mention(mention):
                stored += 1
                self._bump('mentions_found')
                logger.info(f"✅ Mention saved: '{brand}' ({kind}) in r/{subreddit} "
                            f"[{sentiment}] via {source} -> {permalink}")
        return stored

    def _brand_context(self, text: str, brand: str) -> str:
        """~200 chars of context around the brand mention for sentiment."""
        match = self.brands[brand].search(text)
        if not match:
            return text[:400]
        start = max(0, match.start() - 150)
        end = min(len(text), match.end() + 150)
        return text[start:end].strip()

    def process_comment(self, comment, source: str) -> int:
        try:
            permalink = f"https://reddit.com{comment.permalink}"
        except Exception:
            link = getattr(comment, 'link_id', 't3_unknown').split('_')[-1]
            permalink = f"https://reddit.com/comments/{link}//{comment.id}"
        return self.process_text_item(
            kind='comment',
            reddit_id=comment.id,
            subreddit=str(comment.subreddit),
            author=str(comment.author) if comment.author else '[deleted]',
            created_utc=comment.created_utc,
            score=getattr(comment, 'score', 0),
            permalink=permalink,
            title=None,
            body=getattr(comment, 'body', '') or '',
            source=source,
        )

    def process_submission(self, post, source: str) -> int:
        return self.process_text_item(
            kind='post',
            reddit_id=post.id,
            subreddit=str(post.subreddit),
            author=str(post.author) if post.author else '[deleted]',
            created_utc=post.created_utc,
            score=getattr(post, 'score', 0),
            permalink=f"https://reddit.com{post.permalink}",
            title=getattr(post, 'title', '') or '',
            body=getattr(post, 'selftext', '') or '',
            source=source,
        )

    # -- worker threads -------------------------------------------------------

    def _run_stream(self, kind: str):
        """Stream r/all comments or submissions, with automatic reconnect."""
        stat_key = 'comments_streamed' if kind == 'comment' else 'posts_streamed'
        seen_key = 'last_comment_seen' if kind == 'comment' else 'last_post_seen'
        backoff = 5
        while self.running:
            try:
                reddit = self._make_reddit()
                subreddit = reddit.subreddit('all')
                if kind == 'comment':
                    stream = subreddit.stream.comments(skip_existing=False, pause_after=5)
                else:
                    stream = subreddit.stream.submissions(skip_existing=False, pause_after=5)
                logger.info(f"🎯 {kind} stream connected (r/all)")
                backoff = 5
                for item in stream:
                    if not self.running:
                        return
                    if item is None:
                        time.sleep(2)
                        continue
                    self._note_id(kind, item.id)
                    with self._stats_lock:
                        self.stats[stat_key] += 1
                        self.stats[seen_key] = time.time()
                    if kind == 'comment':
                        self.process_comment(item, source='praw_stream')
                    else:
                        self.process_submission(item, source='praw_stream')
            except prawcore.exceptions.ResponseException as e:
                status = getattr(getattr(e, 'response', None), 'status_code', '?')
                if status == 401:
                    logger.error("❌ Reddit API returned 401 - check REDDIT_CLIENT_ID / "
                                 "REDDIT_CLIENT_SECRET. Retrying in 5 minutes.")
                    time.sleep(300)
                else:
                    logger.warning(f"{kind} stream HTTP error ({status}): {e}; "
                                   f"reconnecting in {backoff}s")
                    self._bump('stream_errors')
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 300)
            except Exception as e:
                logger.warning(f"{kind} stream error: {e}; reconnecting in {backoff}s")
                self._bump('stream_errors')
                time.sleep(backoff)
                backoff = min(backoff * 2, 300)

    def _run_gap_filler(self):
        """Fetch IDs the streams skipped via /api/info (100 per request).

        This is what guarantees completeness: rate-limit pauses, reconnects,
        restarts and subreddits excluded from r/all all show up as ID gaps,
        and every gapped ID is checked exactly once.
        """
        reddit = None
        while self.running:
            kind, ids = self._next_gap_batch()
            if not ids:
                time.sleep(5)
                continue
            prefix = 't1_' if kind == 'comment' else 't3_'
            fullnames = [f"{prefix}{to_base36(i)}" for i in ids]
            try:
                if reddit is None:
                    reddit = self._make_reddit()
                fetched = 0
                for thing in reddit.info(fullnames=fullnames):
                    fetched += 1
                    if kind == 'comment':
                        self.process_comment(thing, source='gap_backfill')
                    else:
                        self.process_submission(thing, source='gap_backfill')
                self._bump('gap_items_fetched', fetched)
            except Exception as e:
                logger.warning(f"Gap filler error ({kind}): {e}; retrying batch in 30s")
                self._requeue_gap_batch(kind, ids)
                reddit = None
                time.sleep(30)
                continue
            # Adaptive pacing: hurry when the backlog is big, otherwise stay
            # well inside the rate limit budget.
            backlog = sum(self.gap_backlog().values())
            time.sleep(0.7 if backlog > 20000 else 1.5 if backlog > 2000 else 3.0)

    def _run_sweeper(self):
        """Periodic Reddit search per brand - safety net for posts."""
        first_run = True
        while self.running:
            try:
                reddit = self._make_reddit()
                time_filter = 'week' if first_run else 'day'
                for brand, terms in self.config.get('search_terms', {}).items():
                    if brand not in self.brands:
                        continue
                    for term in terms:
                        if not self.running:
                            return
                        try:
                            for post in reddit.subreddit('all').search(
                                    term, sort='new', time_filter=time_filter, limit=100):
                                self.process_submission(post, source='search_sweep')
                        except Exception as e:
                            logger.warning(f"Search sweep failed for '{term}': {e}")
                        time.sleep(3)
                logger.info(f"🔎 Search sweep completed (time_filter={time_filter})")
                first_run = False
            except Exception as e:
                logger.warning(f"Search sweeper error: {e}")
            # Sleep in small slices so shutdown stays responsive.
            deadline = time.time() + self.config['sweep_interval_seconds']
            while self.running and time.time() < deadline:
                time.sleep(5)

    def _run_housekeeping(self):
        """Persist stream positions and emit a heartbeat log line."""
        last_heartbeat = 0.0
        while self.running:
            time.sleep(self.STATE_SAVE_INTERVAL)
            try:
                with self._gap_lock:
                    last_comment = self._last_id['comment']
                    last_post = self._last_id['post']
                if last_comment:
                    self.db.set_state('last_comment_id', str(last_comment))
                if last_post:
                    self.db.set_state('last_post_id', str(last_post))
                if time.time() - last_heartbeat >= self.HEARTBEAT_INTERVAL:
                    last_heartbeat = time.time()
                    with self._stats_lock:
                        s = dict(self.stats)
                    backlog = self.gap_backlog()
                    logger.info(
                        f"💓 Heartbeat: {s['comments_streamed']} comments / "
                        f"{s['posts_streamed']} posts streamed, "
                        f"{s['gap_items_fetched']} gap items fetched, "
                        f"backlog c={backlog['comment']} p={backlog['post']}, "
                        f"{s['mentions_found']} mentions found this session"
                    )
            except Exception as e:
                logger.warning(f"Housekeeping error: {e}")

    # -- lifecycle --------------------------------------------------------------

    def start(self):
        if not self.has_credentials():
            logger.error("❌ REDDIT_CLIENT_ID / REDDIT_CLIENT_SECRET not set - "
                         "monitoring disabled. The dashboard still works.")
            return
        self.running = True
        with self._stats_lock:
            self.stats['started_at'] = time.time()

        # Seed stream positions from the last run so the gap filler
        # backfills whatever happened while the app was down (capped by the
        # gap limits).
        for kind, state_key in (('comment', 'last_comment_id'), ('post', 'last_post_id')):
            saved = self.db.get_state(state_key)
            if saved and saved.isdigit():
                self._last_id[kind] = int(saved)
                logger.info(f"⏪ Resuming {kind} position from ID {to_base36(int(saved))}")

        workers = [
            ('comment-stream', lambda: self._run_stream('comment')),
            ('post-stream', lambda: self._run_stream('post')),
            ('gap-filler', self._run_gap_filler),
            ('search-sweeper', self._run_sweeper),
            ('housekeeping', self._run_housekeeping),
        ]
        for name, target in workers:
            thread = threading.Thread(target=target, name=name, daemon=True)
            thread.start()
            self.threads.append(thread)
        logger.info(f"🚀 Monitoring started with {len(workers)} workers "
                    f"for brands: {list(self.brands)}")

    def stop(self):
        self.running = False

    def status(self) -> dict:
        with self._stats_lock:
            s = dict(self.stats)
        now = time.time()
        return {
            'running': self.running,
            'credentials_configured': self.has_credentials(),
            'threads_alive': {t.name: t.is_alive() for t in self.threads},
            'comments_streamed': s['comments_streamed'],
            'posts_streamed': s['posts_streamed'],
            'gap_items_fetched': s['gap_items_fetched'],
            'gap_ids_dropped': s['gap_ids_dropped'],
            'gap_backlog': self.gap_backlog(),
            'mentions_found_this_session': s['mentions_found'],
            'stream_errors': s['stream_errors'],
            'seconds_since_last_comment': round(now - s['last_comment_seen'], 1) if s['last_comment_seen'] else None,
            'seconds_since_last_post': round(now - s['last_post_seen'], 1) if s['last_post_seen'] else None,
            'uptime_seconds': round(now - s['started_at'], 1) if s['started_at'] else None,
            'sentiment_enabled': bool(self.config['groq_api_token']),
        }

    def scan_subreddit(self, subreddit_name: str, limit: int = 100) -> dict:
        """Manual backfill: scan a subreddit's newest posts and comments."""
        reddit = self._make_reddit()
        sub = reddit.subreddit(subreddit_name)
        processed = 0
        found = 0
        for post in sub.new(limit=limit):
            processed += 1
            found += self.process_submission(post, source='manual_backfill')
        for comment in sub.comments(limit=limit):
            processed += 1
            found += self.process_comment(comment, source='manual_backfill')
        return {'processed': processed, 'found_mentions': found}


# ---------------------------------------------------------------------------
# Flask web interface
# ---------------------------------------------------------------------------

app = Flask(__name__)
db_manager: Optional[DatabaseManager] = None
reddit_monitor: Optional[RedditMonitor] = None


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@app.route('/')
def index():
    brands_list = list(CONFIG['brands'].keys())
    brands_js = f"<script>window.BRANDS = {brands_list!r};</script>"
    return render_template_string(brands_js + HTML_TEMPLATE)


@app.route('/health')
def health():
    try:
        return jsonify({
            "status": "healthy",
            "timestamp": utcnow_iso(),
            "monitoring": reddit_monitor.running if reddit_monitor else False,
            "total_mentions": db_manager.count_mentions() if db_manager else 0,
        })
    except Exception as e:
        return jsonify({"status": "starting", "timestamp": utcnow_iso(), "error": str(e)})


@app.route('/system-health')
def system_health_status():
    if not reddit_monitor:
        return jsonify({"error": "Monitor not initialized"}), 500
    status = reddit_monitor.status()
    status['timestamp'] = utcnow_iso()
    status['total_mentions_in_db'] = db_manager.count_mentions() if db_manager else 0
    return jsonify(status)


@app.route('/data')
def get_mentions():
    brand = request.args.get('brand', list(CONFIG['brands'].keys())[0])
    page = int(request.args.get('page', 1))
    per_page = min(int(request.args.get('per_page', 50)), 100)
    offset = (page - 1) * per_page

    conn = db_manager.get_connection()
    try:
        cursor = conn.execute('''
            SELECT id, type, title, body, permalink, created, subreddit, author, score, sentiment, brand, source
            FROM mentions
            WHERE brand = ?
            ORDER BY created DESC
            LIMIT ? OFFSET ?
        ''', (brand, per_page, offset))
        mentions = cursor.fetchall()
    finally:
        conn.close()

    keys = ['id', 'type', 'title', 'body', 'permalink', 'created', 'subreddit',
            'author', 'score', 'sentiment', 'brand', 'source']
    return jsonify([dict(zip(keys, row)) for row in mentions])


@app.route('/stats')
def get_stats():
    brand = request.args.get('brand', list(CONFIG['brands'].keys())[0])

    conn = db_manager.get_connection()
    try:
        daily = dict(conn.execute(
            "SELECT type, COUNT(*) FROM mentions WHERE brand = ? AND DATE(created) = DATE('now') GROUP BY type",
            (brand,)).fetchall())
        total = dict(conn.execute(
            "SELECT type, COUNT(*) FROM mentions WHERE brand = ? GROUP BY type",
            (brand,)).fetchall())
        sentiment = dict(conn.execute(
            "SELECT sentiment, COUNT(*) FROM mentions WHERE brand = ? AND sentiment IS NOT NULL GROUP BY sentiment",
            (brand,)).fetchall())
    finally:
        conn.close()

    total_sentiment = sum(sentiment.values())
    if total_sentiment > 0:
        positive_ratio = sentiment.get('positive', 0) / total_sentiment
        negative_ratio = sentiment.get('negative', 0) / total_sentiment
        score = max(0, min(100, int((positive_ratio - negative_ratio + 1) * 50)))
    else:
        score = 50

    return jsonify({
        'brand': brand,
        'daily': {'posts': daily.get('post', 0), 'comments': daily.get('comment', 0)},
        'total': {'posts': total.get('post', 0), 'comments': total.get('comment', 0)},
        'sentiment': {
            'positive': sentiment.get('positive', 0),
            'negative': sentiment.get('negative', 0),
            'neutral': sentiment.get('neutral', 0),
        },
        'score': score,
    })


@app.route('/trending_subreddits')
def trending_subreddits():
    brand = request.args.get('brand')
    conn = db_manager.get_connection()
    try:
        if brand:
            rows = conn.execute('''
                SELECT subreddit, COUNT(*) as mention_count, GROUP_CONCAT(DISTINCT brand) as brands
                FROM mentions WHERE brand = ?
                GROUP BY subreddit ORDER BY mention_count DESC LIMIT 20
            ''', (brand,)).fetchall()
        else:
            rows = conn.execute('''
                SELECT subreddit, COUNT(*) as mention_count, GROUP_CONCAT(DISTINCT brand) as brands
                FROM mentions
                GROUP BY subreddit ORDER BY mention_count DESC LIMIT 20
            ''').fetchall()
    finally:
        conn.close()

    return jsonify([
        {'subreddit': subreddit, 'mention_count': count, 'brands': brands.split(',') if brands else []}
        for subreddit, count, brands in rows
    ])


@app.route('/export')
def export_mentions():
    brand = request.args.get('brand')
    conn = db_manager.get_connection()
    try:
        if brand:
            mentions = conn.execute(
                "SELECT * FROM mentions WHERE brand = ? ORDER BY created DESC", (brand,)).fetchall()
        else:
            mentions = conn.execute("SELECT * FROM mentions ORDER BY created DESC").fetchall()
    finally:
        conn.close()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['ID', 'Type', 'Title', 'Body', 'Permalink', 'Created', 'Subreddit',
                     'Author', 'Score', 'Sentiment', 'Brand', 'Source', 'CreatedAt'])
    writer.writerows(mentions)
    csv_output = output.getvalue()
    output.close()

    return send_file(
        io.BytesIO(csv_output.encode('utf-8')),
        as_attachment=True,
        download_name=f'reddit_mentions_{brand or "all"}_{datetime.now().strftime("%Y%m%d")}.csv',
        mimetype='text/csv'
    )


@app.route('/download')
def download_csv():
    brand = request.args.get('brand')
    conn = db_manager.get_connection()
    try:
        if brand:
            mentions = conn.execute(
                "SELECT type, subreddit, author, permalink, created, title, body, sentiment "
                "FROM mentions WHERE brand = ? ORDER BY created DESC", (brand,)).fetchall()
        else:
            mentions = conn.execute(
                "SELECT type, subreddit, author, permalink, created, title, body, sentiment "
                "FROM mentions ORDER BY created DESC").fetchall()
    finally:
        conn.close()

    output = io.StringIO()
    writer = csv.writer(output, delimiter='\t')
    writer.writerow(['Type', 'Subreddit', 'Author', 'Link', 'Created', 'Preview', 'Sentiment'])
    for type_val, subreddit, author, permalink, created, title, body, sentiment in mentions:
        preview = body or title or ""
        if len(preview) > 200:
            preview = preview[:200] + "..."
        link = permalink if permalink and permalink.startswith('http') else f"https://reddit.com{permalink}"
        writer.writerow([type_val, subreddit, author, link, created, preview, sentiment or "neutral"])

    csv_output = output.getvalue()
    output.close()
    return app.response_class(
        csv_output,
        mimetype='text/csv',
        headers={'Content-Disposition':
                 f'attachment; filename=reddit_mentions_{brand or "all"}_{datetime.now().strftime("%Y%m%d")}.csv'}
    )


@app.route('/delete', methods=['POST'])
def delete_mention():
    data = request.get_json()
    mention_id = data.get('id') if data else None
    if not mention_id:
        return jsonify({"error": "Missing id"}), 400

    conn = db_manager.get_connection()
    try:
        cursor = conn.execute("DELETE FROM mentions WHERE id = ?", (mention_id,))
        conn.commit()
        if cursor.rowcount == 0:
            return jsonify({"error": "Mention not found"}), 404
    finally:
        conn.close()
    return jsonify({"status": "deleted", "id": mention_id})


@app.route('/weekly_mentions')
def weekly_mentions():
    brand = request.args.get('brand', list(CONFIG['brands'].keys())[0])
    week_offset = int(request.args.get('week_offset', 0))

    today = datetime.now()
    monday = (today - timedelta(days=today.weekday()) + timedelta(weeks=week_offset))
    monday = monday.replace(hour=0, minute=0, second=0, microsecond=0)

    conn = db_manager.get_connection()
    try:
        rows = conn.execute('''
            SELECT DATE(created) as date, COUNT(*) as count
            FROM mentions
            WHERE brand = ? AND created >= ? AND created < ?
            GROUP BY DATE(created) ORDER BY date
        ''', (brand, monday.isoformat(), (monday + timedelta(days=7)).isoformat())).fetchall()
    finally:
        conn.close()
    return jsonify({date_str: count for date_str, count in rows})


@app.route('/backfill/<subreddit>')
def backfill_subreddit(subreddit):
    """Manually scan a subreddit's newest posts and comments right now."""
    if not reddit_monitor or not reddit_monitor.has_credentials():
        return jsonify({"status": "error", "message": "Reddit credentials not configured"}), 503
    if not re.fullmatch(r'[A-Za-z0-9_]{2,21}', subreddit):
        return jsonify({"status": "error", "message": "Invalid subreddit name"}), 400
    try:
        result = reddit_monitor.scan_subreddit(subreddit)
        result.update({"status": "success", "subreddit": subreddit})
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/test-groq')
def test_groq():
    api_token = CONFIG.get('groq_api_token', '')
    if not api_token:
        return jsonify({"error": "No GROQ_API_TOKEN configured"})
    analyzer = SentimentAnalyzer(api_token, CONFIG['groq_model'])
    test_text = "I love badinka clothing, it's amazing quality!"
    return jsonify({
        "status": "success",
        "model": CONFIG['groq_model'],
        "test_text": test_text,
        "sentiment_result": analyzer.analyze(test_text, "badinka"),
    })


@app.route('/favicon.ico')
def favicon():
    return '', 204


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
     <button id="btn-candycatz" onclick="switchBrand('candy catz')">Candy Catz</button>
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
            <button onclick="goToToday()" style="margin-left: 10px; background-color: #28a745; color: white; border: none; padding: 5px 10px; border-radius: 3px;">Today</button>
          </div>
          <canvas id="bar-left" style="margin-top: 20px; height: 300px;"></canvas>
        </td>
        <td>
          <div style="text-align: center; margin-top: 30px;">
            <button onclick="changeWeek(-1)">⬅️ Previous Week</button>
            <span id="week-label-right" style="margin: 0 20px; font-weight: bold;">This Week</span>
            <button onclick="changeWeek(1)">Next Week ➡️</button>
            <button onclick="goToToday()" style="margin-left: 10px; background-color: #28a745; color: white; border: none; padding: 5px 10px; border-radius: 3px;">Today</button>
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
       document.getElementById("btn-candycatz").disabled = (brand === "candy catz");
       document.getElementById("btn-stats").disabled = false;
       document.getElementById("csv-btn").style.display = 'inline-block';
       document.getElementById("pdf-btn").style.display = 'none';
       loadData();
     }

         function showStats() {
       document.getElementById("mentions-tab").style.display = "none";
       document.getElementById("stats-tab").style.display = "block";
       document.getElementById("btn-badinka").disabled = false;
       document.getElementById("btn-candycatz").disabled = false;
       document.getElementById("btn-stats").disabled = true;
       document.getElementById("csv-btn").style.display = 'none';
       document.getElementById("pdf-btn").style.display = 'inline-block';
       loadStats();
     }

    function changeWeek(offset) {
      weekOffset += offset;
      loadWeeklyCharts();
    }

    function goToToday() {
      weekOffset = 0;
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
       const leftBrand = window.BRANDS[0] || 'badinka';
       const rightBrand = window.BRANDS[1] || window.BRANDS[0] || 'candy catz';
       fetch(`/stats?brand=${encodeURIComponent(leftBrand)}&tz_offset=${tzOffset}`).then(res => res.json()).then(data => renderStats(data, "left"));
       fetch(`/stats?brand=${encodeURIComponent(rightBrand)}&tz_offset=${tzOffset}`).then(res => res.json()).then(data => renderStats(data, "right"));
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
      const keys = days.map(d => d.toLocaleDateString('en-CA'));
      const leftBrand = window.BRANDS[0] || 'badinka';
      const rightBrand = window.BRANDS[1] || window.BRANDS[0] || 'candy catz';
      Promise.all([
        fetch(`/weekly_mentions?brand=${encodeURIComponent(leftBrand)}&tz=${tz}&week_offset=${weekOffset}`).then(res => res.json()),
        fetch(`/weekly_mentions?brand=${encodeURIComponent(rightBrand)}&tz=${tz}&week_offset=${weekOffset}`).then(res => res.json())
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
  <script>if (!window.BRANDS) window.BRANDS = ['badinka', 'candy catz'];</script>
</body>
</html>
'''


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------

def main():
    global db_manager, reddit_monitor

    port = CONFIG['port']
    logger.info("🚀 Starting Reddit Brand Monitor v3.0")

    # Persistent storage: prefer the Railway volume, fall back to local file.
    data_dir = os.path.dirname(CONFIG['database_file'])
    if data_dir and not os.path.isdir(data_dir):
        try:
            os.makedirs(data_dir, mode=0o755, exist_ok=True)
        except OSError as e:
            logger.warning(f"Cannot create {data_dir} ({e}); using local reddit_monitor.db "
                           f"- data will NOT survive redeploys without a volume!")
            CONFIG['database_file'] = 'reddit_monitor.db'

    db_manager = DatabaseManager(CONFIG['database_file'])
    logger.info(f"✅ Database ready: {CONFIG['database_file']} "
                f"({db_manager.count_mentions()} existing mentions)")

    reddit_monitor = RedditMonitor(CONFIG, db_manager)
    reddit_monitor.start()

    logger.info(f"🌐 Web interface on port {port}")
    try:
        from waitress import serve
        serve(app, host='0.0.0.0', port=port, threads=8)
    except ImportError:
        logger.warning("waitress not installed, using Flask dev server")
        app.run(host='0.0.0.0', port=port, debug=False, threaded=True)


if __name__ == "__main__":
    main()
