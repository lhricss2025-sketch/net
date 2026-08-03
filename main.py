import asyncio
import json
import os
import re
import random
import string
import time
import ssl
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dotenv import load_dotenv

# Fix for Railway asyncio issues
try:
    asyncio.get_running_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

# Fix SSL for Railway
ssl._create_default_https_context = ssl._create_unverified_context

import aiohttp
import requests
import libsql_experimental as libsql
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputFile,
    ChatMember,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

# ============================================================
# LOAD ENVIRONMENT VARIABLES
# ============================================================
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
TURSO_DATABASE_URL = os.getenv("TURSO_DATABASE_URL", "libsql://your-database.turso.io")
TURSO_AUTH_TOKEN = os.getenv("TURSO_AUTH_TOKEN", "your-auth-token")

ADMIN_IDS = [int(id) for id in os.getenv("ADMIN_IDS", "123456789").split(",")]

FORCE_JOIN_CHANNELS = [
    {"id": "Premium Stuff", "link": "https://t.me/+wRaiiDUT9DB41ZiVE9"},
    {"id": "@Senzo_Official", "link": "https://t.me/Senzo_Official"},
]

WORKING_COOLDOWN_MINUTES = 10
MAX_ACCOUNTS_PER_USER = 3
REPORT_TIMEOUT_MINUTES = 30

# ============================================================
# DATABASE WITH RETRY LOGIC
# ============================================================
def get_db_with_retry():
    max_retries = 5
    for i in range(max_retries):
        try:
            db = Database()
            print(f"✅ Database connected successfully!")
            return db
        except Exception as e:
            print(f"⚠️ DB connection attempt {i+1} failed: {e}")
            time.sleep(3)
    raise Exception("❌ Could not connect to database after multiple attempts")

# ============================================================
# TURSO DATABASE CONNECTION
# ============================================================
class Database:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize()
        return cls._instance
    
    def _initialize(self):
        try:
            self.conn = libsql.connect(
                TURSO_DATABASE_URL,
                auth_token=TURSO_AUTH_TOKEN
            )
            self._init_tables()
        except Exception as e:
            print(f"❌ Database init error: {e}")
            raise
    
    def _init_tables(self):
        # Users table
        self.conn.execute('''
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
                accounts_used INT DEFAULT 0
            )
        ''')
        
        # Accounts table
        self.conn.execute('''
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
                FOREIGN KEY (assigned_to) REFERENCES users(user_id)
            )
        ''')
        
        # Reports table
        self.conn.execute('''
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
                FOREIGN KEY (user_id) REFERENCES users(user_id),
                FOREIGN KEY (account_id) REFERENCES accounts(id)
            )
        ''')
        
        # Messages table
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                message TEXT,
                admin_reply TEXT,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                replied_at TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        ''')
        
        # Channels table
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS channels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id TEXT UNIQUE,
                channel_name TEXT,
                invite_link TEXT,
                is_active BOOLEAN DEFAULT 1
            )
        ''')
        
        # Stock logs
        self.conn.execute('''
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
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Insert default channels
        for channel in FORCE_JOIN_CHANNELS:
            self.conn.execute('''
                INSERT OR IGNORE INTO channels (channel_id, channel_name, invite_link, is_active)
                VALUES (?, ?, ?, 1)
            ''', (channel["id"], channel["id"], channel["link"]))
        
        self.conn.commit()
    
    def execute(self, query, params=None):
        if params:
            return self.conn.execute(query, params)
        return self.conn.execute(query)
    
    def commit(self):
        self.conn.commit()
    
    def close(self):
        self.conn.close()

# ============================================================
# DATABASE HELPERS
# ============================================================
db = get_db_with_retry()

def get_user(user_id: int) -> Optional[Dict]:
    result = db.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
    row = result.fetchone()
    if row:
        return {
            "user_id": row[0],
            "username": row[1],
            "first_name": row[2],
            "joined_at": row[3],
            "is_banned": row[4],
            "is_admin": row[5],
            "last_account_time": row[6],
            "total_working": row[7],
            "total_notworking": row[8],
            "working_reports": row[9],
            "notworking_reports": row[10],
            "accounts_used": row[11],
        }
    return None

def create_user(user_id: int, username: str, first_name: str):
    is_admin = 1 if user_id in ADMIN_IDS else 0
    db.execute('''
        INSERT OR IGNORE INTO users (user_id, username, first_name, is_admin)
        VALUES (?, ?, ?, ?)
    ''', (user_id, username, first_name, is_admin))
    db.commit()

def update_user_username(user_id: int, username: str):
    db.execute('UPDATE users SET username = ? WHERE user_id = ?', (username, user_id))
    db.commit()

def get_available_accounts() -> List[Dict]:
    result = db.execute('''
        SELECT id, email, country, plan, cookies, nftoken, nftoken_expiry, status
        FROM accounts 
        WHERE status = 'available' AND is_working = 1
        ORDER BY id ASC
    ''')
    rows = result.fetchall()
    return [
        {
            "id": row[0],
            "email": row[1],
            "country": row[2],
            "plan": row[3],
            "cookies": row[4],
            "nftoken": row[5],
            "nftoken_expiry": row[6],
            "status": row[7],
        }
        for row in rows
    ]

def get_total_accounts() -> Dict:
    result = db.execute('SELECT COUNT(*) FROM accounts WHERE status = "available" AND is_working = 1')
    total = result.fetchone()[0]
    result = db.execute('SELECT plan, COUNT(*) FROM accounts WHERE status = "available" AND is_working = 1 GROUP BY plan')
    plan_counts = {row[0]: row[1] for row in result.fetchall()}
    return {"total": total, "plans": plan_counts}

def assign_account(user_id: int, account_id: int) -> bool:
    # Check if user already has assigned account
    existing = db.execute('SELECT id FROM accounts WHERE assigned_to = ? AND status = "assigned"', (user_id,))
    if existing.fetchone():
        # Release old account first
        db.execute('UPDATE accounts SET assigned_to = NULL, assigned_at = NULL, status = "available" WHERE assigned_to = ?', (user_id,))
    
    db.execute('''
        UPDATE accounts 
        SET assigned_to = ?, assigned_at = CURRENT_TIMESTAMP, status = 'assigned'
        WHERE id = ? AND status = 'available'
    ''', (user_id, account_id))
    
    if db.conn.total_changes() > 0:
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
    result = db.execute('''
        SELECT id, email, country, plan, cookies, nftoken, nftoken_expiry
        FROM accounts 
        WHERE assigned_to = ? AND status = 'assigned'
        ORDER BY assigned_at DESC LIMIT 1
    ''', (user_id,))
    row = result.fetchone()
    if row:
        return {
            "id": row[0],
            "email": row[1],
            "country": row[2],
            "plan": row[3],
            "cookies": row[4],
            "nftoken": row[5],
            "nftoken_expiry": row[6],
        }
    return None

def release_account(account_id: int):
    db.execute('''
        UPDATE accounts 
        SET assigned_to = NULL, assigned_at = NULL, status = 'available'
        WHERE id = ?
    ''', (account_id,))
    db.commit()

def get_pending_reports() -> List[Dict]:
    result = db.execute('''
        SELECT r.id, r.user_id, r.account_id, r.report_type, r.screenshot_file_id, 
               r.status, r.reported_at, u.username, a.email, a.plan
        FROM reports r
        JOIN users u ON r.user_id = u.user_id
        JOIN accounts a ON r.account_id = a.id
        WHERE r.status = 'pending'
        ORDER BY r.reported_at ASC
    ''')
    rows = result.fetchall()
    return [
        {
            "id": row[0],
            "user_id": row[1],
            "account_id": row[2],
            "report_type": row[3],
            "screenshot": row[4],
            "status": row[5],
            "reported_at": row[6],
            "username": row[7],
            "email": row[8],
            "plan": row[9],
        }
        for row in rows
    ]

def create_report(user_id: int, account_id: int, report_type: str, screenshot_file_id: str) -> int:
    db.execute('''
        INSERT INTO reports (user_id, account_id, report_type, screenshot_file_id)
        VALUES (?, ?, ?, ?)
    ''', (user_id, account_id, report_type, screenshot_file_id))
    report_id = db.conn.lastrowid
    
    # Update user stats
    if report_type == "working":
        db.execute('UPDATE users SET working_reports = working_reports + 1 WHERE user_id = ?', (user_id,))
    else:
        db.execute('UPDATE users SET notworking_reports = notworking_reports + 1 WHERE user_id = ?', (user_id,))
    
    db.commit()
    return report_id

def review_report(report_id: int, status: str, admin_id: int, admin_note: str = ""):
    db.execute('''
        UPDATE reports 
        SET status = ?, reviewed_at = CURRENT_TIMESTAMP, admin_note = ?, admin_id = ?
        WHERE id = ?
    ''', (status, admin_note, admin_id, report_id))
    
    # Get report details
    result = db.execute('SELECT account_id, report_type, user_id FROM reports WHERE id = ?', (report_id,))
    row = result.fetchone()
    
    if row and status == "accepted":
        account_id, report_type, user_id = row
        if report_type == "working":
            db.execute('UPDATE accounts SET is_working = 1, status = "available", last_checked = CURRENT_TIMESTAMP WHERE id = ?', (account_id,))
            # Update user working count
            db.execute('UPDATE users SET total_working = total_working + 1 WHERE user_id = ?', (user_id,))
        else:
            db.execute('UPDATE accounts SET is_working = 0, status = "broken", last_checked = CURRENT_TIMESTAMP WHERE id = ?', (account_id,))
            # Update user not working count
            db.execute('UPDATE users SET total_notworking = total_notworking + 1 WHERE user_id = ?', (user_id,))
    
    db.commit()

def ban_user(user_id: int, admin_id: int):
    db.execute('UPDATE users SET is_banned = 1 WHERE user_id = ?', (user_id,))
    db.commit()

def unban_user(user_id: int):
    db.execute('UPDATE users SET is_banned = 0 WHERE user_id = ?', (user_id,))
    db.commit()

def save_account(email: str, country: str, plan: str, cookies: str, nftoken: str, nftoken_expiry: str, source_file: str):
    db.execute('''
        INSERT INTO accounts (email, country, plan, cookies, nftoken, nftoken_expiry, source_file, last_checked)
        VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    ''', (email, country, plan, cookies, nftoken, nftoken_expiry, source_file))
    db.commit()
    return db.conn.lastrowid

def get_all_users() -> List[Dict]:
    result = db.execute('SELECT user_id, username, first_name, joined_at, is_banned, is_admin FROM users ORDER BY joined_at DESC')
    rows = result.fetchall()
    return [
        {
            "user_id": row[0],
            "username": row[1],
            "first_name": row[2],
            "joined_at": row[3],
            "is_banned": row[4],
            "is_admin": row[5],
        }
        for row in rows
    ]

def get_bot_stats() -> Dict:
    result = db.execute('SELECT COUNT(*) FROM users')
    total_users = result.fetchone()[0]
    
    result = db.execute('SELECT COUNT(*) FROM users WHERE is_banned = 0')
    active_users = result.fetchone()[0]
    
    result = db.execute('SELECT COUNT(*) FROM accounts WHERE status = "available" AND is_working = 1')
    available_accounts = result.fetchone()[0]
    
    result = db.execute('SELECT COUNT(*) FROM accounts WHERE is_working = 1')
    total_working = result.fetchone()[0]
    
    result = db.execute('SELECT COUNT(*) FROM reports WHERE status = "pending"')
    pending_reports = result.fetchone()[0]
    
    result = db.execute('SELECT COUNT(*) FROM reports WHERE status = "accepted"')
    accepted_reports = result.fetchone()[0]
    
    result = db.execute('SELECT COUNT(*) FROM reports WHERE status = "rejected"')
    rejected_reports = result.fetchone()[0]
    
    result = db.execute('SELECT COUNT(*) FROM messages WHERE status = "pending"')
    pending_messages = result.fetchone()[0]
    
    return {
        "total_users": total_users,
        "active_users": active_users,
        "available_accounts": available_accounts,
        "total_working": total_working,
        "pending_reports": pending_reports,
        "accepted_reports": accepted_reports,
        "rejected_reports": rejected_reports,
        "pending_messages": pending_messages,
    }

def get_channels() -> List[Dict]:
    result = db.execute('SELECT id, channel_id, channel_name, invite_link, is_active FROM channels')
    rows = result.fetchall()
    return [
        {
            "id": row[0],
            "channel_id": row[1],
            "channel_name": row[2],
            "invite_link": row[3],
            "is_active": row[4],
        }
        for row in rows
    ]

def add_channel(channel_id: str, channel_name: str, invite_link: str):
    db.execute('''
        INSERT OR REPLACE INTO channels (channel_id, channel_name, invite_link, is_active)
        VALUES (?, ?, ?, 1)
    ''', (channel_id, channel_name, invite_link))
    db.commit()

def remove_channel(channel_id: str):
    db.execute('DELETE FROM channels WHERE channel_id = ?', (channel_id,))
    db.commit()

def toggle_channel(channel_id: str, active: bool):
    db.execute('UPDATE channels SET is_active = ? WHERE channel_id = ?', (1 if active else 0, channel_id))
    db.commit()

def create_message(user_id: int, message: str) -> int:
    db.execute('INSERT INTO messages (user_id, message) VALUES (?, ?)', (user_id, message))
    msg_id = db.conn.lastrowid
    db.commit()
    return msg_id

def get_pending_messages() -> List[Dict]:
    result = db.execute('''
        SELECT id, user_id, message, created_at, status
        FROM messages 
        WHERE status = 'pending'
        ORDER BY created_at ASC
    ''')
    rows = result.fetchall()
    return [
        {
            "id": row[0],
            "user_id": row[1],
            "message": row[2],
            "created_at": row[3],
            "status": row[4],
        }
        for row in rows
    ]

def reply_to_message(msg_id: int, reply_text: str):
    db.execute('''
        UPDATE messages 
        SET admin_reply = ?, status = 'replied', replied_at = CURRENT_TIMESTAMP
        WHERE id = ?
    ''', (reply_text, msg_id))
    db.commit()

def get_message_sender(msg_id: int) -> Optional[int]:
    result = db.execute('SELECT user_id FROM messages WHERE id = ?', (msg_id,))
    row = result.fetchone()
    return row[0] if row else None

def get_stock_logs() -> List[Dict]:
    result = db.execute('''
        SELECT id, admin_id, file_name, total_found, valid_found, uploaded_at
        FROM stock_logs
        ORDER BY uploaded_at DESC
        LIMIT 20
    ''')
    rows = result.fetchall()
    return [
        {
            "id": row[0],
            "admin_id": row[1],
            "file_name": row[2],
            "total_found": row[3],
            "valid_found": row[4],
            "uploaded_at": row[5],
        }
        for row in rows
    ]

def log_stock(admin_id: int, file_name: str, total: int, valid: int):
    db.execute('''
        INSERT INTO stock_logs (admin_id, file_name, total_found, valid_found)
        VALUES (?, ?, ?, ?)
    ''', (admin_id, file_name, total, valid))
    db.commit()

def can_get_account(user_id: int) -> Tuple[bool, str]:
    user = get_user(user_id)
    if not user:
        return True, ""
    if user["is_banned"]:
        return False, "🚫 You are banned from using this bot."
    
    # Check cooldown
    last_time = user["last_account_time"]
    if last_time:
        try:
            # Parse timestamp
            if isinstance(last_time, str):
                last = datetime.fromisoformat(last_time.replace(" ", "T"))
            else:
                last = datetime.fromisoformat(str(last_time).replace(" ", "T"))
            diff = datetime.now() - last
            if diff.total_seconds() < WORKING_COOLDOWN_MINUTES * 60:
                remaining = int((WORKING_COOLDOWN_MINUTES * 60 - diff.total_seconds()) / 60)
                return False, f"⏳ Please wait {remaining} minutes before getting another account."
        except:
            pass
    
    # Check max accounts used
    if user["accounts_used"] >= MAX_ACCOUNTS_PER_USER:
        return False, f"⚠️ You have reached the maximum limit of {MAX_ACCOUNTS_PER_USER} accounts per user."
    
    return True, ""

def get_available_plans() -> Dict[str, int]:
    result = db.execute('SELECT plan, COUNT(*) FROM accounts WHERE status = "available" AND is_working = 1 GROUP BY plan')
    return {row[0]: row[1] for row in result.fetchall()}

def get_user_count() -> int:
    result = db.execute('SELECT COUNT(*) FROM users')
    return result.fetchone()[0]

def get_setting(key: str, default: str = "") -> str:
    result = db.execute('SELECT value FROM settings WHERE key = ?', (key,))
    row = result.fetchone()
    if row:
        return row[0]
    return default

def set_setting(key: str, value: str):
    db.execute('''
        INSERT OR REPLACE INTO settings (key, value, updated_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
    ''', (key, value))
    db.commit()

def get_banned_users() -> List[int]:
    result = db.execute('SELECT user_id FROM users WHERE is_banned = 1')
    return [row[0] for row in result.fetchall()]

def get_reports_by_user(user_id: int) -> List[Dict]:
    result = db.execute('''
        SELECT id, report_type, status, reported_at, admin_note
        FROM reports 
        WHERE user_id = ?
        ORDER BY reported_at DESC
        LIMIT 10
    ''', (user_id,))
    rows = result.fetchall()
    return [
        {
            "id": row[0],
            "type": row[1],
            "status": row[2],
            "reported_at": row[3],
            "admin_note": row[4] or "",
        }
        for row in rows
    ]

def get_account_history(user_id: int) -> List[Dict]:
    result = db.execute('''
        SELECT email, plan, assigned_at, status
        FROM accounts 
        WHERE assigned_to = ?
        ORDER BY assigned_at DESC
        LIMIT 5
    ''', (user_id,))
    rows = result.fetchall()
    return [
        {
            "email": row[0],
            "plan": row[1],
            "assigned_at": row[2],
            "status": row[3],
        }
        for row in rows
    ]

# ============================================================
# NFToken Generator
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

def generate_nftoken(netflix_id: str) -> Tuple[Optional[str], Optional[str]]:
    try:
        headers = dict(NFTOKEN_HEADERS)
        headers["Cookie"] = f"NetflixId={netflix_id}"
        
        response = requests.get(
            NFTOKEN_API_URL,
            params=NFTOKEN_QUERY_PARAMS,
            headers=headers,
            timeout=30,
            verify=False,
        )
        if response.status_code != 200:
            return None, None
        
        data = response.json()
        token_data = (
            (((data.get("value") or {}).get("account") or {}).get("token") or {}).get("default")
            or {}
        )
        token = token_data.get("token")
        expires = token_data.get("expires")
        if token:
            return token, expires
    except Exception:
        pass
    return None, None

# ============================================================
# COOKIE PARSER
# ============================================================
def extract_cookies_from_text(content: str) -> List[Dict]:
    cookies = []
    
    netflix_id_pattern = re.compile(r'NetflixId[=:]\s*([^;\s"]+)', re.IGNORECASE)
    secure_pattern = re.compile(r'SecureNetflixId[=:]\s*([^;\s"]+)', re.IGNORECASE)
    
    netflix_id = netflix_id_pattern.search(content)
    secure_id = secure_pattern.search(content)
    
    if netflix_id:
        cookies.append({
            "name": "NetflixId",
            "value": netflix_id.group(1).strip('"\''),
            "domain": ".netflix.com",
            "path": "/",
            "secure": False,
        })
    if secure_id:
        cookies.append({
            "name": "SecureNetflixId",
            "value": secure_id.group(1).strip('"\''),
            "domain": ".netflix.com",
            "path": "/",
            "secure": True,
        })
    
    try:
        data = json.loads(content)
        if isinstance(data, dict):
            if "cookies" in data:
                for cookie in data["cookies"]:
                    if cookie.get("name") in ["NetflixId", "SecureNetflixId"]:
                        cookies.append({
                            "name": cookie["name"],
                            "value": cookie["value"],
                            "domain": cookie.get("domain", ".netflix.com"),
                            "path": cookie.get("path", "/"),
                            "secure": cookie.get("secure", False),
                        })
    except:
        pass
    
    return cookies

def check_account(cookies: List[Dict]) -> Dict:
    if not cookies:
        return {"valid": False, "error": "No cookies found"}
    
    cookie_dict = {c["name"]: c["value"] for c in cookies}
    
    if "NetflixId" not in cookie_dict:
        return {"valid": False, "error": "Missing NetflixId cookie"}
    
    session = requests.Session()
    session.cookies.update(cookie_dict)
    
    try:
        response = session.get(
            "https://www.netflix.com/account/membership",
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept-Encoding": "identity",
            },
            timeout=15,
        )
        
        if response.status_code != 200:
            return {"valid": False, "error": f"HTTP {response.status_code}"}
        
        text = response.text
        
        info = {}
        
        email_match = re.search(r'"emailAddress"\s*:\s*"([^"]+)"', text)
        if email_match:
            info["email"] = email_match.group(1)
        
        country_match = re.search(r'"countryOfSignup":\s*"([^"]+)"', text)
        if country_match:
            info["country"] = country_match.group(1)
        
        plan_match = re.search(r'"localizedPlanName"\s*:\s*"([^"]+)"', text)
        if plan_match:
            info["plan"] = plan_match.group(1)
        
        is_subscribed = "current_member" in text.lower() or "premium" in text.lower() or "standard" in text.lower() or "basic" in text.lower()
        
        if not is_subscribed:
            return {"valid": True, "info": info, "subscribed": False}
        
        nftoken, expiry = generate_nftoken(cookie_dict["NetflixId"])
        
        return {
            "valid": True,
            "subscribed": True,
            "info": info,
            "nftoken": nftoken,
            "nftoken_expiry": expiry,
        }
        
    except requests.exceptions.Timeout:
        return {"valid": False, "error": "Timeout"}
    except requests.exceptions.ConnectionError:
        return {"valid": False, "error": "Connection error"}
    except Exception as e:
        return {"valid": False, "error": str(e)}

# ============================================================
# BOT HANDLERS
# ============================================================

async def check_force_join(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    channels = get_channels()
    channels = [c for c in channels if c["is_active"]]
    
    if not channels:
        return True
    
    for channel in channels:
        try:
            member = await context.bot.get_chat_member(channel["channel_id"], user_id)
            if member.status in [ChatMember.LEFT, ChatMember.KICKED]:
                return False
        except:
            return False
    
    return True

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id
    
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
            "🚨 **Please join our channels first!**\n\n"
            "You must join all channels below to use this bot:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN,
        )
        return
    
    stats = get_total_accounts()
    plans = stats["plans"]
    total_users = get_user_count()
    
    text = f"""
🎬 **Netflix Account Bot** 🎬

👋 Welcome {user.first_name}!

📊 **Available Accounts: {stats['total']}**

"""
    for plan, count in plans.items():
        emoji = "👑" if "premium" in plan.lower() else "⭐" if "standard" in plan.lower() else "🎯" if "basic" in plan.lower() else "📱"
        text += f"{emoji} {plan}: {count}\n"
    
    text += f"""
━━━━━━━━━━━━━━━━━━
👥 Total Users: {total_users}
⏳ Cooldown: {WORKING_COOLDOWN_MINUTES} minutes
📦 Max Accounts: {MAX_ACCOUNTS_PER_USER}
━━━━━━━━━━━━━━━━━━

🔽 Use the buttons below!
"""
    
    keyboard = [
        [InlineKeyboardButton("🎯 Get Account", callback_data="get_account")],
        [InlineKeyboardButton("✅ Working", callback_data="working"),
         InlineKeyboardButton("❌ Not Working", callback_data="notworking")],
        [InlineKeyboardButton("📞 Contact Admin", callback_data="contact")],
        [InlineKeyboardButton("📊 My Status", callback_data="my_status")],
    ]
    
    if user_data and user_data["is_admin"]:
        keyboard.append([InlineKeyboardButton("⚙️ Admin Panel", callback_data="admin_panel")])
    
    await update.message.reply_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.MARKDOWN,
    )

async def check_join_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    
    joined = await check_force_join(user_id, context)
    
    if joined:
        await query.edit_message_text("✅ You've joined all channels! Starting bot...")
        await start(update, context)
    else:
        await query.answer("❌ You haven't joined all channels yet!", show_alert=True)

async def get_account(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    
    user_data = get_user(user_id)
    if user_data and user_data["is_banned"]:
        await query.edit_message_text("🚫 You are banned from using this bot.")
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
        old = get_assigned_account(user_id)
        if old and old["id"] != account["id"]:
            release_account(old["id"])
        
        text = f"""
🎉 **Account Assigned!**

📧 **Email:** `{account['email']}`
🌍 **Country:** {account.get('country', 'Unknown')}
📦 **Plan:** {account.get('plan', 'Unknown')}

**📝 Instructions:**
1. Click the NFToken link below to login
2. If it doesn't work, use the cookie method
3. Report if it's working or not working

"""
        
        keyboard = []
        
        if account.get("nftoken"):
            keyboard.append([
                InlineKeyboardButton("📱 Phone Login", url=f"https://netflix.com/unsupported?nftoken={account['nftoken']}"),
                InlineKeyboardButton("🖥️ PC Login", url=f"https://netflix.com/login?nftoken={account['nftoken']}")
            ])
            keyboard.append([InlineKeyboardButton("📺 TV Login", url=f"https://netflix.com/tv8?nftoken={account['nftoken']}")])
            if account.get("nftoken_expiry"):
                text += f"\n⏳ NFToken expires: {account['nftoken_expiry']}"
        
        keyboard.append([
            InlineKeyboardButton("✅ Working", callback_data=f"report_working_{account['id']}"),
            InlineKeyboardButton("❌ Not Working", callback_data=f"report_notworking_{account['id']}")
        ])
        keyboard.append([InlineKeyboardButton("🔙 Back to Menu", callback_data="back_menu")])
        
        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN,
        )
    else:
        await query.edit_message_text("❌ Failed to assign account. Please try again!")

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
        await query.edit_message_text("🚫 You are banned from using this bot.")
        return
    
    context.user_data["pending_report"] = {
        "account_id": account_id,
        "report_type": report_type,
    }
    
    await query.edit_message_text(
        f"📸 Please upload a **screenshot proof** for this {report_type} report.\n\n"
        "This will be sent to admin for verification.",
        parse_mode=ParseMode.MARKDOWN,
    )

async def handle_screenshot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_data = get_user(user_id)
    
    if user_data and user_data["is_banned"]:
        await update.message.reply_text("🚫 You are banned.")
        return
    
    pending = context.user_data.get("pending_report")
    if not pending:
        await update.message.reply_text("❌ No pending report. Use /working or /notworking first.")
        return
    
    photo = update.message.photo
    if not photo:
        await update.message.reply_text("❌ Please upload an image/screenshot.")
        return
    
    file_id = photo[-1].file_id
    
    report_id = create_report(
        user_id,
        pending["account_id"],
        pending["report_type"],
        file_id,
    )
    
    context.user_data["pending_report"] = None
    
    await update.message.reply_text(
        "✅ **Report submitted!**\n\n"
        "Admin will review your report shortly.\n"
        "Please wait for verification.",
        parse_mode=ParseMode.MARKDOWN,
    )
    
    await notify_admins_report(context.bot, report_id, user_id, pending["account_id"], pending["report_type"])

async def notify_admins_report(bot, report_id: int, user_id: int, account_id: int, report_type: str):
    user = get_user(user_id)
    account = get_assigned_account(user_id)
    
    text = f"""
📋 **New Report Received!**

Report ID: #{report_id}
User: {user.get('username', 'Unknown')} (ID: {user_id})
Type: {report_type.upper()}
Account: {account.get('email', 'Unknown')} ({account.get('plan', 'Unknown')})

🔍 Use /reports to review all pending reports.
"""
    
    result = db.execute('SELECT screenshot_file_id FROM reports WHERE id = ?', (report_id,))
    row = result.fetchone()
    
    keyboard = [
        [
            InlineKeyboardButton("✅ Accept", callback_data=f"review_accept_{report_id}"),
            InlineKeyboardButton("❌ Reject", callback_data=f"review_reject_{report_id}"),
        ],
        [InlineKeyboardButton("👁️ View All Reports", callback_data="admin_reports")],
    ]
    
    for admin_id in ADMIN_IDS:
        try:
            if row:
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
            pass

async def review_report_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    data = query.data
    parts = data.split("_")
    action = parts[1]
    report_id = int(parts[2])
    
    user_id = query.from_user.id
    
    if user_id not in ADMIN_IDS:
        await query.answer("⛔ You are not authorized!", show_alert=True)
        return
    
    status = "accepted" if action == "accept" else "rejected"
    review_report(report_id, status, user_id)
    
    result = db.execute('SELECT user_id, report_type, account_id FROM reports WHERE id = ?', (report_id,))
    row = result.fetchone()
    
    if row:
        user_id_reporter, report_type, account_id = row
        
        try:
            if status == "accepted":
                msg = f"✅ Your report (#{report_id}) was **accepted** by admin!"
                if report_type == "working":
                    msg += "\n\n🎉 Account marked as working. Thank you!"
                else:
                    msg += "\n\n🔄 Account has been removed from stock. Thank you!"
            else:
                msg = f"❌ Your report (#{report_id}) was **rejected** by admin.\n\n⚠️ Please only submit genuine reports. Fake reports may lead to ban."
            
            await context.bot.send_message(user_id_reporter, msg, parse_mode=ParseMode.MARKDOWN)
        except:
            pass
    
    await query.edit_message_text(f"✅ Report #{report_id} has been {status}!")

async def contact_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    
    user_data = get_user(user_id)
    if user_data and user_data["is_banned"]:
        await query.edit_message_text("🚫 You are banned.")
        return
    
    await query.edit_message_text(
        "📝 **Contact Admin**\n\n"
        "Please type your message below.\n"
        "Admin will reply to you as soon as possible.",
        parse_mode=ParseMode.MARKDOWN,
    )
    context.user_data["waiting_for_message"] = True

async def handle_user_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_data = get_user(user_id)
    
    if user_data and user_data["is_banned"]:
        await update.message.reply_text("🚫 You are banned.")
        return
    
    if context.user_data.get("waiting_for_message"):
        message_text = update.message.text
        if not message_text:
            await update.message.reply_text("❌ Please send text message.")
            return
        
        msg_id = create_message(user_id, message_text)
        context.user_data["waiting_for_message"] = False
        
        await update.message.reply_text(
            "✅ **Message sent to admin!**\n\n"
            "You will receive a reply here when admin responds.",
            parse_mode=ParseMode.MARKDOWN,
        )
        
        for admin_id in ADMIN_IDS:
            try:
                await context.bot.send_message(
                    admin_id,
                    f"📩 **New Message from User**\n\n"
                    f"User: {user_data.get('username', 'Unknown')} (ID: {user_id})\n"
                    f"Message: {message_text}\n\n"
                    f"Use `/reply {msg_id} <message>` to reply.",
                    parse_mode=ParseMode.MARKDOWN,
                )
            except:
                pass
        return
    
    if user_id in ADMIN_IDS:
        text = update.message.text
        if text and text.startswith("/reply "):
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
                            f"📩 **Admin Reply:**\n\n{reply_text}",
                            parse_mode=ParseMode.MARKDOWN,
                        )
                        await update.message.reply_text(f"✅ Reply sent to user!")
                    else:
                        await update.message.reply_text("❌ Message not found.")
                except:
                    await update.message.reply_text("❌ Invalid format. Use: /reply <msg_id> <message>")
            return

async def my_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    user = get_user(user_id)
    
    if not user:
        await query.edit_message_text("❌ User not found. Use /start first.")
        return
    
    account = get_assigned_account(user_id)
    reports = get_reports_by_user(user_id)
    history = get_account_history(user_id)
    
    text = f"""
📊 **Your Status**

👤 **Name:** {user.get('first_name', 'Unknown')}
🆔 **ID:** {user_id}
📅 **Joined:** {user.get('joined_at', 'Unknown')}
✅ **Working Reports:** {user.get('working_reports', 0)}
❌ **Not Working Reports:** {user.get('notworking_reports', 0)}
📦 **Accounts Used:** {user.get('accounts_used', 0)}/{MAX_ACCOUNTS_PER_USER}

🔑 **Current Assigned Account:**
"""
    
    if account:
        text += f"""
📧 {account['email']}
🌍 {account.get('country', 'Unknown')}
📦 {account.get('plan', 'Unknown')}
"""
    else:
        text += "None\n"
    
    if reports:
        text += "\n📋 **Recent Reports:**\n"
        for r in reports[:5]:
            status_emoji = "✅" if r["status"] == "accepted" else "❌" if r["status"] == "rejected" else "⏳"
            text += f"{status_emoji} #{r['id']} - {r['type']} - {r['status']}\n"
    
    if history:
        text += "\n📜 **Recent Accounts:**\n"
        for h in history[:3]:
            text += f"▫️ {h['email']} - {h['plan']}\n"
    
    text += f"\n⏳ Cooldown: {WORKING_COOLDOWN_MINUTES} minutes"
    
    keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="back_menu")]]
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)

async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    
    if user_id not in ADMIN_IDS:
        await query.answer("⛔ You are not authorized!", show_alert=True)
        return
    
    stats = get_bot_stats()
    pending_reports = get_pending_reports()
    pending_messages = get_pending_messages()
    
    text = f"""
⚙️ **Admin Panel**

📊 **Statistics:**
┌─────────────────────
│ 👥 Total Users: {stats['total_users']}
│ ✅ Active Users: {stats['active_users']}
│ 📦 Available Accounts: {stats['available_accounts']}
│ 🟢 Working Accounts: {stats['total_working']}
│ 📋 Pending Reports: {stats['pending_reports']}
│ ✅ Accepted Reports: {stats['accepted_reports']}
│ ❌ Rejected Reports: {stats['rejected_reports']}
│ 📩 Pending Messages: {stats['pending_messages']}
└─────────────────────

📋 **Pending Reports:** {len(pending_reports)}
"""
    
    keyboard = [
        [InlineKeyboardButton("📤 Upload Stock", callback_data="admin_upload")],
        [InlineKeyboardButton("📋 View Reports", callback_data="admin_reports")],
        [InlineKeyboardButton("📩 View Messages", callback_data="admin_messages")],
        [InlineKeyboardButton("👥 Users List", callback_data="admin_users")],
        [InlineKeyboardButton("📢 Broadcast", callback_data="admin_broadcast")],
        [InlineKeyboardButton("⚙️ Manage Channels", callback_data="admin_channels")],
        [InlineKeyboardButton("📊 Stock Logs", callback_data="admin_stock_logs")],
        [InlineKeyboardButton("🔙 Back", callback_data="back_menu")],
    ]
    
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
        "📤 **Upload Cookie File**\n\n"
        "Please send the cookie file (.txt or .json) containing Netflix cookies.\n\n"
        "The bot will check and add valid accounts to stock.",
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
    if not file_name.lower().endswith(('.txt', '.json')):
        await update.message.reply_text("❌ Please send .txt or .json file.")
        return
    
    await update.message.reply_text("⏳ Processing file... This may take a while.")
    
    try:
        file = await context.bot.get_file(document.file_id)
        file_content = await file.download_as_bytearray()
        content = file_content.decode('utf-8', errors='ignore')
        
        cookies = extract_cookies_from_text(content)
        
        if not cookies:
            await update.message.reply_text("❌ No Netflix cookies found in file.")
            context.user_data["waiting_for_upload"] = False
            return
        
        seen_ids = set()
        unique_cookies = []
        for c in cookies:
            if c["name"] == "NetflixId" and c["value"] not in seen_ids:
                seen_ids.add(c["value"])
                unique_cookies.append(c)
        
        progress_msg = await update.message.reply_text(f"🔄 Checking {len(unique_cookies)} accounts...")
        
        valid = 0
        added = 0
        
        for i, cookie in enumerate(unique_cookies):
            cookie_dict = {cookie["name"]: cookie["value"]}
            for c in cookies:
                if c["name"] == "SecureNetflixId" and c["value"]:
                    cookie_dict["SecureNetflixId"] = c["value"]
            
            result = check_account([{"name": k, "value": v} for k, v in cookie_dict.items()])
            
            if result.get("valid") and result.get("subscribed"):
                info = result.get("info", {})
                email = info.get("email", f"user_{random.randint(1000, 9999)}")
                country = info.get("country", "Unknown")
                plan = info.get("plan", "Unknown")
                nftoken = result.get("nftoken")
                nftoken_expiry = result.get("nftoken_expiry")
                
                cookie_str = "\n".join([f"{k}\tTRUE\t/\tFALSE\t0\t{k}\t{v}" for k, v in cookie_dict.items()])
                
                save_account(email, country, plan, cookie_str, nftoken, nftoken_expiry, file_name)
                added += 1
                valid += 1
            
            if (i + 1) % 5 == 0:
                try:
                    await progress_msg.edit_text(f"🔄 Checking accounts... {i+1}/{len(unique_cookies)} | Found: {valid}")
                except:
                    pass
        
        log_stock(user_id, file_name, len(unique_cookies), added)
        
        context.user_data["waiting_for_upload"] = False
        
        await progress_msg.edit_text(
            f"✅ **Stock Upload Complete!**\n\n"
            f"📁 File: {file_name}\n"
            f"🔍 Total Checked: {len(unique_cookies)}\n"
            f"✅ Valid & Added: {added}\n"
            f"❌ Invalid: {len(unique_cookies) - added}\n\n"
            f"📊 New Total Accounts: {get_total_accounts()['total']}",
            parse_mode=ParseMode.MARKDOWN,
        )
        
    except Exception as e:
        await update.message.reply_text(f"❌ Error processing file: {str(e)}")
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
        await query.edit_message_text(
            "📋 **No pending reports.**\n\nAll reports have been reviewed.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return
    
    text = "📋 **Pending Reports:**\n\n"
    keyboard = []
    
    for r in reports[:10]:
        text += f"#{r['id']} | {r['report_type'].upper()} | {r['username']} | {r['email']}\n"
        keyboard.append([
            InlineKeyboardButton(f"#{r['id']} - {r['report_type'].upper()}", callback_data=f"view_report_{r['id']}")
        ])
    
    keyboard.append([InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel")])
    
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.MARKDOWN,
    )

async def admin_channels(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    
    if user_id not in ADMIN_IDS:
        await query.answer("⛔ Not authorized!", show_alert=True)
        return
    
    channels = get_channels()
    
    text = "⚙️ **Manage Channels**\n\n"
    
    for ch in channels:
        status = "🟢 Active" if ch["is_active"] else "🔴 Inactive"
        text += f"▫️ {ch['channel_name']} - {status}\n"
    
    text += f"\nTotal: {len(channels)} channels"
    
    keyboard = [
        [InlineKeyboardButton("➕ Add Channel", callback_data="admin_add_channel")],
        [InlineKeyboardButton("❌ Remove Channel", callback_data="admin_remove_channel")],
        [InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel")],
    ]
    
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
        "➕ **Add Channel**\n\n"
        "Please send the channel details in this format:\n\n"
        "`channel_id, channel_name, invite_link`\n\n"
        "Example: `@mychannel, My Channel, https://t.me/mychannel`",
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
        await query.edit_message_text("❌ No channels to remove.")
        return
    
    keyboard = []
    for ch in channels:
        keyboard.append([
            InlineKeyboardButton(f"❌ {ch['channel_name']}", callback_data=f"remove_ch_{ch['channel_id']}")
        ])
    keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="admin_channels")])
    
    await query.edit_message_text(
        "Select channel to remove:",
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
    
    await query.edit_message_text(f"✅ Channel {channel_id} has been removed!")

async def admin_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    
    if user_id not in ADMIN_IDS:
        await query.answer("⛔ Not authorized!", show_alert=True)
        return
    
    users = get_all_users()
    
    text = "👥 **Users List**\n\n"
    for u in users[:20]:
        status = "🚫 Banned" if u["is_banned"] else "✅ Active"
        admin = "⭐ Admin" if u["is_admin"] else "👤 User"
        text += f"▫️ {u['username'] or u['first_name']} - {status} - {admin}\n"
    
    text += f"\nTotal: {len(users)} users"
    
    keyboard = [
        [InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel")],
    ]
    
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.MARKDOWN,
    )

async def admin_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    
    if user_id not in ADMIN_IDS:
        await query.answer("⛔ Not authorized!", show_alert=True)
        return
    
    await query.edit_message_text(
        "📢 **Broadcast**\n\n"
        "Send the message you want to broadcast to all users.",
        parse_mode=ParseMode.MARKDOWN,
    )
    context.user_data["waiting_for_broadcast"] = True

async def handle_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("⛔ Not authorized!")
        return
    
    if not context.user_data.get("waiting_for_broadcast"):
        return
    
    message_text = update.message.text
    if not message_text:
        await update.message.reply_text("❌ Please send text message.")
        return
    
    context.user_data["waiting_for_broadcast"] = False
    
    users = get_all_users()
    
    sent = 0
    failed = 0
    
    progress = await update.message.reply_text(f"⏳ Sending broadcast to {len(users)} users...")
    
    for u in users:
        if u["is_banned"]:
            continue
        try:
            await context.bot.send_message(u["user_id"], message_text, parse_mode=ParseMode.MARKDOWN)
            sent += 1
        except:
            failed += 1
        await asyncio.sleep(0.05)
    
    await progress.edit_text(
        f"✅ **Broadcast Complete!**\n\n"
        f"✅ Sent: {sent}\n"
        f"❌ Failed: {failed}\n"
        f"📊 Total Users: {len(users)}",
        parse_mode=ParseMode.MARKDOWN,
    )

async def admin_stock_logs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    
    if user_id not in ADMIN_IDS:
        await query.answer("⛔ Not authorized!", show_alert=True)
        return
    
    logs = get_stock_logs()
    
    if not logs:
        await query.edit_message_text("📊 No stock uploads yet.")
        return
    
    text = "📊 **Stock Upload Logs**\n\n"
    for log in logs:
        text += f"📁 {log['file_name']}\n"
        text += f"   📅 {log['uploaded_at']}\n"
        text += f"   🔍 Total: {log['total_found']} | ✅ Valid: {log['valid_found']}\n\n"
    
    keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]]
    
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
        await query.edit_message_text("📩 **No pending messages.**", parse_mode=ParseMode.MARKDOWN)
        return
    
    text = "📩 **Pending Messages from Users:**\n\n"
    keyboard = []
    
    for msg in messages[:10]:
        user = get_user(msg["user_id"])
        username = user.get("username", "Unknown") if user else "Unknown"
        text += f"#{msg['id']} | {username} | {msg['message'][:30]}...\n"
        keyboard.append([
            InlineKeyboardButton(f"#{msg['id']} - Reply", callback_data=f"reply_msg_{msg['id']}")
        ])
    
    keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="admin_panel")])
    
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
        f"Send your reply as: `/reply {msg_id} <your message>`",
        parse_mode=ParseMode.MARKDOWN,
    )

async def back_to_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    await start(update, context)

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    if context.user_data.get("waiting_for_channel"):
        text = update.message.text
        parts = text.split(",")
        if len(parts) == 3:
            channel_id = parts[0].strip()
            channel_name = parts[1].strip()
            invite_link = parts[2].strip()
            
            add_channel(channel_id, channel_name, invite_link)
            context.user_data["waiting_for_channel"] = False
            
            await update.message.reply_text(f"✅ Channel {channel_name} added successfully!")
            return
        else:
            await update.message.reply_text("❌ Invalid format. Use: `channel_id, channel_name, invite_link`")
            return
    
    if user_id in ADMIN_IDS:
        text = update.message.text
        if text and text.startswith("/ban "):
            parts = text.split()
            if len(parts) == 2:
                try:
                    target_id = int(parts[1])
                    ban_user(target_id, user_id)
                    await update.message.reply_text(f"✅ User {target_id} has been banned!")
                except:
                    await update.message.reply_text("❌ Invalid user ID.")
            return
        
        if text and text.startswith("/unban "):
            parts = text.split()
            if len(parts) == 2:
                try:
                    target_id = int(parts[1])
                    unban_user(target_id)
                    await update.message.reply_text(f"✅ User {target_id} has been unbanned!")
                except:
                    await update.message.reply_text("❌ Invalid user ID.")
            return

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print(f"Error: {context.error}")
    try:
        if update and update.effective_message:
            await update.effective_message.reply_text("❌ An error occurred. Please try again later.")
    except:
        pass

# ============================================================
# HEALTH CHECK SERVER FOR RAILWAY
# ============================================================
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(b'OK')
    
    def log_message(self, format, *args):
        pass  # Suppress logs to avoid spam

def start_health_server():
    port = int(os.getenv('PORT', 8080))
    server = HTTPServer(('0.0.0.0', port), HealthCheckHandler)
    print(f"✅ Health check server running on port {port}")
    server.serve_forever()

# ============================================================
# MAIN
# ============================================================
def main():
    print("🤖 Netflix Account Bot Starting...")
    print(f"📊 Database: Turso ({TURSO_DATABASE_URL})")
    print(f"👤 Admin IDs: {ADMIN_IDS}")
    print(f"⏳ Cooldown: {WORKING_COOLDOWN_MINUTES} minutes")
    print(f"📦 Max Accounts Per User: {MAX_ACCOUNTS_PER_USER}")
    print("✅ Bot is running...")
    
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Commands
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", start))
    
    # Callbacks
    application.add_handler(CallbackQueryHandler(check_join_callback, pattern="check_join"))
    application.add_handler(CallbackQueryHandler(get_account, pattern="get_account"))
    application.add_handler(CallbackQueryHandler(report_account, pattern="^report_"))
    application.add_handler(CallbackQueryHandler(contact_admin, pattern="contact"))
    application.add_handler(CallbackQueryHandler(my_status, pattern="my_status"))
    application.add_handler(CallbackQueryHandler(admin_panel, pattern="admin_panel"))
    application.add_handler(CallbackQueryHandler(admin_upload, pattern="admin_upload"))
    application.add_handler(CallbackQueryHandler(admin_reports, pattern="admin_reports"))
    application.add_handler(CallbackQueryHandler(admin_messages, pattern="admin_messages"))
    application.add_handler(CallbackQueryHandler(admin_users, pattern="admin_users"))
    application.add_handler(CallbackQueryHandler(admin_broadcast, pattern="admin_broadcast"))
    application.add_handler(CallbackQueryHandler(admin_channels, pattern="admin_channels"))
    application.add_handler(CallbackQueryHandler(admin_add_channel, pattern="admin_add_channel"))
    application.add_handler(CallbackQueryHandler(admin_remove_channel, pattern="admin_remove_channel"))
    application.add_handler(CallbackQueryHandler(admin_stock_logs, pattern="admin_stock_logs"))
    application.add_handler(CallbackQueryHandler(back_to_menu, pattern="back_menu"))
    application.add_handler(CallbackQueryHandler(review_report_callback, pattern="^review_"))
    application.add_handler(CallbackQueryHandler(reply_message_callback, pattern="^reply_msg_"))
    application.add_handler(CallbackQueryHandler(remove_channel_callback, pattern="^remove_ch_"))
    
    # Message handlers
    application.add_handler(MessageHandler(filters.PHOTO, handle_screenshot))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_file_upload))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_user_message))
    application.add_handler(MessageHandler(filters.TEXT & filters.COMMAND, handle_text))
    
    # Broadcast handler
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_broadcast))
    
    # Error handler
    application.add_error_handler(error_handler)
    
    # Start bot polling
    try:
        print("🚀 Starting bot polling...")
        application.run_polling(allowed_updates=Update.ALL_TYPES)
    except Exception as e:
        print(f"⚠️ Polling error: {e}")

if __name__ == "__main__":
    # Start health check server first
    health_thread = threading.Thread(target=start_health_server, daemon=True)
    health_thread.start()
    
    # Small delay to ensure health server starts
    time.sleep(1)
    
    # Run the bot
    main()
