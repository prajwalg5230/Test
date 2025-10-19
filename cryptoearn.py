import logging
import json
import re
import random
from datetime import datetime, timedelta
import asyncio
import sqlite3

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, Bot, BotCommand
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
    ConversationHandler,
)
from telegram.error import TelegramError, BadRequest

# --- CONFIGURATION ---
BOT_TOKEN = "8138076906:AAH0ckhuJ7bMrjJbnB5zUWT7lyrkrcTRyP8"
BOT_USERNAME = "CRYPTOEARNPREMIUMBOT"
DB_NAME = "cryptoearn.db"
OWNER_ID = 7521967912
SUPPORT_CONTACT = "@YourSupportUsername"

# --- NOTIFICATION BOTS CONFIG ---
WITHDRAWAL_BOT_TOKEN = "7679640674:AAET4ci3o0JLP5pWQcWnLhmMuCJAsmvkeEA"
WITHDRAWAL_GROUP_CHAT_ID = -1002802113172
GAME_RESULT_BOT_TOKEN = "8081204980:AAGXUzwXyRWFFguqaTENaf1plMkgKg39s5g"
GAME_RESULT_GROUP_CHAT_ID = -1002456184953

# --- GAME & ECONOMY VALUES ---
INITIAL_BONUS_INR = 100.0; HOURLY_BONUS_INR = 50.0; DAILY_BONUS_INR = 250.0; WEEKLY_BONUS_INR = 1000.0;
MIN_WITHDRAWAL_INR = 2000.0; MIN_REFERRALS_FOR_WITHDRAWAL = 15
REFERRAL_BONUS_INR = (MIN_WITHDRAWAL_INR - INITIAL_BONUS_INR) / MIN_REFERRALS_FOR_WITHDRAWAL
LUCKY_DRAW_CONFIG = {"win_chance": 0.5, "win_multiplier": 2.0}
MINING_LEVELS = {
    1: {"speed_inr_per_day": (MIN_WITHDRAWAL_INR / 15), "upgrade_cost_inr": 0},
    2: {"speed_inr_per_day": (MIN_WITHDRAWAL_INR / 12), "upgrade_cost_inr": MIN_WITHDRAWAL_INR},
    3: {"speed_inr_per_day": (MIN_WITHDRAWAL_INR / 9),  "upgrade_cost_inr": MIN_WITHDRAWAL_INR},
    4: {"speed_inr_per_day": (MIN_WITHDRAWAL_INR / 6),  "upgrade_cost_inr": MIN_WITHDRAWAL_INR},
    5: {"speed_inr_per_day": (MIN_WITHDRAWAL_INR / 3),  "upgrade_cost_inr": MIN_WITHDRAWAL_INR},
}

# --- CHANNELS CONFIGURATION ---
CHANNELS = [
    {"type": "public", "url": "https://t.me/websappdevgroup", "id": None, "name": "📢 Official Group"},
    {"type": "public", "url": "https://t.me/SufiLogs", "id": None, "name": "🔔 Sponsored Group"},
    {"type": "public", "url": "https://t.me/luckydrawresults", "id": None, "name": "🎰 Game Results"},
    {"type": "public", "url": "https://t.me/CryptoEarnPayout", "id": None, "name": "💳 Payouts Channel"}
]

# --- CRYPTO CONFIGURATION ---
CRYPTO_DATA = {"BTC":{"name":"Bitcoin","inr_rate":5800000,"regex":r"^(bc1|[13])[a-zA-HJ-NP-Z0-9]{25,39}$"},"ETH":{"name":"Ethereum","inr_rate":315000,"regex":r"^0x[a-fA-F0-9]{40}$"},"SOL":{"name":"Solana","inr_rate":14000,"regex":r"^[1-9A-HJ-NP-Za-km-z]{32,44}$"},"TWT":{"name":"Trust Wallet Token","inr_rate":95,"regex":r"^bnb1[0-9a-z]{38}$"},"BNB":{"name":"BNB","inr_rate":50000,"regex":r"^bnb1[0-9a-z]{38}$"},"USDT":{"name":"USDT (TRC20)","inr_rate":90,"regex":r"^T[A-Za-z1-9]{33}$"},"USDC":{"name":"USDC (TRC20)","inr_rate":90,"regex":r"^T[A-Za-z1-9]{33}$"},"TON":{"name":"Toncoin","inr_rate":650,"regex":r"^(EQ|UQ)[A-Za-z0-9_\-]{46}$"},"XRP":{"name":"XRP","inr_rate":42,"regex":r"^r[a-zA-Z0-9]{24,34}$"},"DOGE":{"name":"Dogecoin","inr_rate":13,"regex":r"^D[5-9A-HJ-NP-U][1-9A-HJ-NP-Za-km-z]{32}$"},"TRX":{"name":"Tron","inr_rate":10,"regex":r"^T[A-Za-z1-9]{33}$"},"AVAX":{"name":"Avalanche","inr_rate":3000,"regex":r"^(X-avax1|C-avax1)[0-9a-z]{38}$"},"SUI":{"name":"Sui","inr_rate":85,"regex":r"^0x[a-fA-F0-9]{64}$"},"POL":{"name":"Polygon","inr_rate":60,"regex":r"^0x[a-fA-F0-9]{40}$"},"LTC":{"name":"Litecoin","inr_rate":7000,"regex":r"^[LM3][a-km-zA-HJ-NP-Z1-9]{26,33}$"}}

# --- STATE DEFINITIONS & LOGGING ---
AWAIT_ADDRESS, CONFIRM_WITHDRAWAL = range(2); AWAIT_LUCKY_DRAW_BET, CONFIRM_LUCKY_DRAW = range(2)
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO); logging.getLogger("httpx").setLevel(logging.WARNING); logger = logging.getLogger(__name__)

# --- GAME STATE CLASSES ---
class AviatorState:
    def __init__(self): self.state="WAITING"; self.current_multiplier=1.00; self.crash_multiplier=0.00; self.bets={}; self.last_crashes=[]; self.countdown=5
class WingoState:
    def __init__(self):
        self.timers={'1min':60,'3min':180,'5min':300}
        self.period_ids={}; self.bets={}; self.history=[]
    def generate_initial_period_ids(self):
        now=datetime.now()
        for mode in self.timers.keys(): self.period_ids[mode]=f"{now.strftime('%Y%m%d%H%M%S')}{mode}"
aviator_state, wingo_state = AviatorState(), WingoState(); wingo_state.generate_initial_period_ids()
subscribers = set()

# --- PERFORMANCE & STABILITY UPGRADES ---
db_connection = None
user_states = {} # In-memory cache for user data

# --- DATABASE FUNCTIONS (UPGRADED FOR PERFORMANCE) ---
def setup_database():
    """
    एकल, स्थायी और मजबूत डेटाबेस कनेक्शन प्रारंभ करता है।
    उच्च समरूपता के लिए WAL मोड सक्षम करता है और एक उदार टाइमआउट सेट करता है।
    तालिकाएँ और इंडेक्स बनाता है यदि वे मौजूद नहीं हैं।
    """
    global db_connection
    try:
        db_connection = sqlite3.connect(DB_NAME, timeout=15, check_same_thread=False)
        db_connection.row_factory = sqlite3.Row
        db_connection.execute("PRAGMA journal_mode=WAL;")
        db_connection.execute("PRAGMA synchronous=NORMAL;")
        
        with db_connection:
            db_connection.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                chosen_crypto TEXT,
                balance REAL DEFAULT 0.0,
                last_hourly_claim TEXT,
                last_daily_claim TEXT,
                last_weekly_claim TEXT,
                referrals INTEGER DEFAULT 0,
                is_verified INTEGER DEFAULT 0,
                is_admin INTEGER DEFAULT 0,
                join_date TEXT,
                used_codes TEXT DEFAULT '[]',
                aviator_bet INTEGER DEFAULT 10,
                wingo_bet INTEGER DEFAULT 10,
                current_game TEXT,
                last_message_id INTEGER,
                wingo_mode TEXT DEFAULT '1min',
                mining_level INTEGER DEFAULT 1,
                mining_start_time TEXT,
                awaiting_confirmation INTEGER DEFAULT 0
            )
            """)
            db_connection.execute("""
            CREATE TABLE IF NOT EXISTS bonus_codes (
                code TEXT PRIMARY KEY,
                inr_value REAL NOT NULL
            )
            """)
            # Performance: Add index for faster user lookups
            db_connection.execute('CREATE INDEX IF NOT EXISTS idx_user_id ON users(user_id);')
        logger.info("✅ Database initialized with WAL mode.")
    except sqlite3.Error as e:
        logger.critical(f"FATAL: Database setup failed: {e}")
        raise SystemExit(f"Database initialization error: {e}")

def load_all_users_from_db():
    """Loads all users from DB into the in-memory cache at startup."""
    global user_states
    try:
        with db_connection:
            cur = db_connection.cursor()
            cur.execute("SELECT * FROM users")
            rows = cur.fetchall()
            user_states = {row['user_id']: dict(row) for row in rows}
            logger.info(f"✅ Loaded {len(user_states)} users into cache.")
    except sqlite3.Error as e:
        logger.error(f"Failed to load users from DB: {e}")

def get_user_state(user_id):
    """Fetches a user's data from the in-memory cache."""
    return user_states.get(user_id)

def save_user_state(user_id, state_dict):
    """Updates a user's state in both cache and DB."""
    if user_id not in user_states:
        user_states[user_id] = {}
    
    # Update cache
    user_states[user_id].update(state_dict)

    # Persist to DB
    columns = ', '.join(state_dict.keys())
    placeholders = ', '.join(['?'] * len(state_dict))
    values = tuple(state_dict.values())
    
    # Using INSERT OR REPLACE to handle both new and existing users
    query = f"INSERT OR REPLACE INTO users (user_id, {columns}) VALUES (?, {placeholders})"
    
    try:
        with db_connection:
            db_connection.execute(query, (user_id,) + values)
    except sqlite3.Error as e:
        logger.error(f"Failed to save user state for {user_id}: {e}")

def get_db_value(query, params=(), fetchone=False):
    """General purpose DB read function."""
    try:
        cur = db_connection.cursor()
        cur.execute(query, params)
        if fetchone:
            return cur.fetchone()
        return cur.fetchall()
    except sqlite3.Error as e:
        logger.error(f"DB read error: {e} - Query: {query}")
        return None

def execute_db_commit(query, params=()):
    """General purpose DB write function."""
    try:
        with db_connection:
            db_connection.execute(query, params)
        return True
    except sqlite3.Error as e:
        logger.error(f"DB write error: {e} - Query: {query}")
        return False
        
async def update_balance(user_id, amount, reason):
    """Atomically updates a user's balance in cache and database."""
    user_state = get_user_state(user_id)
    if not user_state:
        logger.warning(f"update_balance failed: User {user_id} not found in cache.")
        return False
    
    current_balance = user_state.get('balance', 0.0)
    new_balance = current_balance + amount
    
    # Update cache first for responsiveness
    user_states[user_id]['balance'] = new_balance
    
    # Persist to DB
    if execute_db_commit("UPDATE users SET balance = ? WHERE user_id = ?", (new_balance, user_id)):
        logger.info(f"Balance Update: User {user_id}, Amount {amount:+.8f}, New Bal {new_balance:.8f}, Reason: {reason}")
        return True
    else:
        # Rollback cache if DB write fails
        user_states[user_id]['balance'] = current_balance
        logger.error(f"Failed to update balance for user {user_id} in DB. Cache rolled back.")
        return False

# --- HELPER & SETUP FUNCTIONS ---
async def setup_bot_menu(app: Application):
    bot_commands=[BotCommand("start","🚀 Start Bot"), BotCommand("profile","📊 My Profile"), BotCommand("bonus","🎁 Redeem Code"), BotCommand("referrals","🔗 Referral Link"), BotCommand("help","ℹ️ Help"), BotCommand("panel","👑 Admin Panel")]
    await app.bot.set_my_commands(bot_commands); logger.info("✅ Bot commands set.")

async def post_init_callback(app: Application):
    logger.info("--- Performing Startup Checks & Tasks ---")
    load_all_users_from_db() # Load users into cache
    await setup_bot_menu(app)
    for channel in CHANNELS:
        if not channel.get("id"):
            try:
                username="@"+channel["url"].split('/')[-1]; chat=await app.bot.get_chat(username); channel["id"]=chat.id; logger.info(f"✅ Found ID for '{channel['name']}': {chat.id}")
            except Exception as e: logger.error(f"❌ FATAL: Could not find ID for {channel['name']}. Error: {e}")
    for channel in CHANNELS:
        if not channel.get("id"): logger.error(f"❌ FATAL: ID for '{channel['name']}' not set."); continue
        try:
            member = await app.bot.get_chat_member(channel["id"], app.bot.id)
            if member.status not in ['administrator', 'creator']: logger.error(f"❌ Bot is NOT ADMIN in '{channel['name']}'")
        except Exception as e: logger.error(f"❌ Could not check status for '{channel['name']}'. Error: {e}")
    
    subscribers.update(user_states.keys())
    
    logger.info(f"--- Startup Checks Complete --- Loaded {len(subscribers)} users from cache.")
    app.create_task(aviator_game_loop(app))
    app.create_task(wingo_game_loop(app))
    app.create_task(mining_loop(app))
    app.create_task(game_ui_update_loop(app))
    logger.info("--- Background game loops started ---")
    
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Catches all unhandled exceptions and logs them."""
    logger.error("Exception while handling an update:", exc_info=context.error)
    if isinstance(update, Update) and update.effective_message:
        try:
            await update.effective_message.reply_text("An unexpected error occurred. Please try again or contact support.")
        except Exception as e:
            logger.error(f"Failed to send error message to user: {e}")


# --- CORE BOT & VERIFICATION ---
async def start(update, context):
    user, user_id = update.effective_user, update.effective_user.id
    subscribers.add(user_id)
    user_data = get_user_state(user_id)

    if not user_data:
        new_user_data = {
            'join_date': datetime.now().isoformat(),
            'balance': 0.0, 'referrals': 0, 'is_verified': 0, 'is_admin': 0,
            'used_codes': '[]', 'aviator_bet': 10, 'wingo_bet': 10,
            'mining_level': 1, 'awaiting_confirmation': 0
        }
        user_states[user_id] = new_user_data # Add to cache
        execute_db_commit(
            "INSERT OR IGNORE INTO users (user_id, join_date) VALUES (?, ?)",
            (user_id, new_user_data['join_date'])
        )
        logger.info(f"New user created in DB: {user_id}")

        if context.args and len(context.args) > 0 and context.args[0].isdigit() and context.args[0] != str(user_id):
            ref_id = int(context.args[0])
            referrer_data = get_user_state(ref_id)
            if referrer_data:
                new_referrals = referrer_data.get('referrals', 0) + 1
                save_user_state(ref_id, {'referrals': new_referrals})
                if referrer_data.get("chosen_crypto"):
                    rate = CRYPTO_DATA[referrer_data["chosen_crypto"]]["inr_rate"]
                    bonus = REFERRAL_BONUS_INR / rate
                    await update_balance(ref_id, bonus, "referral_bonus")
                    try:
                        await context.bot.send_message(ref_id, f"🎉 You have a new referral! A bonus of <b>{bonus:.8f} {referrer_data['chosen_crypto']}</b> has been added.", parse_mode=ParseMode.HTML)
                    except TelegramError:
                        pass
        user_data = get_user_state(user_id)

    if user_data.get("is_verified", 0):
        await show_main_menu(update, context, user)
    else:
        text = f"👋 <b>Welcome, {user.first_name}!</b>\n\nTo use this bot, you must join our channels. This helps us grow.\n\n👇 Please join all channels, then click 'Verify'."
        btns = [[InlineKeyboardButton(c['name'],url=c['url'])] for c in CHANNELS] + [[InlineKeyboardButton("✅ Verify Subscription",callback_data="verify_subscription")]]
        await update.message.reply_text(text,reply_markup=InlineKeyboardMarkup(btns),parse_mode=ParseMode.HTML,disable_web_page_preview=True)


async def verify_subscription_handler(update, context):
    query=update.callback_query; await query.answer(); user_id=query.from_user.id; unjoined=[]
    for channel in CHANNELS:
        try:
            member=await context.bot.get_chat_member(chat_id=channel['id'], user_id=user_id)
            if member.status not in ['member','administrator','creator']: unjoined.append(channel)
        except TelegramError: unjoined.append(channel)
    if unjoined:
        text="❌ <b>Verification Failed!</b>\n\nOops! Join these channels and try again:"; btns=[[InlineKeyboardButton(f"➡️ Join {c['name']}",url=c['url'])] for c in unjoined]+[[InlineKeyboardButton("🔄 Verify Again",callback_data="verify_subscription")]]
        await query.message.reply_text(text,reply_markup=InlineKeyboardMarkup(btns),parse_mode=ParseMode.HTML,disable_web_page_preview=True); return
    
    save_user_state(user_id, {'is_verified': 1})
    keys=[InlineKeyboardButton(s,callback_data=f"claim_{s}") for s in CRYPTO_DATA.keys()]; kb=[keys[i:i+3] for i in range(0,len(keys),3)]
    await query.edit_message_text("✅ <b>Verified!</b>\n\nFinal step: Choose your account's cryptocurrency. <b>This choice is permanent.</b>",reply_markup=InlineKeyboardMarkup(kb),parse_mode=ParseMode.HTML)

# --- DASHBOARD & MAIN BUTTONS ---
async def show_main_menu(update, context, user=None):
    if not user: user=update.effective_user
    user_id = user.id
    user_data = get_user_state(user_id)
    if user_data:
        save_user_state(user_id, {'current_game': None, 'awaiting_confirmation': 0})
        
    bal,crp=user_data.get('balance',0.0),user_data.get('chosen_crypto','N/A')

    hourly_timer_text, hourly_bonus_val_text = "✨ Ready!", ""
    if crp != 'N/A' and crp in CRYPTO_DATA:
        hourly_bonus_val=HOURLY_BONUS_INR/CRYPTO_DATA[crp]['inr_rate']; hourly_bonus_val_text=f" (+{hourly_bonus_val:.6f} {crp})"
    if last_hourly:=user_data.get("last_hourly_claim"):
        try:
            if datetime.now() < (next_claim_time:=datetime.fromisoformat(last_hourly)+timedelta(hours=1)):
                wait=next_claim_time-datetime.now(); m,s=divmod(int(wait.total_seconds()),60); hourly_timer_text=f"⏳ {m}m {s}s"
        except (ValueError, TypeError): pass

    dash = (f"<b>👤 {user.first_name}'s Dashboard</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"💰 <b>Balance:</b> <code>{bal:.8f} {crp}</code>\n"
            f"💎 <b>Crypto:</b> <code>{crp}</code>\n\n"
            f"🕒 <b>Hourly Bonus:</b> {hourly_timer_text}<code>{hourly_bonus_val_text}</code>\n\n"
            f"What would you like to do next?")

    keys=[[InlineKeyboardButton("🎁 Bonuses",callback_data="bonuses_menu"),InlineKeyboardButton("🎮 Games", callback_data="games_menu")],
          [InlineKeyboardButton("📊 Profile",callback_data="my_profile"),InlineKeyboardButton("🔗 Referrals",callback_data="referrals")],
          [InlineKeyboardButton("❓ About Us", url="https://websappdev.lovestoblog.com/?i=1"), InlineKeyboardButton("👑 Contact Owner", url="t.me/Prajwalraut4230")],
          [InlineKeyboardButton("💸 Withdraw Funds",callback_data="withdraw_start")]]

    message = update.callback_query.message if update.callback_query else update.message
    try: await message.edit_text(dash,reply_markup=InlineKeyboardMarkup(keys),parse_mode=ParseMode.HTML)
    except (BadRequest, TelegramError):
         try:
            await context.bot.send_message(user.id,dash,reply_markup=InlineKeyboardMarkup(keys),parse_mode=ParseMode.HTML)
         except TelegramError: pass

async def play_real_games_handler(update, context):
    query = update.callback_query
    url = "https://telegram-mini-app-livid-iota.vercel.app/"
    text = (
        "To play our real games, please copy the link below and "
        "open it in your device's main browser (like Chrome, Safari, etc.).\n\n"
        f"<b>Link:</b> <code>{url}</code>"
    )
    keyboard = [[InlineKeyboardButton("⬅️ Back to Games", callback_data="games_menu")]]
    await query.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True
    )

# --- BUTTON HANDLER ---
async def button_handler(update, context):
    query=update.callback_query
    if not query.data: return
    await query.answer()

    data = query.data; parts = data.split("_")

    action_map = {
        "main_menu": lambda u, c: show_main_menu(u, c, query.from_user),
        "referrals": lambda u, c: referrals_command(u, c, from_button=True),
        "my_profile": lambda u, c: profile_command(u, c, from_button=True),
        "bonuses_menu": show_bonuses_menu,
        "games_menu": show_games_menu,
        "game_mining": mining_panel_entry,
        "game_aviator": aviator_panel_entry,
        "game_wingo": wingo_panel_entry,
        "play_real_games": play_real_games_handler,
    }

    if data in action_map:
        await action_map[data](update, context)
        return

    if data.startswith("claim_"): await claim_crypto(update, context)
    elif data in ["hourly_bonus","daily_bonus", "weekly_bonus"]: await claim_timed_bonus(update, context)
    elif data == "bonus_code_prompt": await query.message.reply_text("Please use the command `/bonus YOUR_CODE` to redeem a code.")
    elif parts[0] in ["aviator", "wingo", "mining"]: await handle_game_action(update, context)

async def claim_crypto(update, context):
    query = update.callback_query; user_id = query.from_user.id
    user_data = get_user_state(user_id)
    if user_data.get("chosen_crypto"): return
    sym = query.data.split("_")[-1]; rate = CRYPTO_DATA[sym]["inr_rate"]; amt = INITIAL_BONUS_INR / rate
    save_user_state(user_id, {'chosen_crypto': sym, 'balance': amt})
    await query.edit_message_text(f"🎉 <b>Success! Account Active!</b>\nYou received a bonus of <b>{amt:.8f} {sym}</b>.", parse_mode=ParseMode.HTML)
    await show_main_menu(update, context, query.from_user)

async def claim_timed_bonus(update, context):
    query = update.callback_query; user_id = query.from_user.id
    user_data = get_user_state(user_id)
    if not user_data.get("chosen_crypto"): return

    data = query.data
    btype, tdelta, binr = None, None, None

    if data == "hourly_bonus":
        btype = "hourly"
        tdelta = timedelta(hours=1)
        binr = HOURLY_BONUS_INR
    elif data == "daily_bonus":
        btype = "daily"
        tdelta = timedelta(days=1)
        binr = DAILY_BONUS_INR
    elif data == "weekly_bonus":
        btype = "weekly"
        tdelta = timedelta(weeks=1)
        binr = WEEKLY_BONUS_INR

    if btype is None: return

    lckey = f"last_{btype}_claim"
    lctime_str = user_data.get(lckey)
    lctime = datetime.fromisoformat(lctime_str) if lctime_str else None

    if lctime and datetime.now() < lctime + tdelta:
        wait = lctime + tdelta - datetime.now()
        d = wait.days
        h, r = divmod(int(wait.total_seconds()) % (24 * 3600), 3600)
        m, s = divmod(r, 60)
        wtxt = ""
        if d > 0: wtxt += f"{d}d "
        if h > 0: wtxt += f"{h}h "
        wtxt += f"{m}m"
        await query.answer(f"⏳ Wait {wtxt.strip()} for next {btype} bonus.", show_alert=True)
        return

    sym, rate = user_data["chosen_crypto"], CRYPTO_DATA[user_data["chosen_crypto"]]["inr_rate"]
    bonus = binr / rate
    await update_balance(user_id, bonus, f"{btype}_bonus")
    save_user_state(user_id, {lckey: datetime.now().isoformat()})
    await query.answer(f"✅ Success! {bonus:.8f} {sym} added!", show_alert=False)
    await show_main_menu(update, context, query.from_user)

async def handle_game_action(update, context):
    game_type = update.callback_query.data.split("_")[0]
    if game_type == "mining": await handle_mining_action(update, context)
    elif game_type == "aviator": await handle_aviator_action(update, context)
    elif game_type == "wingo": await handle_wingo_action(update, context)

# --- GAME PANELS & BACKGROUND LOOPS ---
async def show_bonuses_menu(update, context):
    query = update.callback_query
    keys = [[InlineKeyboardButton("💰 Hourly Bonus", callback_data="hourly_bonus"), InlineKeyboardButton("☀️ Daily Bonus", callback_data="daily_bonus")],
            [InlineKeyboardButton("🗓️ Weekly Bonus", callback_data="weekly_bonus")],
            [InlineKeyboardButton("🔑 Redeem Code", callback_data="bonus_code_prompt")],
            [InlineKeyboardButton("⬅️ Back to Dashboard", callback_data="main_menu")]]
    await query.edit_message_text("🎁 <b>Bonus Center</b>\n\nChoose a bonus to claim:", reply_markup=InlineKeyboardMarkup(keys), parse_mode=ParseMode.HTML)

async def show_games_menu(update, context):
    query = update.callback_query
    user_id = query.from_user.id
    save_user_state(user_id, {'current_game': None, 'awaiting_confirmation': 0})
    keys = [
        [InlineKeyboardButton("⛏️ Mining", callback_data="game_mining"), InlineKeyboardButton("✈️ Aviator", callback_data="game_aviator")],
        [InlineKeyboardButton("🟢 Wingo", callback_data="game_wingo"), InlineKeyboardButton("🎰 Lucky Draw", callback_data="game_luckydraw")],
        [InlineKeyboardButton("🔥 Play real games 🔥", callback_data="play_real_games")],
        [InlineKeyboardButton("⬅️ Back to Dashboard", callback_data="main_menu")]
    ]
    await query.edit_message_text("🎮 <b>Game Center</b>\n\nChoose a game to play:", reply_markup=InlineKeyboardMarkup(keys), parse_mode=ParseMode.HTML)

# --- MINING SECTION ---
async def mining_panel_entry(update, context):
    query = update.callback_query; user_id = query.from_user.id
    save_user_state(user_id, {'current_game': 'mining', 'awaiting_confirmation': 0, 'last_message_id': query.message.message_id})
    user_data = get_user_state(user_id)
    text, kbd = await get_mining_panel(user_id, user_data)
    if text: await query.message.edit_text(text, reply_markup=kbd, parse_mode=ParseMode.HTML)

async def get_mining_panel(user_id, user_data):
    if not user_data or not user_data.get('chosen_crypto'): return None, None
    bal, crp = user_data.get('balance', 0.0), user_data.get('chosen_crypto'); level = user_data.get('mining_level', 1); mining_info = MINING_LEVELS[level]
    speed_inr_per_hour = mining_info['speed_inr_per_day'] / 24; speed_crypto = speed_inr_per_hour / CRYPTO_DATA[crp]['inr_rate']
    mining_start_time = user_data.get("mining_start_time"); keys = []
    if mining_start_time:
        start_time = datetime.fromisoformat(mining_start_time); end_time = start_time + timedelta(hours=24)
        if datetime.now() < end_time:
            remaining = end_time - datetime.now(); h, r = divmod(int(remaining.total_seconds()), 3600); m, s = divmod(r, 60); mining_status_text = f"⏳ Mining... Time left: {h:02d}:{m:02d}:{s:02d}"
            if level < len(MINING_LEVELS): keys.append([InlineKeyboardButton("⚡️ Upgrade Speed", callback_data="mining_upgrade")])
        else:
            mining_status_text = "✅ Session complete!"; keys.append([InlineKeyboardButton("🚀 Start Mining", callback_data="mining_start")])
    else:
        mining_status_text = "💤 Idle."; keys.append([InlineKeyboardButton("🚀 Start Mining", callback_data="mining_start")])
    text = f"⛏️ <b>Mining Dashboard</b>\n\n<b>Balance:</b> <code>{bal:.8f} {crp}</code>\n<b>Status:</b> {mining_status_text}\n\n<b>Level:</b> {level}\n<b>Speed:</b> {speed_crypto:.8f} {crp}/hour\n\n"
    if level < len(MINING_LEVELS):
        next_level = MINING_LEVELS[level + 1]; cost_crypto = next_level["upgrade_cost_inr"] / CRYPTO_DATA[crp]['inr_rate'];
        next_speed_inr_hour = next_level['speed_inr_per_day'] / 24; next_speed = next_speed_inr_hour / CRYPTO_DATA[crp]['inr_rate']
        text += f"<b>Next Upgrade (Lvl {level+1}):</b>\n- Cost: {cost_crypto:.8f} {crp}\n- Speed: {next_speed:.8f} {crp}/hour"
    else: text += "You are at the maximum mining level!"
    keys.append([InlineKeyboardButton("⬅️ Back to Games", callback_data="games_menu")])
    return text, InlineKeyboardMarkup(keys)

async def handle_mining_action(update, context):
    query = update.callback_query; user_id = query.from_user.id;
    user_data = get_user_state(user_id); action = query.data.split("_")[1]
    
    if action == "start":
        save_user_state(user_id, {'awaiting_confirmation': 0})
        last_start_time_str = user_data.get("mining_start_time")
        if not last_start_time_str or datetime.now() > datetime.fromisoformat(last_start_time_str) + timedelta(hours=24):
            save_user_state(user_id, {'mining_start_time': datetime.now().isoformat()})
            await query.answer("⛏️ Mining has started!")
        else:
            await query.answer("Mining is already in progress.", show_alert=True)
    elif action == "upgrade":
        level = user_data.get("mining_level", 1)
        if level >= len(MINING_LEVELS):
            await query.answer("You are already at the maximum level!", show_alert=True)
            return
        save_user_state(user_id, {'awaiting_confirmation': 1})
        next_level = MINING_LEVELS[level + 1];
        cost_crypto = next_level["upgrade_cost_inr"] / CRYPTO_DATA[user_data["chosen_crypto"]]['inr_rate']
        keys = [[InlineKeyboardButton("✅ Yes, Upgrade", callback_data="mining_upgrade_confirm")], [InlineKeyboardButton("❌ No, Cancel", callback_data="game_mining")]];
        await query.message.edit_text(f"<b>Confirm Upgrade</b>\n\nUpgrade to Level {level+1} for <code>{cost_crypto:.8f} {user_data['chosen_crypto']}</code>?", reply_markup=InlineKeyboardMarkup(keys), parse_mode=ParseMode.HTML)
        return
    elif action == "upgrade_confirm":
        save_user_state(user_id, {'awaiting_confirmation': 0})
        level = user_data.get("mining_level", 1)
        next_level_info = MINING_LEVELS[level + 1]
        upgrade_cost_crypto = next_level_info["upgrade_cost_inr"] / CRYPTO_DATA[user_data["chosen_crypto"]]["inr_rate"]
        if user_data.get("balance", 0.0) >= upgrade_cost_crypto:
            await update_balance(user_id, -upgrade_cost_crypto, f"upgrade_mining_level_{level+1}")
            save_user_state(user_id, {'mining_level': level + 1})
            await query.answer(f"✅ Upgraded to Level {level+1} successfully!", show_alert=True)
        else:
            await query.answer("❌ Not enough balance to upgrade.", show_alert=True)

    user_data = get_user_state(user_id) # Refresh data from cache
    text, kbd = await get_mining_panel(user_id, user_data)
    if text: await query.message.edit_text(text, reply_markup=kbd, parse_mode=ParseMode.HTML)

async def mining_loop(app: Application):
    while True:
        await asyncio.sleep(60)
        now = datetime.now()
        # Iterate over the cached user states, which is much faster than querying the DB
        active_miners = [
            (uid, udata) for uid, udata in user_states.items() 
            if udata.get('mining_start_time') and udata.get('chosen_crypto')
        ]
        if not active_miners: continue

        for user_id, user_data in active_miners:
            try:
                start_time = datetime.fromisoformat(user_data["mining_start_time"])
                if now >= start_time + timedelta(hours=24):
                    save_user_state(user_id, {"mining_start_time": None})
                else:
                    level = user_data.get("mining_level", 1)
                    speed_inr_per_day = MINING_LEVELS[level]['speed_inr_per_day']
                    rate = CRYPTO_DATA[user_data['chosen_crypto']]['inr_rate']
                    earnings_per_minute = (speed_inr_per_day / rate) / (24 * 60)
                    await update_balance(user_id, earnings_per_minute, "mining_reward")
            except Exception as e:
                logger.error(f"Error processing mining for user {user_id}: {e}")
                continue

# --- AVIATOR SECTION ---
async def aviator_panel_entry(update, context):
    query = update.callback_query; user_id = query.from_user.id
    save_user_state(user_id, {'current_game': 'aviator', 'awaiting_confirmation': 0, 'last_message_id': query.message.message_id})
    user_data = get_user_state(user_id)
    text, kbd = await get_aviator_panel(user_id, user_data)
    if text: await query.message.edit_text(text, reply_markup=kbd, parse_mode=ParseMode.MARKDOWN)

async def get_aviator_panel(user_id, user_data):
    user_id = int(user_id)
    balance,crypto,bet_inr = user_data.get('balance',0),user_data.get('chosen_crypto','N/A'),user_data.get("aviator_bet",10); last_crashes_str=" ".join([f"`{m:.2f}x`" for m in aviator_state.last_crashes])
    text=f"✈️ **AVIATOR**\n💰 Balance: `{balance:.8f} {crypto}`\n📊 History: {last_crashes_str}\n" + "─"*20 + "\n\n"; keyboard=[]
    if aviator_state.state=="WAITING":
        text+=f"✈️ **STARTING IN: `{aviator_state.countdown}s`**"
        if user_id in aviator_state.bets: text+=f"\n\n✅ **Bet Placed:** `{aviator_state.bets[user_id]['amount']:.8f} {crypto}`"; keyboard.append([InlineKeyboardButton("❌ Cancel Bet",callback_data="aviator_cancel")])
        else: keyboard.append([InlineKeyboardButton("➖",callback_data="aviator_bet_decrease"), InlineKeyboardButton(f"Bet {bet_inr} INR",callback_data=f"aviator_bet_place"), InlineKeyboardButton("➕",callback_data="aviator_bet_increase")])
    elif aviator_state.state=="IN_PROGRESS":
        text+=f"# 🚀 ` {aviator_state.current_multiplier:.2f}x ` 🚀\n"
        if user_id in aviator_state.bets:
            bet_info=aviator_state.bets[user_id]
            if not bet_info["cashed_out"]:
                winnings=bet_info["amount"]*aviator_state.current_multiplier; text+=f"\n*Your Bet:* `{bet_info['amount']:.8f} {crypto}`\n*Current Win:* `{winnings:.8f} {crypto}`"; keyboard.append([InlineKeyboardButton(f"💸 CASHOUT",callback_data="aviator_cashout")])
            else: text+=f"🎉 **Cashed out!**\nYou won `{bet_info['amount']*bet_info['cashout_multiplier']:.8f} {crypto}` at `{bet_info['cashout_multiplier']:.2f}x`."
    elif aviator_state.state=="CRASHED": text+=f"💥 **CRASHED @ {aviator_state.last_crashes[0]:.2f}x** 💥"
    keyboard.append([InlineKeyboardButton("⬅️ Back to Games",callback_data="games_menu")]); return text, InlineKeyboardMarkup(keyboard)

async def handle_aviator_action(update, context):
    query = update.callback_query; user_id = query.from_user.id;
    user_data = get_user_state(user_id); action = query.data.split("_")[-1]
    bet_inr = user_data.get("aviator_bet", 10)
    
    if action == "increase": save_user_state(user_id, {'aviator_bet': bet_inr + 10})
    elif action == "decrease": save_user_state(user_id, {'aviator_bet': max(10, bet_inr - 10)})
    elif action == "place":
        if aviator_state.state == "WAITING" and user_id not in aviator_state.bets:
            rate = CRYPTO_DATA[user_data["chosen_crypto"]]["inr_rate"]; bet_crypto = float(bet_inr) / rate
            if user_data.get('balance', 0.0) >= bet_crypto:
                if await update_balance(user_id, -bet_crypto, "aviator_bet"):
                    aviator_state.bets[user_id] = {"amount": bet_crypto, "cashed_out": False}; await query.answer("✅ Bet placed!")
            else: await query.answer("❌ Insufficient balance!", show_alert=True)
        else: await query.answer("❌ You can only bet while waiting for the next round.", show_alert=True)
    elif action == "cancel":
        if aviator_state.state == "WAITING" and user_id in aviator_state.bets:
            await update_balance(user_id, aviator_state.bets[user_id]['amount'], "aviator_cancel"); del aviator_state.bets[user_id]; await query.answer("✅ Bet cancelled!")
    elif action == "cashout":
        if aviator_state.state == "IN_PROGRESS" and user_id in aviator_state.bets and not aviator_state.bets[user_id]["cashed_out"]:
            bet_info = aviator_state.bets[user_id]; winnings = bet_info["amount"] * aviator_state.current_multiplier
            await update_balance(user_id, winnings, "aviator_win"); bet_info["cashed_out"] = True; bet_info["cashout_multiplier"] = aviator_state.current_multiplier; await query.answer(f"💸 Cashed out at {aviator_state.current_multiplier:.2f}x!")


async def aviator_game_loop(app):
    while True:
        try:
            aviator_state.state="WAITING"
            aviator_state.bets.clear()
            for i in range(5, 0, -1):
                aviator_state.countdown = i
                await asyncio.sleep(1)

            aviator_state.state="IN_PROGRESS"
            aviator_state.crash_multiplier = random.uniform(1.0, 1.99) if random.random() < 0.4 else random.uniform(2.0, 35.0)
            aviator_state.current_multiplier = 1.00

            while aviator_state.current_multiplier < aviator_state.crash_multiplier:
                increment = 0.01 + (aviator_state.current_multiplier / 100)
                aviator_state.current_multiplier += increment
                await asyncio.sleep(0.05)

            aviator_state.state="CRASHED"
            crashed_at=max(1.0, aviator_state.crash_multiplier)
            aviator_state.last_crashes.insert(0, crashed_at)
            aviator_state.last_crashes=aviator_state.last_crashes[:5]

            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"Aviator loop crashed: {e}", exc_info=True)
            await asyncio.sleep(5)

# --- WINGO SECTION ---
async def wingo_panel_entry(update, context):
    query = update.callback_query; user_id = query.from_user.id
    save_user_state(user_id, {'current_game': 'wingo', 'awaiting_confirmation': 0, 'last_message_id': query.message.message_id})
    user_data = get_user_state(user_id)
    text, kbd = await get_wingo_panel(user_id, user_data)
    if text: await query.message.edit_text(text, reply_markup=kbd, parse_mode=ParseMode.MARKDOWN)

async def get_wingo_panel(user_id_str, user_data):
    if not user_data: return None, None
    balance,crypto,bet_inr=user_data.get('balance',0),user_data.get('chosen_crypto','N/A'),user_data.get("wingo_bet",10); active_mode=user_data.get("wingo_mode","1min")
    text=f"🟢 **WINGO**\n💰 Balance: `{balance:.8f} {crypto}`\n\n"; keyboard=[[InlineKeyboardButton(f"{'▶️ ' if active_mode==m else ''}{m.replace('m',' Min')}",callback_data=f"wingo_mode_{m}") for m in ['1min','3min','5min']]]
    timer_val=wingo_state.timers.get(active_mode,60); mins,secs=divmod(timer_val,60); period_id=wingo_state.period_ids.get(active_mode,'N/A')

    history_text = ""
    for r in wingo_state.history[:10]:
        if r['mode'] == active_mode:
            color_emoji = "🟩" if "green" in r['color'] else "🟥" if "red" in r['color'] else "🟪"
            if "violet" in r['color'] and color_emoji != "🟪": color_emoji += "🟪"
            size_emoji = "🔼" if r['is_big'] else "🔽"
            history_text += f"`{r['number']}`{color_emoji}{size_emoji} "
    text += f"📊 History: {history_text or 'No history yet.'}\n"
    text += "─"*20 + "\n\n"

    text+=f"Mode: **Win Go {active_mode}** | Period: `{period_id}`\nTime Left: `{mins:02d}:{secs:02d}`\n\nChoose your bet:"
    keyboard.extend([[InlineKeyboardButton("🟩",callback_data="wingo_bet_color_green"), InlineKeyboardButton("🟪",callback_data="wingo_bet_color_violet"), InlineKeyboardButton("🟥",callback_data="wingo_bet_color_red")], [InlineKeyboardButton("🔼 Big",callback_data="wingo_bet_size_big"), InlineKeyboardButton("🔽 Small",callback_data="wingo_bet_size_small")], [InlineKeyboardButton(f"{i}",callback_data=f"wingo_bet_number_{i}") for i in range(5)], [InlineKeyboardButton(f"{i}",callback_data=f"wingo_bet_number_{i}") for i in range(5,10)], [InlineKeyboardButton("➖",callback_data="wingo_bet_decrease"), InlineKeyboardButton(f"Bet: {bet_inr} INR",callback_data="wingo_noop"), InlineKeyboardButton("➕",callback_data="wingo_bet_increase")]])
    my_bets=wingo_state.bets.get(str(user_id_str),{}).get(period_id,[]); text+="\n*Your Bets for this round:*\n";
    if my_bets:
        for b in my_bets: text+=f"- `{b['amount_inr']} INR` on `{b['value'].replace('_', ' ').title()}`\n"
    else: text += "No bets placed yet.\n"
    keyboard.append([InlineKeyboardButton("⬅️ Back to Games",callback_data="games_menu")]); return text,InlineKeyboardMarkup(keyboard)

async def handle_wingo_action(update, context):
    query = update.callback_query; user_id = query.from_user.id
    user_data = get_user_state(user_id); parts = query.data.split("_")
    action, sub_action = parts[1], parts[2] if len(parts) > 2 else None

    if action == "mode": save_user_state(user_id, {'wingo_mode': sub_action})
    elif action == "bet":
        bet_inr = user_data.get("wingo_bet", 10)
        if sub_action == "increase": save_user_state(user_id, {'wingo_bet': bet_inr + 10})
        elif sub_action == "decrease": save_user_state(user_id, {'wingo_bet': max(10, bet_inr - 10)})
        else:
            save_user_state(user_id, {'awaiting_confirmation': 1})
            bet_type, bet_value = sub_action, parts[3];
            text = f"Confirm bet of **{bet_inr} INR** on **{bet_value.title()}**?"
            kbd = [[InlineKeyboardButton("✅ Confirm", callback_data=f"wingo_confirm_{bet_type}_{bet_value}"), InlineKeyboardButton("❌ Cancel", callback_data="game_wingo")]]
            try: await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(kbd), parse_mode=ParseMode.MARKDOWN)
            except (BadRequest, TelegramError): pass
            return
    elif action == "confirm":
        save_user_state(user_id, {'awaiting_confirmation': 0})
        active_mode = user_data.get("wingo_mode", "1min")
        if wingo_state.timers.get(active_mode, 0) <= 10:
            await query.answer("❌ Too late to bet for this round.", show_alert=True)
        else:
            bet_type, bet_value = sub_action, parts[3]; bet_inr = user_data.get("wingo_bet", 10)
            rate = CRYPTO_DATA[user_data["chosen_crypto"]]["inr_rate"]; bet_crypto = float(bet_inr) / rate
            if user_data.get('balance', 0.0) >= bet_crypto:
                period_id = wingo_state.period_ids.get(active_mode)
                if str(user_id) not in wingo_state.bets: wingo_state.bets[str(user_id)] = {}
                if period_id not in wingo_state.bets[str(user_id)]: wingo_state.bets[str(user_id)][period_id] = []
                if await update_balance(user_id, -bet_crypto, "wingo_bet"):
                    wingo_state.bets[str(user_id)][period_id].append({"type": bet_type, "value": bet_value, "amount_inr": bet_inr, "amount_crypto": bet_crypto})
                    await query.answer("✅ Bet placed!")
            else: await query.answer("❌ Insufficient balance!", show_alert=True)

    user_data = get_user_state(user_id) # Refresh data
    text, kbd = await get_wingo_panel(user_id, user_data)
    if text:
        try: await query.message.edit_text(text, reply_markup=kbd, parse_mode=ParseMode.MARKDOWN)
        except (BadRequest, TelegramError): pass

async def wingo_game_loop(app):
    while True:
        try:
            now = datetime.now()
            for mode, timer_val in list(wingo_state.timers.items()):
                if timer_val <= 0:
                    try:
                        period_id = wingo_state.period_ids[mode]
                        number = random.randint(0, 9)
                        is_big = number >= 5
                        color = "red" if number % 2 == 0 else "green"
                        if number in [0, 5]: color += ",violet"
                        result = {"period": period_id, "mode": mode, "number": number, "is_big": is_big, "color": color}
                        wingo_state.history.insert(0, result)
                        wingo_state.history = wingo_state.history[:20]

                        for user_id_str, user_bets_by_period in list(wingo_state.bets.items()):
                            if period_id in user_bets_by_period:
                                try:
                                    total_win_crypto = 0
                                    for bet in user_bets_by_period[period_id]:
                                        win_crypto = 0
                                        if bet['type'] == 'number' and bet['value'] == str(result['number']):
                                            win_crypto = bet['amount_crypto'] * 9
                                        elif bet['type'] == 'color':
                                            if bet['value'] in result['color']:
                                                rate = 4.5 if bet['value'] == 'violet' else 1.5 if 'violet' in result['color'] and bet['value'] != 'violet' else 2
                                                win_crypto = bet['amount_crypto'] * rate
                                        elif bet['type'] == 'size' and bet['value'] == ('big' if result['is_big'] else 'small'):
                                            win_crypto = bet['amount_crypto'] * 2
                                        if win_crypto > 0:
                                            total_win_crypto += win_crypto
                                    if total_win_crypto > 0:
                                        await update_balance(int(user_id_str), total_win_crypto, f"wingo_win_{mode}")
                                    if user_id_str in wingo_state.bets and period_id in wingo_state.bets[user_id_str]:
                                        del wingo_state.bets[user_id_str][period_id]
                                except Exception as e:
                                    logger.error(f"Error processing Wingo payouts for user {user_id_str}: {e}")
                                    continue

                        wingo_state.timers[mode] = {'1min': 60, '3min': 180, '5min': 300}[mode]
                        wingo_state.period_ids[mode] = f"{now.strftime('%Y%m%d%H%M%S')}_{mode}"
                    except Exception as e:
                        logger.error(f"Error processing Wingo result for mode {mode}: {e}")
                else:
                    wingo_state.timers[mode] -= 1
        except Exception as e:
            logger.error(f"Wingo main loop error: {e}", exc_info=True)
        await asyncio.sleep(1)

# --- LIVE UI UPDATE LOOP ---
async def game_ui_update_loop(app: Application):
    while True:
        await asyncio.sleep(1)
        # Performance: Iterate over cache instead of DB
        active_users = {
            uid: udata for uid, udata in user_states.items()
            if udata.get("current_game") and udata.get("last_message_id") and not udata.get("awaiting_confirmation")
        }
        if not active_users:
            continue

        async def update_user_ui(uid, udata):
            try:
                game = udata["current_game"]; msg_id = udata["last_message_id"]
                text, kbd = None, None

                if game == "aviator": text, kbd = await get_aviator_panel(uid, udata)
                elif game == "wingo": text, kbd = await get_wingo_panel(uid, udata)
                elif game == "mining": text, kbd = await get_mining_panel(uid, udata)

                if text and kbd:
                    parse_mode = ParseMode.HTML if game == "mining" else ParseMode.MARKDOWN
                    await app.bot.edit_message_text(text, chat_id=uid, message_id=msg_id, reply_markup=kbd, parse_mode=parse_mode)
            except BadRequest as e:
                if "Message is not modified" not in str(e):
                    logger.warning(f"UI Update BadRequest for user {uid}: {e}")
                    save_user_state(uid, {'current_game': None}) # Stop updates
            except TelegramError as e:
                logger.warning(f"UI Update TelegramError for user {uid}: {e}")
                save_user_state(uid, {'current_game': None}) # Stop updates
            except Exception as e:
                logger.error(f"Critical error updating UI for user {uid}: {e}", exc_info=True)
                save_user_state(uid, {'current_game': None})

        tasks = [update_user_ui(uid, udata) for uid, udata in active_users.items()]
        if tasks: await asyncio.gather(*tasks, return_exceptions=True)

# --- NOTIFICATION & OTHER HANDLERS ---
async def send_game_result_notification(game_name, result_data):
    if not GAME_RESULT_BOT_TOKEN or not GAME_RESULT_GROUP_CHAT_ID:
        logger.warning("Game result notification skipped: Token or Chat ID is not set.")
        return
    try:
        notif_bot = Bot(token=GAME_RESULT_BOT_TOKEN)
        text = (f"🥳 *Lucky Draw Winner!* 🥳\n\n"
                f"Congratulations to *{result_data['user_name']}*!\n\n"
                f"💰 Bet: `{result_data['bet_amount']:.8f} {result_data['crypto']}`\n"
                f"🎉 Won: `{result_data['win_amount']:.8f} {result_data['crypto']}`")

        await notif_bot.send_message(GAME_RESULT_GROUP_CHAT_ID, text, parse_mode=ParseMode.MARKDOWN)
        logger.info(f"Sent {game_name} result notification to group {GAME_RESULT_GROUP_CHAT_ID}.")
    except Exception as e:
        logger.error(f"Could not send game result notification! Error: {e}", exc_info=True)

# --- LUCKY DRAW, PROFILE, REFERRALS, ETC. ---
async def lucky_draw_start(update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query; await query.answer(); user_id = query.from_user.id
    user_data = get_user_state(user_id)
    if not user_data.get("chosen_crypto"): await query.edit_message_text("Please set your crypto via /start first.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Games", callback_data="games_menu")]])); return ConversationHandler.END
    crypto = user_data["chosen_crypto"]; balance = user_data.get("balance", 0.0)
    text = (f"🎰 **Lucky Draw**\n\nYour Balance: `{balance:.8f} {crypto}`\n\nSend the amount of `{crypto}` you want to bet.\nWin Chance: {LUCKY_DRAW_CONFIG['win_chance']*100}%\nWin Payout: {LUCKY_DRAW_CONFIG['win_multiplier']}x\n\nSend /cancel to exit.")
    await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN); return AWAIT_LUCKY_DRAW_BET

async def lucky_draw_bet(update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user; user_id = user.id
    user_data = get_user_state(user_id); crypto = user_data["chosen_crypto"]
    try:
        bet_amount = float(update.message.text)
        if not (0 < bet_amount <= user_data.get("balance", 0.0)): raise ValueError
    except (ValueError, TypeError): await update.message.reply_text("❌ Invalid or insufficient amount. Please send a valid number that is within your balance."); return AWAIT_LUCKY_DRAW_BET
    context.user_data["lucky_draw_bet"] = bet_amount
    keyboard = [[InlineKeyboardButton("✅ Confirm Bet", callback_data="confirm_bet"), InlineKeyboardButton("❌ Cancel", callback_data="cancel_draw")]]; await update.message.reply_text(f"You are about to bet `{bet_amount:.8f} {crypto}`. Are you sure?", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN); return CONFIRM_LUCKY_DRAW

async def lucky_draw_confirm(update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query; await query.answer(); user = query.from_user; user_id = user.id
    user_data = get_user_state(user_id); bet_amount = context.user_data.get("lucky_draw_bet")
    if not bet_amount: await query.edit_message_text("Error: Bet amount not found. Please start again.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="games_menu")]])); return ConversationHandler.END

    is_win = random.random() < LUCKY_DRAW_CONFIG["win_chance"]

    if is_win:
        win_amount = bet_amount * (LUCKY_DRAW_CONFIG["win_multiplier"])
        await update_balance(user_id, win_amount - bet_amount, "lucky_draw_win")
        result_text = f"🥳 YOU WON! 🥳\n\nYou won `{win_amount:.8f} {user_data['chosen_crypto']}`!"
        notification_data = {'user_name': user.first_name, 'user_id': user.id, 'bet_amount': bet_amount, 'win_amount': win_amount, 'crypto': user_data['chosen_crypto']}
        context.application.create_task(send_game_result_notification("luckydraw", notification_data))
    else:
        await update_balance(user_id, -bet_amount, "lucky_draw_loss")
        result_text = f"😭 YOU LOST! 😭\n\nYou lost `{bet_amount:.8f} {user_data['chosen_crypto']}`."

    await query.edit_message_text(result_text, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Games", callback_data="games_menu")]]))
    context.user_data.clear();
    return ConversationHandler.END

async def cancel_draw(update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.callback_query:
        query = update.callback_query; await query.answer(); await query.edit_message_text("🎰 Lucky Draw cancelled.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Games", callback_data="games_menu")]]))
    else: await update.message.reply_text("🎰 Lucky Draw cancelled.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Games", callback_data="games_menu")]]))
    context.user_data.clear(); return ConversationHandler.END

async def profile_command(update, context, from_button=False):
    user_id=update.effective_user.id; user_data=get_user_state(user_id)
    join_date_str = "N/A"
    if jd:=user_data.get("join_date"):
        try: join_date_str = datetime.fromisoformat(jd).strftime('%d %b, %Y')
        except (ValueError, TypeError): pass
    text=(f"📊 <b>My Profile</b>\n\n👤 <b>User ID:</b> <code>{user_id}</code>\n📆 <b>Join Date:</b> {join_date_str}\n💎 <b>Account Crypto:</b> {user_data.get('chosen_crypto','N/A')}\n💰 <b>Balance:</b> {user_data.get('balance',0.0):.8f}\n👥 <b>Total Referrals:</b> {user_data.get('referrals',0)}")
    reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Dashboard", callback_data="main_menu")]])
    message = update.callback_query.message if from_button else update.message
    try:
        if from_button: await message.edit_text(text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
        else: await message.reply_text(text,parse_mode=ParseMode.HTML, reply_markup=reply_markup)
    except (BadRequest, TelegramError): pass

async def referrals_command(update, context, from_button=False):
    user_id=update.effective_user.id; user_data=get_user_state(user_id)
    sym=user_data.get("chosen_crypto","your chosen crypto"); bonus_per_ref=0
    if sym in CRYPTO_DATA: bonus_per_ref=REFERRAL_BONUS_INR/CRYPTO_DATA[sym]["inr_rate"]
    link=f"https://t.me/{BOT_USERNAME}?start={user_id}"
    text=(f"🔗 <b>Your Referral Link</b>\n\nShare this link to earn big bonuses! You'll get ~<b>{bonus_per_ref:.8f} {sym}</b> for each new referral.\n\n<code>{link}</code>\n\n👥 <b>Your Referrals:</b> {user_data.get('referrals',0)} / {MIN_REFERRALS_FOR_WITHDRAWAL} required to withdraw.")
    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Dashboard",callback_data="main_menu")]])
    message = update.callback_query.message if from_button else update.message
    if from_button: await message.edit_text(text,reply_markup=reply_markup,parse_mode=ParseMode.HTML)
    else: await message.reply_text(text,reply_markup=reply_markup,parse_mode=ParseMode.HTML)

async def bonus_code_handler(update, context):
    user_id = update.effective_user.id
    user_data = get_user_state(user_id)
    if not user_data or not user_data.get('is_verified'):
        await update.message.reply_text("Please complete verification first via /start.")
        return
    if not user_data.get('chosen_crypto'):
        await update.message.reply_text("You must choose your main crypto before redeeming a bonus code.")
        return
    if not context.args:
        await update.message.reply_text("Usage: `/bonus YOUR_CODE`")
        return

    code = context.args[0].upper()
    used_codes = json.loads(user_data.get('used_codes', '[]'))

    if code in used_codes:
        await update.message.reply_text("❌ This code has already been used.")
        return

    bonus_code_data = get_db_value("SELECT inr_value FROM bonus_codes WHERE code = ?", (code,), fetchone=True)

    if bonus_code_data:
        bonus_inr = bonus_code_data['inr_value']
        sym = user_data["chosen_crypto"]
        rate = CRYPTO_DATA[sym]["inr_rate"]
        bonus_crypto = bonus_inr / rate
        await update_balance(user_id, bonus_crypto, f"bonus_code_{code}")
        used_codes.append(code)
        save_user_state(user_id, {'used_codes': json.dumps(used_codes)})
        await update.message.reply_text(f"🎉 Success! You've redeemed a bonus of <b>{bonus_crypto:.8f} {sym}</b>.", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text("❌ This code is invalid.")

async def stats_cmd(upd,ctx):
    user_count = len(user_states)
    await upd.message.reply_text(f"📊 <b>Bot Statistics</b>\n\n👥 Total Users: {user_count}",parse_mode=ParseMode.HTML)

async def top_ref_cmd(upd,ctx):
    sorted_users = sorted(user_states.values(), key=lambda x: x.get('referrals', 0), reverse=True)[:5]
    text = "🏆 <b>Top 5 Referrers</b> 🏆\n\n"
    if not sorted_users or all(u.get('referrals', 0) == 0 for u in sorted_users):
        text += "No referrals yet."
    else:
        for i, udata in enumerate(sorted_users):
            if udata.get('referrals', 0) > 0:
                try:
                    name = (await ctx.bot.get_chat(udata['user_id'])).first_name
                except:
                    name = "User"
                text += f"🏅 Top {i+1}: {name} - <b>{udata.get('referrals',0)} referrals</b>\n"
    await upd.message.reply_text(text, parse_mode=ParseMode.HTML)

async def help_cmd(upd,ctx): await upd.message.reply_text("ℹ️ <b>How It Works</b>\n\n1. Join channels & verify.\n2. Choose permanent crypto.\n3. Earn via bonuses, referrals & games.\n4. Withdraw when you have 15 referrals and enough balance.",parse_mode=ParseMode.HTML)

async def support_cmd(upd,ctx): await upd.message.reply_text(f"For help, contact support: {SUPPORT_CONTACT}",parse_mode=ParseMode.HTML)

async def send_withdrawal_notification(user,udata,address):
    try:
        n_bot=Bot(token=WITHDRAWAL_BOT_TOKEN); text=(f"🔥 <b>New Withdrawal Request</b> 🔥\n\n━━━━━━━━━━━━━━━━━━\n<b>User:</b> {user.first_name} (<code>{user.id}</code>)\n<a href='tg://user?id={user.id}'>Profile Link</a>\n\n<b>Details:</b>\n • <b>Amount:</b> <code>{udata['balance']:.8f} {udata['chosen_crypto']}</code>\n • <b>Wallet:</b>\n<code>{address}</code>\n\n<b>Timestamp:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"); await n_bot.send_message(WITHDRAWAL_GROUP_CHAT_ID,text,parse_mode=ParseMode.HTML)
    except Exception as e: logger.error(f"Could not send withdrawal notification! Error: {e}")

async def withdraw_start(upd,ctx):
    q=upd.callback_query; await q.answer(); uid=q.from_user.id; udata=get_user_state(uid); sym=udata.get("chosen_crypto")
    if not sym: await q.edit_message_text("Error: Account not set up. /start"); return ConversationHandler.END
    if udata.get('referrals', 0) < MIN_REFERRALS_FOR_WITHDRAWAL: await q.edit_message_text(f"⚠️ <b>Withdrawal Locked!</b>\n\nYou need at least <b>{MIN_REFERRALS_FOR_WITHDRAWAL} referrals</b> to withdraw. You have {udata.get('referrals', 0)}.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back",callback_data="main_menu")]]), parse_mode=ParseMode.HTML); return ConversationHandler.END
    bal,rate=udata.get("balance",0.0),CRYPTO_DATA[sym]["inr_rate"]; min_w=MIN_WITHDRAWAL_INR/rate
    if bal<min_w: await q.edit_message_text(f"⚠️ <b>Insufficient Balance!</b>\n\nMinimum: {min_w:.8f} {sym}\nYou have: {bal:.8f} {sym}",reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back",callback_data="main_menu")]]),parse_mode=ParseMode.HTML); return ConversationHandler.END
    await q.edit_message_text(f"💸 Withdrawing <b>{bal:.8f} {sym}</b>.\n\nPlease send your <b>{CRYPTO_DATA[sym]['name']} ({sym})</b> wallet address.\n\nSend /cancel to exit.",parse_mode=ParseMode.HTML); return AWAIT_ADDRESS

async def withdraw_address(upd,ctx):
    addr,uid=upd.message.text,upd.effective_user.id; udata=get_user_state(uid); sym=udata["chosen_crypto"]
    if not re.match(CRYPTO_DATA[sym]["regex"],addr): await upd.message.reply_text(f"⚠️ Invalid address for {sym}. Please resend.",parse_mode=ParseMode.HTML); return AWAIT_ADDRESS
    ctx.user_data['withdraw_address']=addr; bal=udata.get("balance",0.0); await upd.message.reply_text(f"<b>Final Confirmation</b>\n\nAmount: {bal:.8f} {sym}\nTo: <code>{addr}</code>\n\n⚠️ Irreversible. Proceed?",reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✅ Yes, I'm Sure",callback_data="confirm_final")],[InlineKeyboardButton("❌ No, Cancel",callback_data="cancel_w")]]),parse_mode=ParseMode.HTML); return CONFIRM_WITHDRAWAL

async def withdraw_confirm(upd,ctx):
    q=upd.callback_query; await q.answer(); uid=q.from_user.id; udata=get_user_state(uid); address=ctx.user_data.get('withdraw_address')
    if not address: return ConversationHandler.END
    await send_withdrawal_notification(q.from_user,udata,address);
    save_user_state(uid, {"balance": 0.0})
    await q.edit_message_text("✅ <b>Withdrawal Processed!</b> Your request is submitted.",parse_mode=ParseMode.HTML); await show_main_menu(upd,ctx,q.from_user); return ConversationHandler.END

async def cancel_w(upd,ctx): q=upd.callback_query; await q.answer(); await q.edit_message_text("Withdrawal cancelled."); await show_main_menu(upd,ctx,q.from_user); return ConversationHandler.END

async def cancel_w_cmd(update, context): await update.message.reply_text("Withdrawal cancelled."); await show_main_menu(update, context, update.effective_user); return ConversationHandler.END

# --- ADMIN COMMANDS ---
async def panel_command(upd,ctx):
    if upd.effective_user.id != OWNER_ID: return
    await upd.message.reply_text("Advanced Admin Panel features are under development.")

async def admin_command(upd,ctx):
    if upd.effective_user.id != OWNER_ID: return
    if not ctx.args: await upd.message.reply_text("Usage: /admin <user_id>"); return
    target_id=int(ctx.args[0])
    if get_user_state(target_id):
        save_user_state(target_id, {'is_admin': 1})
        await upd.message.reply_text(f"✅ User {target_id} is now an admin.")
    else: await upd.message.reply_text("❌ User not found.")

async def add_balance_command(upd,ctx):
    user_id=upd.effective_user.id
    user_data=get_user_state(user_id)
    if not (user_data and user_data.get('is_admin')) and user_id != OWNER_ID: return
    if len(ctx.args)!=2: await upd.message.reply_text("Usage: /addbalance <user_id> <inr_value>"); return
    target_id_str,inr_value=ctx.args[0],ctx.args[1]
    target_id = int(target_id_str)
    target_data = get_user_state(target_id)
    if not target_data: await upd.message.reply_text("❌ Target user not found."); return
    if not target_data.get('chosen_crypto'): await upd.message.reply_text("❌ Target user has not chosen a crypto yet."); return
    try:
        amt_inr=float(inr_value); sym=target_data['chosen_crypto']; rate=CRYPTO_DATA[sym]['inr_rate']; amt_crypto=amt_inr/rate
        await update_balance(target_id, amt_crypto, f"admin_add_{user_id}")
        await upd.message.reply_text(f"✅ Successfully added {amt_crypto:.8f} {sym} to user {target_id}.")
        await ctx.bot.send_message(target_id, f"🎁 An admin has added <b>{amt_crypto:.8f} {sym}</b> to your balance!", parse_mode=ParseMode.HTML)
    except (ValueError, TelegramError): await upd.message.reply_text("❌ Invalid INR amount or failed to send message.")

def main() -> None:
    setup_database() # Initialize the database on startup
    app = Application.builder().token(BOT_TOKEN).build()
    app.post_init = post_init_callback

    withdraw_handler=ConversationHandler(entry_points=[CallbackQueryHandler(withdraw_start,pattern="^withdraw_start$")],states={AWAIT_ADDRESS:[MessageHandler(filters.TEXT & ~filters.COMMAND, withdraw_address)],CONFIRM_WITHDRAWAL:[CallbackQueryHandler(withdraw_confirm,pattern="^confirm_final$")]},fallbacks=[CallbackQueryHandler(cancel_w, pattern="^cancel_w$"),CommandHandler("cancel", cancel_w_cmd)], per_message=False)
    lucky_draw_handler = ConversationHandler(entry_points=[CallbackQueryHandler(lucky_draw_start, pattern="^game_luckydraw$")],states={AWAIT_LUCKY_DRAW_BET: [MessageHandler(filters.TEXT & ~filters.COMMAND, lucky_draw_bet)],CONFIRM_LUCKY_DRAW: [CallbackQueryHandler(lucky_draw_confirm, pattern="^confirm_bet$")]},fallbacks=[CallbackQueryHandler(cancel_draw, pattern="^cancel_draw$"),CommandHandler("cancel", cancel_draw)],per_message=False)

    app.add_handler(CommandHandler("start", start)); app.add_handler(CommandHandler("profile", profile_command)); app.add_handler(CommandHandler("bonus", bonus_code_handler));
    app.add_handler(CommandHandler("stats", stats_cmd)); app.add_handler(CommandHandler("top", top_ref_cmd)); app.add_handler(CommandHandler("help", help_cmd)); app.add_handler(CommandHandler("support", support_cmd))
    app.add_handler(CommandHandler("panel", panel_command)); app.add_handler(CommandHandler("admin", admin_command)); app.add_handler(CommandHandler("addbalance", add_balance_command))

    app.add_handler(withdraw_handler); app.add_handler(lucky_draw_handler)
    app.add_handler(CallbackQueryHandler(verify_subscription_handler, pattern="^verify_subscription$"))
    app.add_handler(CallbackQueryHandler(button_handler))
    
    # Add global error handler for stability
    app.add_error_handler(error_handler)

    print("Bot is running with UPGRADED performance and stability features...")
    app.run_polling()

if __name__ == "__main__":
    main()