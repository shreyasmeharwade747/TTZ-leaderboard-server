import MetaTrader5 as mt5
import json
import logging
from datetime import datetime, timedelta
import time
import threading
import traceback
import psycopg2
from psycopg2.extras import RealDictCursor
import psycopg2.pool
from decimal import Decimal

# Database configuration
DB_CONFIG = {
    'host': 'aws-0-ap-south-1.pooler.supabase.com',
    'port': 5432,
    'user': 'postgres.ldonzqwaxlugziqtqedy',
    'password': 'Shreyas@ttz',
    'dbname': 'postgres',
    'sslmode': 'require',
    'connect_timeout': 10,  # Add timeout
    'keepalives': 1,
    'keepalives_idle': 30,
    'keepalives_interval': 10,
    'keepalives_count': 5
}

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("trading_log.log"),
        logging.StreamHandler()
    ]
)

class DatabasePool:
    def __init__(self):
        self.pool = None

    def __enter__(self):
        self.pool = psycopg2.pool.SimpleConnectionPool(5, 20, **DB_CONFIG)  # Increased pool size
        return self.pool

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.pool:
            self.pool.closeall()

# Initialize connection pool
db_pool = psycopg2.pool.SimpleConnectionPool(1, 10, **DB_CONFIG)

def get_db_connection():
    try:
        return db_pool.getconn()
    except Exception as e:
        logging.error(f"Database connection error: {str(e)}")
        raise

def return_db_connection(conn):
    if conn:
        db_pool.putconn(conn)

# Get accounts from database
def fetch_accounts():
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT account_id, server, password, contestant_name FROM leaderboard")
            accounts = cur.fetchall()
            return [dict(account) for account in accounts]
    finally:
        if conn:
            return_db_connection(conn)

def fetch_all_breach_statuses():
    """Fetch breach status for all accounts in one query"""
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT account_id, breached FROM leaderboard")
            return {str(row[0]): row[1] for row in cur.fetchall()}
    finally:
        if conn:
            return_db_connection(conn)

# Constants
ACCOUNTS = fetch_accounts()
INITIAL_BALANCE = 100000
START_DATE = datetime(2025, 3, 1)
main_running = False

def connect_to_mt5(account):
    if mt5.initialize(login=int(account["account_id"]), 
                     server=account["server"], 
                     password=account["password"]):
        logging.info(f"Connected to account {account['account_id']}")
        return True
    logging.error(f"Failed to connect to account {account['account_id']}: {mt5.last_error()}")
    return False

def fetch_trading_data(account_id, contestant_name, starting_day_balances):
    """Modified to accept pre-fetched starting day balances"""
    try:
        account_info = mt5.account_info()
        if not account_info:
            logging.error(f"Failed to get account info for {account_id}: {mt5.last_error()}")
            return None

        positions = mt5.positions_get()
        open_positions_count = len(positions) if positions is not None else 0
        now = datetime.now()
        
        # Use pre-fetched starting day balance
        starting_day_balance = starting_day_balances.get(str(account_id), INITIAL_BALANCE)
        
        # Update starting day balance at 3:30 AM
        if now.hour == 3 and now.minute in (30, 31):
            starting_day_balance = float(account_info.equity)
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE leaderboard SET starting_day_balance = %s WHERE account_id = %s::numeric",
                        (starting_day_balance, str(account_id))
                    )
                    conn.commit()

        # Calculate daily drawdown limit with null check
        daily_dd_limit = round(float(starting_day_balance) * 0.97, 2) if starting_day_balance is not None else round(INITIAL_BALANCE * 0.97, 2)

        history_deals = mt5.history_deals_get(START_DATE, now)
        if history_deals is None:
            logging.error(f"Failed to get history deals for {account_id}: {mt5.last_error()}")
            history_deals = []
            
        # Initialize variables
        total_lots = 0
        winning_trades = 0
        losing_trades = 0
        total_trades = 0
        symbol_trade_count = {}
        filtered_deals = []

        if history_deals:
            # Filter deals where entry == 0
            filtered_deals = [deal for deal in history_deals if deal is not None]

            if filtered_deals:
                total_lots = sum(deal.volume for deal in filtered_deals)
                winning_trades = sum(1 for deal in filtered_deals if deal.profit > 0)
                losing_trades = sum(1 for deal in filtered_deals if deal.profit < 0)
                total_trades = len(filtered_deals) // 2

                for deal in filtered_deals:
                    symbol = deal.symbol
                    symbol_trade_count[symbol] = symbol_trade_count.get(symbol, 0) + 1

        profit_loss = account_info.balance - INITIAL_BALANCE

        # Calculate average lots traded
        average_lots = round(total_lots / total_trades, 2) if total_trades > 0 else 0

        # Find most traded symbol and its count
        most_traded_symbol = max(symbol_trade_count.items(), key=lambda x: x[1], default=(None, 0))

        # Check for breaches
        breaches = []
        is_breached = False
        if daily_dd_limit is not None and account_info.equity < daily_dd_limit:
            breaches.append({
                "time": now.isoformat(),
                "type": "daily_drawdown",
                "details": {
                    "account_id": account_id,
                    "contestant_name": contestant_name,
                    "equity": account_info.equity,
                    "daily_dd_limit": daily_dd_limit
                }
            })
            is_breached = True
        elif account_info.equity < INITIAL_BALANCE * 0.95:
            breaches.append({
                "time": now.isoformat(),
                "type": "max_drawdown",
                "details": {
                    "account_id": account_id,
                    "contestant_name": contestant_name,
                    "equity": account_info.equity,
                    "max_drawdown_limit": INITIAL_BALANCE * 0.95
                }
            })
            is_breached = True

        # Set balance equal to equity if breached
        final_balance = account_info.equity if is_breached else account_info.balance

        return {
            "account_id": account_id,
            "contestant_name": contestant_name,
            "balance": final_balance,  # Using the adjusted balance
            "equity": account_info.equity,
            "profit_loss": round(profit_loss, 2),
            "return": round((profit_loss / INITIAL_BALANCE * 100), 2),
            "lots_traded": round(total_lots, 2),
            "average_lots": average_lots,
            "most_traded_symbol": most_traded_symbol[0],
            "symbol_trade_counts": symbol_trade_count,  # Add the full trade count dictionary
            "total_trades": total_trades,
            "winning_trades": winning_trades,
            "losing_trades": losing_trades,
            "win_rate": round((winning_trades / (winning_trades + losing_trades) * 100), 2) if (winning_trades + losing_trades) > 0 else 0,
            "starting_day_balance": starting_day_balance,
            "daily_dd_limit": daily_dd_limit,
            "breaches": breaches,
            "breached": is_breached,
            "open_positions": open_positions_count  # Add open positions count
        }
    except Exception as e:
        logging.error(f"Error processing account {account_id}: {str(e)}")
        return None

def fetch_breach_status(account_id):
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT breached FROM leaderboard WHERE account_id = %s::numeric", (str(account_id),))
            result = cur.fetchone()
            return result[0] if result else False
    except Exception as e:
        logging.error(f"Error fetching breach status: {str(e)}")
        return False
    finally:
        if conn:
            return_db_connection(conn)

def process_account(account):
    try:
        # Check if account is already breached
        if fetch_breach_status(account["account_id"]):
            logging.info(f"Skipping update for breached account {account['account_id']}")
            return None
            
        if connect_to_mt5(account):
            data = fetch_trading_data(account["account_id"], account["contestant_name"])
            if data:
                # Only update if the account wasn't previously breached
                return data
    finally:
        mt5.shutdown()

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return super(DecimalEncoder, self).default(obj)

def safe_decimal(value, default=0):
    """Safely convert a value to Decimal"""
    if value is None:
        return Decimal(str(default))
    try:
        return Decimal(str(float(value)))
    except (ValueError, TypeError, decimal.InvalidOperation):
        return Decimal(str(default))

def update_leaderboard_db(account_data_list):
    if not account_data_list:
        return
        
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            # Cast account_ids to numeric array
            account_ids = [safe_decimal(data["account_id"]) for data in account_data_list]
            cur.execute(
                "SELECT account_id, breached FROM leaderboard WHERE account_id = ANY(%s::numeric[])",
                (account_ids,)
            )
            breached_accounts = {str(row[0]): row[1] for row in cur.fetchall()}

            # Prepare batch update data
            batch_data = []
            for data in account_data_list:
                account_id = str(data["account_id"])
                # Skip if account is already breached
                if breached_accounts.get(account_id, False):
                    logging.info(f"Skipping DB update for breached account {account_id}")
                    continue

                symbol_trade_counts_json = json.dumps(
                    {k: str(v) for k, v in data["symbol_trade_counts"].items()}, 
                    cls=DecimalEncoder
                )
                breaches_json = json.dumps(data["breaches"], cls=DecimalEncoder)

                batch_data.append((
                    safe_decimal(data["balance"]),
                    safe_decimal(data["equity"]),
                    safe_decimal(data["profit_loss"]),
                    safe_decimal(data["return"]),
                    safe_decimal(data["lots_traded"]),
                    safe_decimal(data["average_lots"]),
                    data["most_traded_symbol"],
                    int(data["total_trades"]),
                    int(data["winning_trades"]),
                    int(data["losing_trades"]),
                    safe_decimal(data["win_rate"]),
                    safe_decimal(data["starting_day_balance"]),
                    safe_decimal(data["daily_dd_limit"]),
                    data["breached"],
                    datetime.now(),
                    symbol_trade_counts_json,
                    breaches_json,
                    safe_decimal(data["open_positions"]),
                    str(data["account_id"])
                ))

            if batch_data:
                cur.executemany("""
                    UPDATE leaderboard SET 
                        balance = %s::numeric,
                        equity = %s::numeric,
                        profit_loss = %s::numeric,
                        return = %s::numeric,
                        lots_traded = %s::numeric,
                        average_lots = %s::numeric,
                        most_traded_symbol = %s,
                        total_trades = %s,
                        winning_trades = %s,
                        losing_trades = %s,
                        win_rate = %s::numeric,
                        starting_day_balance = %s::numeric,
                        daily_dd_limit = %s::numeric,
                        breached = %s,
                        last_update_time = %s,
                        symbol_trade_counts = %s::jsonb,
                        breaches = %s::jsonb,
                        open_positions = %s::numeric
                    WHERE account_id = %s
                """, batch_data)
                conn.commit()
                logging.info(f"Batch updated {len(batch_data)} accounts")
    except Exception as e:
        logging.error(f"Error updating database: {str(e)}")
        traceback.print_exc()
    finally:
        if conn:
            return_db_connection(conn)

def update_metadata(all_account_data):
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            # Calculate global trade counts
            global_trade_counts = {}
            for account_data in all_account_data:
                if account_data and "symbol_trade_counts" in account_data:
                    for symbol, count in account_data["symbol_trade_counts"].items():
                        global_trade_counts[symbol] = global_trade_counts.get(symbol, 0) + int(count)

            # Find the most traded symbol
            most_traded_overall = None
            if global_trade_counts:
                most_traded_symbol = max(global_trade_counts.items(), key=lambda x: x[1])
                most_traded_overall = {
                    "most_traded_symbol": most_traded_symbol[0],
                    "total_trades": most_traded_symbol[1],
                    "global_trade_counts": global_trade_counts
                }

            # Update metadata table
            metadata_json = json.dumps(most_traded_overall, cls=DecimalEncoder) if most_traded_overall else '{}'
            cur.execute("""
                INSERT INTO metadata (id, most_traded)
                VALUES (1, %s::jsonb)
                ON CONFLICT (id) DO UPDATE
                SET most_traded = %s::jsonb,
                    last_updated_time = NOW()
            """, (metadata_json, metadata_json))
            
            conn.commit()
            logging.info("Metadata updated successfully")
    except Exception as e:
        logging.error(f"Error updating metadata: {str(e)}")
        traceback.print_exc()
    finally:
        if conn:
            return_db_connection(conn)

def recreate_db_pool():
    global db_pool
    try:
        # Close existing pool if it exists
        if db_pool:
            db_pool.closeall()
        
        # Create new pool
        db_pool = psycopg2.pool.SimpleConnectionPool(1, 10, **DB_CONFIG)
        logging.info("Database pool recreated successfully")
    except Exception as e:
        logging.error(f"Error recreating database pool: {str(e)}")
        raise

def main():
    global main_running
    if main_running:
        logging.info("Main function is already running. Exiting.")
        return
    
    try:
        main_running = True
        logging.info("Starting main function.")
        
        with DatabasePool() as pool:
            global db_pool
            db_pool = pool
            
            while True:
                try:
                    # Reconnect to database pool before each cycle
                    logging.info("Reconnecting to database pool...")
                    recreate_db_pool()
                    logging.info("Database pool reconnection successful")

                    # Fetch all breach statuses at once
                    breach_statuses = fetch_all_breach_statuses()
                    
                    # Fetch all starting day balances at once
                    conn = get_db_connection()
                    with conn.cursor() as cur:
                        cur.execute("SELECT account_id, starting_day_balance FROM leaderboard")
                        starting_day_balances = {str(row[0]): row[1] for row in cur.fetchall()}
                    return_db_connection(conn)
                    
                    all_account_data = []
                    for account in ACCOUNTS:
                        account_id = str(account["account_id"])
                        
                        # Skip already breached accounts
                        if breach_statuses.get(account_id, False):
                            logging.info(f"Skipping breached account {account_id}")
                            continue
                            
                        if connect_to_mt5(account):
                            data = fetch_trading_data(
                                account["account_id"], 
                                account["contestant_name"],
                                starting_day_balances
                            )
                            if data:
                                all_account_data.append(data)
                            mt5.shutdown()
                            time.sleep(0.5)

                    # Batch update all accounts at once
                    if all_account_data:
                        update_leaderboard_db(all_account_data)
                        update_metadata(all_account_data)

                    current_time = datetime.now()
                    next_5_minute_mark = current_time.replace(second=0, microsecond=0) + timedelta(minutes=(5 - (current_time.minute % 5)))
                    time_to_wait = (next_5_minute_mark - current_time).total_seconds()
                    if time_to_wait < 0:
                        next_5_minute_mark += timedelta(minutes=5)
                        time_to_wait = (next_5_minute_mark - current_time).total_seconds()
                    logging.info(f"Waiting for the next 5-minute mark. Waiting {time_to_wait} seconds.")
                    time.sleep(time_to_wait)
                    
                except Exception as e:
                    logging.error(f"Error in main loop: {str(e)}")
                    traceback.print_exc()
                    time.sleep(5)  # Wait before retrying
                    
    except Exception as e:
        logging.error(f"Main function error: {str(e)}")
    finally:
        main_running = False

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"Application startup error: {str(e)}\n{traceback.format_exc()}")
        raise
