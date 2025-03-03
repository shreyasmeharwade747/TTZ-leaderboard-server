import MetaTrader5 as mt5
import json
import logging
from datetime import datetime, timedelta
import time
import threading
import traceback
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import socket
from functools import wraps
from urllib.parse import urlparse
import psycopg2.pool
import dns.resolver
from decimal import Decimal

# Update to use direct connection URL
DATABASE_URL = "postgresql://postgres:2AT4lCvfnpcCZtPY@db.ldonzqwaxlugziqtqedy.supabase.co:5432/postgres"

# Update constants
DB_CONNECT_TIMEOUT = 30  # Renamed from DB_POOL_TIMEOUT
db_pool = None

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("trading_log.log"),
        logging.StreamHandler()
    ]
)

def retry_on_dns_error(max_retries=3, delay=2):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except (socket.gaierror, socket.error) as e:
                    retries += 1
                    if retries == max_retries:
                        logging.error(f"Max retries ({max_retries}) reached. Database connection failed: {str(e)}")
                        raise
                    logging.warning(f"Database connection attempt {retries} failed. Retrying in {delay} seconds...")
                    time.sleep(delay)
            return None
        return wrapper
    return decorator

def init_db_pool():
    global db_pool
    try:
        db_url = DATABASE_URL
        if not db_url:
            raise ValueError("DATABASE_URL is not set")
        
        # Parse the URL
        parsed_url = urlparse(db_url)
        
        # Simplified connection parameters for direct connection
        db_params = {
            'host': parsed_url.hostname,  # Use hostname directly
            'port': parsed_url.port or 5432,
            'user': parsed_url.username,
            'password': parsed_url.password,
            'dbname': parsed_url.path.lstrip('/'),
            'connect_timeout': DB_CONNECT_TIMEOUT,  # Use timeout only in connection params
            'sslmode': 'require'
        }
        
        # Create a simple connection pool with minimal connections
        db_pool = psycopg2.pool.SimpleConnectionPool(
            1, 3,  # Minimal pool size for direct connections
            **db_params
        )
        logging.info(f"Database connection initialized successfully using direct connection to Supabase")
    except Exception as e:
        logging.error(f"Failed to initialize database connection: {str(e)}")
        raise

# Add new connection management class
class DatabaseConnection:
    def __init__(self):
        self.conn = None

    def __enter__(self):
        retries = 3
        while retries > 0:
            try:
                self.conn = get_db_connection()
                return self.conn
            except psycopg2.Error as e:
                retries -= 1
                if retries == 0:
                    raise
                time.sleep(1)
                logging.warning(f"Retrying database connection. Attempts left: {retries}")

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            return_db_connection(self.conn)

# Update the get_db_connection function
@retry_on_dns_error()
def get_db_connection():
    global db_pool
    if db_pool is None:
        init_db_pool()
    try:
        # Remove timeout parameter from getconn
        conn = db_pool.getconn()
        return conn
    except Exception as e:
        logging.error(f"Database connection error: {str(e)}")
        raise

def return_db_connection(conn):
    global db_pool
    if db_pool and conn:
        db_pool.putconn(conn)

# Get accounts from database
@retry_on_dns_error()
def fetch_accounts():
    try:
        with DatabaseConnection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT account_id, server, password, contestant_name FROM leaderboard")
                accounts = cur.fetchall()
                return [dict(account) for account in accounts]
    except Exception as e:
        logging.error(f"Error fetching accounts: {str(e)}")
        return []

# Constants
ACCOUNTS = fetch_accounts()
INITIAL_BALANCE = 100000
START_DATE = datetime(2025, 1, 19)
main_running = False

def connect_to_mt5(account):
    if mt5.initialize(login=int(account["account_id"]), 
                     server=account["server"], 
                     password=account["password"]):
        logging.info(f"Connected to account {account['account_id']}")
        return True
    logging.error(f"Failed to connect to account {account['account_id']}: {mt5.last_error()}")
    return False

def fetch_trading_data(account_id, contestant_name):
    try:
        account_info = mt5.account_info()
        if not account_info:
            logging.error(f"Failed to get account info for {account_id}: {mt5.last_error()}")
            return None

        now = datetime.now()
        
        # Get starting day balance from database with default fallback
        starting_day_balance = INITIAL_BALANCE
        try:
            with DatabaseConnection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT starting_day_balance FROM leaderboard WHERE account_id = %s", (account_id,))
                    result = cur.fetchone()
                    if result and result[0] is not None:
                        starting_day_balance = float(result[0])
                    else:
                        # If no starting balance found, use current equity
                        starting_day_balance = float(account_info.equity)
                        
                    # Update starting day balance at 3:30 AM or 3:31 AM
                    if now.hour == 3 and now.minute in (30, 31):
                        starting_day_balance = float(account_info.equity)
                        cur.execute("UPDATE leaderboard SET starting_day_balance = %s WHERE account_id = %s",
                                  (starting_day_balance, account_id))
                        conn.commit()
        except Exception as e:
            logging.error(f"Database error while fetching starting balance: {str(e)}")

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
                winning_trades = sum(1 for deal in filtered_deals if deal.profit > 0) - 1
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
            "breached": is_breached
        }
    except Exception as e:
        logging.error(f"Error processing account {account_id}: {str(e)}")
        return None

def process_account(account):
    try:
        if connect_to_mt5(account):
            data = fetch_trading_data(account["account_id"], account["contestant_name"])
            if data:
                update_leaderboard_db(data)
            return data
    finally:
        mt5.shutdown()

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return super(DecimalEncoder, self).default(obj)

# Update update_leaderboard_db to use the new context manager
@retry_on_dns_error()
def update_leaderboard_db(data):
    try:
        with DatabaseConnection() as conn:
            with conn.cursor() as cur:
                # Convert numeric values to Decimal for database storage
                symbol_trade_counts_json = json.dumps(
                    {k: str(v) for k, v in data["symbol_trade_counts"].items()}, 
                    cls=DecimalEncoder
                )
                breaches_json = json.dumps(data["breaches"], cls=DecimalEncoder)
                
                cur.execute("""
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
                        breaches = %s::jsonb
                    WHERE account_id = %s
                """, (
                    Decimal(str(data["balance"])),
                    Decimal(str(data["equity"])),
                    Decimal(str(data["profit_loss"])),
                    Decimal(str(data["return"])),
                    Decimal(str(data["lots_traded"])),
                    Decimal(str(data["average_lots"])),
                    data["most_traded_symbol"],
                    int(data["total_trades"]),
                    int(data["winning_trades"]),
                    int(data["losing_trades"]),
                    Decimal(str(data["win_rate"])),
                    Decimal(str(data["starting_day_balance"])),
                    Decimal(str(data["daily_dd_limit"])),
                    data["breached"],
                    datetime.now(),
                    symbol_trade_counts_json,
                    breaches_json,
                    str(data["account_id"])
                ))
                conn.commit()
    except Exception as e:
        logging.error(f"Error updating database: {str(e)}")
        traceback.print_exc()  # Add stack trace for better debugging

# Update update_metadata to use the new context manager
@retry_on_dns_error()
def update_metadata(all_account_data):
    try:
        with DatabaseConnection() as conn:
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

# Update main function to include better cleanup
def main():
    global main_running, db_pool
    if main_running:
        logging.info("Main function is already running. Exiting.")
        return
    
    try:
        init_db_pool()
        main_running = True
        logging.info("Starting main function.")
        
        while True:
            try:
                all_account_data = []
                for account in ACCOUNTS:
                    data = process_account(account)
                    if data:
                        all_account_data.append(data)
                        time.sleep(0.5)

                # Update metadata after processing all accounts
                if all_account_data:
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
                time.sleep(5)  # Add delay before retry
                
    except Exception as e:
        logging.error(f"Main function error: {str(e)}")
    finally:
        main_running = False
        if db_pool:
            db_pool.closeall()
            db_pool = None

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"Application startup error: {str(e)}\n{traceback.format_exc()}")
        raise
