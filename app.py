import MetaTrader5 as mt5
import json
import logging
from datetime import datetime, timedelta
import time
import threading
import traceback
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor
import os

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("trading_log.log"),
        logging.StreamHandler()
    ]
)

# Database connection setup
def get_db_connection():
    try:
        conn = psycopg2.connect(os.getenv('DATABASE_URL'))
        return conn
    except Exception as e:
        logging.error(f"Database connection error: {str(e)}")
        raise

# Get accounts from database
def fetch_accounts():
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT account_id, server, password, contestant_name FROM leaderboard")
            accounts = cur.fetchall()
            return [dict(account) for account in accounts]

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
            raise ValueError(f"Failed to fetch data: {mt5.last_error()}")

        now = datetime.now()
        
        # Get starting day balance from database
        starting_day_balance = INITIAL_BALANCE
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT starting_day_balance FROM leaderboard WHERE account_id = %s", (account_id,))
                result = cur.fetchone()
                if result:
                    starting_day_balance = result[0]
                
                # Update starting day balance at 3:30 AM or 3:31 AM
                if now.hour == 3 and now.minute in (30, 31):
                    starting_day_balance = account_info.equity
                    cur.execute("UPDATE leaderboard SET starting_day_balance = %s WHERE account_id = %s",
                              (starting_day_balance, account_id))
                    conn.commit()

        # Calculate daily drawdown limit
        daily_dd_limit = round(starting_day_balance * 0.97, 2)

        history_deals = mt5.history_deals_get(START_DATE, now)
        
        # Initialize variables for trading data
        total_lots = 0
        winning_trades = 0
        total_trades = 0
        symbol_trade_count = {}  # Dictionary to count trades per symbol
        
        if history_deals:
            # Filter deals where entry == 0
            filtered_deals = [deal for deal in history_deals if True]

            total_lots = sum(deal.volume for deal in filtered_deals) / 2
            winning_trades = sum(1 for deal in filtered_deals if deal.profit > 0) -1
            total_trades = len(filtered_deals) // 2  # Each trade has two deals (open and close)

            # Count trades per symbol
            for deal in filtered_deals:
                symbol = deal.symbol
                if symbol in symbol_trade_count:
                    symbol_trade_count[symbol] += 1
                else:
                    symbol_trade_count[symbol] = 1

        profit_loss = account_info.balance - INITIAL_BALANCE
        losing_trades = sum(1 for deal in filtered_deals if deal.profit < 0)

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

def update_leaderboard_db(data):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE leaderboard SET 
                        balance = %s,
                        equity = %s,
                        profit_loss = %s,
                        return = %s,
                        lots_traded = %s,
                        average_lots = %s,
                        most_traded_symbol = %s,
                        total_trades = %s,
                        winning_trades = %s,
                        losing_trades = %s,
                        win_rate = %s,
                        starting_day_balance = %s,
                        daily_dd_limit = %s,
                        breached = %s,
                        last_update_time = %s,
                        symbol_trade_counts = %s,
                        breaches = %s
                    WHERE account_id = %s
                """, (
                    data["balance"],
                    data["equity"],
                    data["profit_loss"],
                    data["return"],
                    data["lots_traded"],
                    data["average_lots"],
                    data["most_traded_symbol"],
                    data["total_trades"],
                    data["winning_trades"],
                    data["losing_trades"],
                    data["win_rate"],
                    data["starting_day_balance"],
                    data["daily_dd_limit"],
                    data["breached"],
                    datetime.now(),
                    json.dumps(data["symbol_trade_counts"]),
                    json.dumps(data["breaches"]),
                    data["account_id"]
                ))
                conn.commit()
    except Exception as e:
        logging.error(f"Error updating database: {str(e)}")

def main():
    global main_running
    if main_running:
        logging.info("Main function is already running. Exiting.")
        return
    main_running = True
    logging.info("Starting main function.")

    while True:
        for account in ACCOUNTS:
            data = process_account(account)
            if data:
                time.sleep(0.5)

        current_time = datetime.now()
        next_5_minute_mark = current_time.replace(second=0, microsecond=0) + timedelta(minutes=(5 - (current_time.minute % 5)))
        time_to_wait = (next_5_minute_mark - current_time).total_seconds()
        if time_to_wait < 0:
            next_5_minute_mark += timedelta(minutes=5)
            time_to_wait = (next_5_minute_mark - current_time).total_seconds()
        logging.info(f"Waiting for the next 5-minute mark. Waiting {time_to_wait} seconds.")
        time.sleep(time_to_wait)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"Application startup error: {str(e)}\n{traceback.format_exc()}")
        raise
