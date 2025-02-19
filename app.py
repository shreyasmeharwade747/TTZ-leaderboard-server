from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import MetaTrader5 as mt5
import json
import logging
from datetime import datetime, timedelta
import time
from filelock import FileLock
import os  # Import os to check for file existence
import threading  # Import threading to run the Flask app in a separate thread
import traceback
import uvicorn
from typing import Dict, List, Optional
from fastapi.staticfiles import StaticFiles
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("trading_log.log"),
        logging.StreamHandler()
    ]
)

ACCOUNTS = [
    {
        "account_id": "243108302",
        "server": "Exness-MT5Trial14",
        "password": "FubK)4qj",
        "contestant_name": "CK"
    },
    {
        "account_id": "243108377",
        "server": "Exness-MT5Trial14",
        "password": "C2Y)SzR8",
        "contestant_name": "annuayan"
    },
    {
        "account_id": "243108443",
        "server": "Exness-MT5Trial14",
        "password": "ep(5yN3)",
        "contestant_name": "SAMEER"
    },
    {
        "account_id": "243108493",
        "server": "Exness-MT5Trial14",
        "password": "wT8vtA)2",
        "contestant_name": "Ravibankar27"
    },
    {
        "account_id": "243108560",
        "server": "Exness-MT5Trial14",
        "password": "a2(ekDTY",
        "contestant_name": "Pune ka Sheikh"
    },
    {
        "account_id": "239242676",
        "server": "Exness-MT5Trial6",
        "password": "R)4JMv(c",
        "contestant_name": "Raghav"
    },
    {
        "account_id": "243108663",
        "server": "Exness-MT5Trial14",
        "password": "YRVx7F(T",
        "contestant_name": "Ronit"
    },
    {
        "account_id": "243108712",
        "server": "Exness-MT5Trial14",
        "password": "P9d(YxuD",
        "contestant_name": "Bhai"
    },
    {
        "account_id": "243108771",
        "server": "Exness-MT5Trial14",
        "password": "ED2q)5sY",
        "contestant_name": "Arpit"
    },
    {
        "account_id": "243108822",
        "server": "Exness-MT5Trial14",
        "password": "Rp(v7TrD",
        "contestant_name": "Rawhim"
    },
    {
        "account_id": "243108868",
        "server": "Exness-MT5Trial14",
        "password": "Cpn)8J49",
        "contestant_name": "Piyush"
    },
    {
        "account_id": "202847834",
        "server": "Exness-MT5Trial7",
        "password": "dLq(s3G9",
        "contestant_name": "Aman Khurshid"
    },
    {
        "account_id": "243108962",
        "server": "Exness-MT5Trial14",
        "password": "Smw5a(c)",
        "contestant_name": "Lolz"
    },
    {
        "account_id": "243109004",
        "server": "Exness-MT5Trial14",
        "password": "f(2W74Es",
        "contestant_name": "Bhavya"
    },
    {
        "account_id": "243109051",
        "server": "Exness-MT5Trial14",
        "password": "wV3yG(QZ",
        "contestant_name": "CryptonX_nitin"
    },
    {
        "account_id": "243109252",
        "server": "Exness-MT5Trial14",
        "password": "KP8c4)ge",
        "contestant_name": "Trader"
    },
    {
        "account_id": "239242777",
        "server": "Exness-MT5Trial6",
        "password": "q4)Z9dyR",
        "contestant_name": "metahuman092"
    },
    {
        "account_id": "239242784",
        "server": "Exness-MT5Trial6",
        "password": "j4)pC8nM",
        "contestant_name": "VEDANT"
    },
    {
        "account_id": "239242794",
        "server": "Exness-MT5Trial6",
        "password": "Tp)3aukc",
        "contestant_name": "MOhit"
    },
    {
        "account_id": "243109436",
        "server": "Exness-MT5Trial14",
        "password": "U)gPre4D",
        "contestant_name": "Reapexx"
    },
    {
        "account_id": "239242808",
        "server": "Exness-MT5Trial6",
        "password": "W9VXh7)B",
        "contestant_name": "Berserker"
    },
    {
        "account_id": "243109534",
        "server": "Exness-MT5Trial14",
        "password": "H2(NZQqv",
        "contestant_name": "SushBoi"
    },
    {
        "account_id": "202847932",
        "server": "Exness-MT5Trial7",
        "password": "dyHP5S(2",
        "contestant_name": "BaSU"
    },
    {
        "account_id": "243110702",
        "server": "Exness-MT5Trial14",
        "password": "b2gPe(GE",
        "contestant_name": "FLASH"
    },
    {
        "account_id": "239243021",
        "server": "Exness-MT5Trial6",
        "password": "VU(7eBpr",
        "contestant_name": "Gautam"
    },
    {
        "account_id": "243110890",
        "server": "Exness-MT5Trial14",
        "password": "jRcp(X4C",
        "contestant_name": "Dhama"
    }


]

INITIAL_BALANCE = 100000
START_DATE = datetime(2025, 1, 19)

# Initialize FastAPI app and enable CORS
app = FastAPI(title="Trading Data API")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# Global flag to check if main is running
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
        
        # Simplified starting day balance logic
        starting_day_balance = INITIAL_BALANCE
        try:
            with FileLock("leaderboard.json.lock"):
                with open("leaderboard.json", "r") as f:
                    leaderboard_data = json.load(f)
                    # Find the account's current data
                    for entry in leaderboard_data.get("data", []):
                        if entry["account_id"] == account_id:
                            starting_day_balance = entry.get("starting_day_balance", INITIAL_BALANCE)
                            break
                            
                # Update starting day balance at 3:30 AM or 3:31 AM
                if now.hour == 3 and now.minute in (30, 31):
                    starting_day_balance = account_info.equity
        except (FileNotFoundError, json.JSONDecodeError):
            pass  # Use default INITIAL_BALANCE if file doesn't exist or is invalid

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
            return data
    finally:
        mt5.shutdown()

def update_leaderboard(new_data):
    try:
        with FileLock("leaderboard.json.lock"):
            # Read existing data
            try:
                with open("leaderboard.json", "r") as f:
                    existing_data = json.load(f)
                    if isinstance(existing_data, list):
                        existing_data = {"metadata": {}, "data": [], "breaches": []}
            except FileNotFoundError:
                existing_data = {"metadata": {}, "data": [], "breaches": []}

            # Ensure breaches key exists
            if "breaches" not in existing_data:
                existing_data["breaches"] = []

            # Initialize or get the global trade counts from metadata
            if "global_trade_counts" not in existing_data["metadata"]:
                existing_data["metadata"]["global_trade_counts"] = {}

            # Collect and aggregate symbol trade counts from all accounts
            global_trade_counts = {}
            for new_entry in new_data:
                if "symbol_trade_counts" in new_entry:
                    for symbol, count in new_entry["symbol_trade_counts"].items():
                        if symbol in global_trade_counts:
                            global_trade_counts[symbol] += count
                        else:
                            global_trade_counts[symbol] = count

            # Update the global trade counts in metadata
            existing_data["metadata"]["global_trade_counts"] = global_trade_counts
            
            # Find the most traded symbol across all accounts
            if global_trade_counts:
                most_traded_overall = max(global_trade_counts.items(), key=lambda x: x[1])
                existing_data["metadata"]["most_traded_overall"] = {
                    "symbol": most_traded_overall[0],
                    "total_trades": most_traded_overall[1]
                }

            # Update existing data or add new data
            for new_entry in new_data:
                for existing_entry in existing_data["data"]:
                    if existing_entry["account_id"] == new_entry["account_id"]:
                        if existing_entry.get("breached"):
                            break
                        existing_entry.update(new_entry)
                        if new_entry.get("breaches"):
                            existing_data["breaches"].extend(new_entry["breaches"])
                        break
                else:
                    existing_data["data"].append(new_entry)
                    if new_entry.get("breaches"):
                        existing_data["breaches"].extend(new_entry["breaches"])

            # Update the last update time in metadata
            existing_data["metadata"]["last_update_time"] = datetime.now().isoformat()

            # Write updated data back to the file
            with open("leaderboard.json", "w") as f:
                json.dump(existing_data, f, indent=4)

            logging.info(f"Leaderboard updated successfully at {existing_data['metadata']['last_update_time']}.")
    except Exception as e:
        logging.error(f"Error updating leaderboard: {str(e)}")

def read_leaderboard():
    """Read the leaderboard data from the JSON file, creating it if it doesn't exist."""
    try:
        # Check if the leaderboard.json file exists
        if not os.path.exists("leaderboard.json"):
            # Create the file with an initial structure
            with open("leaderboard.json", "w") as f:
                json.dump({"metadata": {}, "data": [], "breaches": []}, f)  # Initialize with an empty structure

        # Now read the file
        with open("leaderboard.json", "r") as f:
            leaderboard_data = json.load(f)
        return leaderboard_data
    except Exception as e:
        logging.error(f"Error reading leaderboard: {str(e)}")
        return None

@app.get("/data")
async def get_leaderboard_data():
    try:
        leaderboard_data = read_leaderboard()
        if leaderboard_data is None:
            logging.error("Failed to read leaderboard data")
            raise HTTPException(status_code=500, detail="An error occurred while reading the leaderboard.")
        return JSONResponse(content=leaderboard_data)
    except Exception as e:
        logging.error(f"Error in /data endpoint: {str(e)}\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail="An unexpected error occurred.")

@app.get("/health")
async def health_check():
    """Simple health check endpoint to verify the API is responsive"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

os.makedirs(".well-known/acme-challenge", exist_ok=True)
app.mount("/.well-known", StaticFiles(directory=".well-known"), name="well-known")

def main():
    global main_running
    if main_running:
        logging.info("Main function is already running. Exiting.")
        return  # Exit if main is already running
    main_running = True  # Set the flag to indicate main is running
    logging.info("Starting main function.")

    while True:
        results = []
        for account in ACCOUNTS:
            data = process_account(account)
            if data:
                results.append(data)
                time.sleep(0.5)  # Add a slight delay between processing accounts

        # Update leaderboard with new results
        update_leaderboard(results)

        current_time = datetime.now()
        next_5_minute_mark = current_time.replace(second=0, microsecond=0) + timedelta(minutes=(5 - (current_time.minute % 5)))
        time_to_wait = (next_5_minute_mark - current_time).total_seconds()
        if time_to_wait < 0:
            next_5_minute_mark += timedelta(minutes=5)
            time_to_wait = (next_5_minute_mark - current_time).total_seconds()
        logging.info(f"Waiting for the next 5-minute mark. Waiting {time_to_wait} seconds.")
        time.sleep(time_to_wait)
        
def run_fastapi():
    try:
        uvicorn.run(
            "app:app",
            host="0.0.0.0",
            port=443,
            ssl_keyfile="C:/Users/shreyascracked/Desktop/TTZServer/privkey.pem",
            ssl_certfile="C:/Users/shreyascracked/Desktop/TTZServer/cert.pem",
            reload=False
        )
    except Exception as e:
        logging.error(f"FastAPI server error: {str(e)}\n{traceback.format_exc()}")
        raise

if __name__ == "__main__":
    try:
        # Start the main function in a separate thread
        main_thread = threading.Thread(target=main, daemon=True)
        main_thread.start()
        
        # Run the FastAPI app
        run_fastapi()
    except Exception as e:
        logging.error(f"Application startup error: {str(e)}\n{traceback.format_exc()}")
        raise
    