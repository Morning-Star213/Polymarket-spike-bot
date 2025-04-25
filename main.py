import json
import os
import time
import requests
import threading
import json
import logging
import colorlog
from halo import Halo
from datetime import datetime, timedelta, UTC
from dotenv import load_dotenv
from web3 import Web3
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import MarketOrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY, SELL
import logging
from typing import Dict, List, Tuple, Optional
from collections import deque

# Setup logging

file_handler = logging.FileHandler('polymarket_spike_bot.log', mode='a', encoding='utf-8')
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# Create a custom StreamHandler that uses UTF-8 encoding
class UTF8StreamHandler(logging.StreamHandler):
    def emit(self, record):
        try:
            msg = self.format(record)
            stream = self.stream
            if not isinstance(msg, str):
                msg = str(msg)
            stream.write(msg + self.terminator)
            self.flush()
        except Exception:
            self.handleError(record)

console_handler = UTF8StreamHandler()
console_handler.setFormatter(colorlog.ColoredFormatter(
    "%(log_color)s%(message)s%(reset)s",
    log_colors={
        'DEBUG': 'cyan',
        'INFO': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'red,bg_white'
    }
))

logger = colorlog.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)
logger.addHandler(console_handler)


# Add locks for thread-safe access to shared data
price_history_lock = threading.Lock()
active_trades_lock = threading.Lock()
positions_lock = threading.Lock()
asset_pairs_lock = threading.Lock()
recent_trades_lock = threading.Lock()
last_trade_closed_at_lock = threading.Lock()

# Global state variables
positions = {}  
price_history = {}  # {(asset_id): deque[(timestamp, price, eventslug, outcome)]}
active_trades = {}  # {asset_id: {time.time(): type}}
latest_active_trade = {}  # {asset_id: {time.time(): type}}
last_trade_closed_at = 0
last_spike_asset = None
last_spike_price = None
initialized_assets = set()
asset_pairs = {}  # asset_id -> opposite_id
recent_trades = {}  # asset_id -> timestamp of last spike trade
max_price_history_size = 120  # entries per asset
TRADE_UNIT = 3.0  # Amount in USD for each trade
MIN_TRADE_INTERVAL = 60  # Minimum seconds between trades for the same pair
SOLD_POSITION_TIME = 1800  # 30 minutes
counter = 0
COOLDOWN_TIME = 5 # Wait for 5 seconds to update positions

load_dotenv(".env")

WEB3_PROVIDER = "https://polygon-rpc.com"
YOUR_PROXY_WALLET = Web3.to_checksum_address(os.getenv("YOUR_PROXY_WALLET"))
BOT_TRADER_ADDRESS = Web3.to_checksum_address(os.getenv("BOT_TRADER_ADDRESS"))
USDC_CONTRACT_ADDRESS = os.getenv("USDC_CONTRACT_ADDRESS")
POLYMARKET_SETTLEMENT_CONTRACT = os.getenv("POLYMARKET_SETTLEMENT_CONTRACT")

PRIVATE_KEY = os.getenv("PK")
if not PRIVATE_KEY:
    raise ValueError("❌ Private key not found. Please check your .env file.")

web3 = Web3(Web3.HTTPProvider(WEB3_PROVIDER))

# Initialize ClobClient
client = ClobClient(
    host="https://clob.polymarket.com",
    key=PRIVATE_KEY,
    chain_id=137,
    signature_type=2, # Replace with 1 if you sign in with Gmail.
    funder=YOUR_PROXY_WALLET
)
api_creds = client.create_or_derive_api_creds()
client.set_api_creds(api_creds)

# Get price history
def get_price_history(asset_id: str) -> List[Tuple[datetime, float, str, str]]:
    with price_history_lock:
        return list(price_history.get(asset_id, deque()))

# Add price to price history
def add_price(asset_id: str, timestamp: datetime, price: float, eventslug: str, outcome: str):
    with price_history_lock:
        if asset_id not in price_history:
            price_history[asset_id] = deque(maxlen=max_price_history_size)
        price_history[asset_id].append((timestamp, price, eventslug, outcome))


def get_active_trades() -> Dict[str, dict]:
    with active_trades_lock:
        return dict(active_trades)

# Add active trade
def add_active_trade(asset_id: str, trade_data: dict):
    with active_trades_lock:
        global active_trades
        active_trades[asset_id] = trade_data

# Remove active trade
def remove_active_trade(asset_id: str):
    with active_trades_lock:
        global active_trades
        active_trades.pop(asset_id, None)

# Add recent trade
def update_recent_trade(asset_id: str, type: str):
    with recent_trades_lock:
        global recent_trades
        recent_trades[asset_id] = {time.time(): type}

# Get recent trade
def get_recent_trade(asset_id: str) -> Optional[str]:
    with recent_trades_lock:
        return recent_trades.get(asset_id)

# Remove recent trade
def remove_recent_trade(asset_id: str):
    with recent_trades_lock:
        recent_trades.pop(asset_id, None)

# Get last trade time
def get_last_trade_time() -> float:
    with last_trade_closed_at_lock:
        global last_trade_closed_at
    return last_trade_closed_at

# Set last trade time
def set_last_trade_time(timestamp: float):
    with last_trade_closed_at_lock:
        global last_trade_closed_at
        last_trade_closed_at = timestamp

# Get asset pair
def get_asset_pair(asset_id: str) -> Optional[str]:
    with asset_pairs_lock:
        return asset_pairs.get(asset_id)

# Add asset pair
def add_asset_pair(asset1: str, asset2: str):
    with asset_pairs_lock:
        global initialized_assets
        asset_pairs[asset1] = asset2
        asset_pairs[asset2] = asset1
        initialized_assets.add(asset1)
        initialized_assets.add(asset2)

def is_initialized() -> bool:
    return len(initialized_assets) > 0

def update_recent_trade(asset_id: str, type: str):
    with recent_trades_lock:
        recent_trades[asset_id] = {time.time(): type}


def fetch_positions():
    url = f"https://data-api.polymarket.com/positions?user={YOUR_PROXY_WALLET}"
    try:
        response = requests.get(url)
        data = response.json()
        if not isinstance(data, list):
            return {}
        positions = {}
        for pos in data:
            eventslug = pos.get("eventSlug")
            outcome = pos.get("outcome")
            asset = pos.get("asset")
            avg_price = pos.get("avgPrice")
            shares = pos.get("size")
            initial_value = pos.get("initialValue")
            current_value = pos.get("currentValue")
            cashpnl = pos.get("cashPnl")
            percent_pnl = pos.get("percentPnl")
            realized_pnl = pos.get("realizedPnl")
            current_price = pos.get("curPrice")
            event_id = pos.get("conditionId") or pos.get("eventId") or pos.get("marketId")
            if asset and avg_price is not None and shares and current_price is not None and event_id:
                if event_id not in positions:
                    positions[event_id] = []
                positions[event_id].append({
                    "eventslug": eventslug,
                    "outcome": outcome,
                    "asset": asset,
                    "avg_price": avg_price,
                    "shares": shares,
                    "current_price": current_price,
                    "initial_value": initial_value,
                    "current_value": current_value,
                    "pnl": cashpnl,
                    "percent_pnl": percent_pnl,
                    "realized_pnl": realized_pnl
                })
        logger.info(f"💰 Fetched positions: {json.dumps(positions, indent=2)}")
        return positions
    except Exception as e:
        logger.error(f"⚠️ Failed to fetch positions: {e}")
        return {}

# def get_actual_balance(asset_id):
#     positions = fetch_positions()
#     for sides in positions.values():
#         for pos in sides:
#             if pos["asset"] == asset_id:
#                 return float(pos["shares"])
#     return 0.0

# def ensure_usdc_allowance(required_amount: float) -> bool:
#     max_retries = 3
#     base_delay = 1  # seconds
    
#     for attempt in range(max_retries):
#         try:
#             contract = web3.eth.contract(address=USDC_CONTRACT_ADDRESS, abi=[
#                 {"constant": True, "inputs": [{"name": "owner", "type": "address"}, {"name": "spender", "type": "address"}],
#                  "name": "allowance", "outputs": [{"name": "", "type": "uint256"}],
#                  "payable": False, "stateMutability": "view", "type": "function"},
#                 {"constant": False, "inputs": [{"name": "spender", "type": "address"}, {"name": "value", "type": "uint256"}],
#                  "name": "approve", "outputs": [{"name": "", "type": "bool"}],
#                  "payable": False, "stateMutability": "nonpayable", "type": "function"}
#             ])

#             wallet_address = BOT_TRADER_ADDRESS
#             current_allowance = contract.functions.allowance(wallet_address, POLYMARKET_SETTLEMENT_CONTRACT).call()
            
#             # Add 10% buffer to required amount
#             required_amount_with_buffer = int(required_amount * 1.1 * 10**6)
            
#             if current_allowance >= required_amount_with_buffer:
#                 return True

#             logger.info(f"🔄 Approving USDC allowance... (attempt {attempt + 1}/{max_retries})")
            
#             # Calculate new allowance: max of current and required
#             new_allowance = max(current_allowance, required_amount_with_buffer)
            
#             txn = contract.functions.approve(POLYMARKET_SETTLEMENT_CONTRACT, new_allowance).build_transaction({
#                 "from": wallet_address,
#                 "gas": 200000,
#                 "gasPrice": web3.eth.gas_price,
#                 "nonce": web3.eth.get_transaction_count(wallet_address),
#                 "chainId": 137
#             })
            
#             signed_txn = web3.eth.account.sign_transaction(txn, private_key=PRIVATE_KEY)
#             tx_hash = web3.eth.send_raw_transaction(signed_txn.raw_transaction)
#             receipt = web3.eth.wait_for_transaction_receipt(tx_hash)
            
#             if receipt.status == 1:
#                 logger.info(f"✅ USDC allowance updated: {tx_hash.hex()}")
#                 return True
#             else:
#                 logger.error(f"❌ USDC allowance update failed: {tx_hash.hex()}")
                
#         except Exception as e:
#             logger.error(f"⚠️ Error in USDC allowance update (attempt {attempt + 1}): {e}")
            
#         if attempt < max_retries - 1:
#             delay = base_delay * (2 ** attempt)  # exponential backoff
#             logger.info(f"⏳ Waiting {delay}s before retry...")
#             time.sleep(delay)
    
#     return False

def refresh_api_credetials() -> bool:
    """Refresh API credentials for the trading client"""
    try:
        global client, api_creds
        api_creds = client.create_or_derive_api_creds()
        client.set_api_creds(api_creds)
        logger.info("✅ API credentials refreshed successfully")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to refresh API credentials: {str(e)}")
        return False

# def place_order(side: str, asset: str, amount_in_shares: float, reason: str, 
#                 fallback_to_sell: bool = False, fallback_to_buy: bool = False) -> bool:
#     if not asset:
#         logger.warning(f"⚠️ Invalid asset. Skipping order for {asset}")
#         return False
    
#     logger.info(f"🔍 Order parameters - Side: {side}, Asset: {asset}, Amount: {amount_in_shares}, Reason: {reason}")
#     logger.info(f"🔄 Fallback settings - fallback_to_sell: {fallback_to_sell}, fallback_to_buy: {fallback_to_buy}")
    
#     max_retries = 3
#     base_delay = 1  # seconds
    
#     for attempt in range(max_retries):
#         logger.info(f"🔄 Order attempt {attempt + 1}/{max_retries} for {side} {asset}")
#         try:
#             current_price = get_current_price(asset)
#             logger.info(f"💰 Current price for {asset}: {current_price}")
#             if not current_price:
#                 logger.warning(f"⚠️ Could not fetch price for {asset}. Skipping order.")
#                 return False

#             # 💵 Enforce $10 cap on BUY orders
#             if side.upper() == "BUY":
#                 logger.info("🛒 Processing BUY order...")
#                 max_usd = os.getenv("buy_unit_size")
#                 amount_in_shares = round(max_usd / current_price, 4)
#                 usdc_needed = current_price * amount_in_shares
                
#                 # Check USDC balance before proceeding
#                 try:
#                     usdc_contract = web3.eth.contract(address=USDC_CONTRACT_ADDRESS, abi=[
#                         {"constant": True, "inputs": [{"name": "account", "type": "address"}],
#                          "name": "balanceOf", "outputs": [{"name": "", "type": "uint256"}],
#                          "payable": False, "stateMutability": "view", "type": "function"}
#                     ])
#                     usdc_balance = usdc_contract.functions.balanceOf(YOUR_PROXY_WALLET).call() / 10**6
                    
#                     logger.info(f"💵 USDC Balance: ${usdc_balance:.2f}, Required: ${usdc_needed:.2f}")
                    
#                     if usdc_balance < usdc_needed:
#                         logger.warning(f"❌ Insufficient USDC balance. Required: ${usdc_needed:.2f}, Available: ${usdc_balance:.2f}")
#                         return False
                        
#                     if not ensure_usdc_allowance(usdc_needed):
#                         logger.error(f"❌ Failed to ensure USDC allowance for {asset}")
#                         return False
#                 except Exception as e:
#                     logger.error(f"❌ Failed to check USDC balance: {str(e)}")
#                     return False
#             else:
#                 actual_shares = get_actual_balance(asset)
#                 current_price = get_current_price(asset)
#                 amount_in_shares = actual_shares - round(1.0 / current_price, 4)
#             if amount_in_shares <= 0:
#                 logger.warning(f"⚠️ Invalid share amount. Skipping SELL order for {asset}")
#                 return False
#             side_formatted = BUY if side.upper() == "BUY" else SELL
#             logger.info(f"📤 Preparing {side.upper()} order: {amount_in_shares:.4f} shares of {asset} ({reason})")

#             order_args = MarketOrderArgs(
#                 token_id=str(asset),
#                 amount=float(amount_in_shares),
#                 side=side_formatted,
#             )

#             logger.info(f"🔑 Creating signed order for {asset}...")
#             signed_order = client.create_market_order(order_args)
            
#             # Log order details without JSON serialization
#             logger.info(f"📝 Order details: {str(signed_order)}")
            
#             order_type = OrderType.FOK if (fallback_to_sell or fallback_to_buy) else OrderType.FOK
#             logger.info(f"📨 Posting order with type: {order_type}")
            
#             response = client.post_order(signed_order, order_type)
#             # Log response without JSON serialization
#             logger.info(f"📥 Order response: {str(response)}")

#             if response.get("success"):
#                 filled = response.get("data", {}).get("filledAmount", amount_in_shares)
#                 logger.info(f"✅ [{reason}] Order placed: {side.upper()} {filled:.4f} shares of {asset}")
#                 update_recent_trade(asset)
#                 return True
#             else:
#                 error_msg = response.get('errorMsg', 'Unknown error')
#                 error_code = response.get('errorCode', 'No error code')
#                 logger.error(f"❌ [{reason}] Order failed with code {error_code}: {error_msg}")
#                 logger.error(f"❌ Full error response: {json.dumps(response, indent=2)}")
                
#                 # Handle SELL fallback without recursion
#                 if side.upper() == "SELL" and fallback_to_sell and not reason.endswith("_fallback"):
#                     logger.warning(f"⚠️ SELL failed — attempting fallback with actual balance...")
#                     actual_shares = get_actual_balance(asset)
#                     logger.info(f"📊 Actual balance for {asset}: {actual_shares}")
                    
#                     if actual_shares and actual_shares > 0.001:
#                         # Create new order args with actual shares
#                         order_args = MarketOrderArgs(
#                             token_id=str(asset),
#                             amount=float(actual_shares),
#                             side=SELL,
#                         )
#                         logger.info(f"🔄 Retrying with actual shares: {actual_shares}")
#                         signed_order = client.create_market_order(order_args)
#                         response = client.post_order(signed_order, OrderType.FOK)
                        
#                         logger.info(f"📥 Fallback order response: {json.dumps(response, indent=2)}")
                        
#                         if response.get("success"):
#                             filled = response.get("data", {}).get("filledAmount", actual_shares)
#                             logger.info(f"✅ [{reason}_fallback] Order placed: SELL {filled:.4f} shares of {asset}")
#                             update_recent_trade(asset)
#                             return True
#                         else:
#                             error_msg = response.get('errorMsg', 'Unknown error')
#                             error_code = response.get('errorCode', 'No error code')
#                             logger.error(f"❌ [{reason}_fallback] Order failed with code {error_code}: {error_msg}")
#                             logger.error(f"❌ Full fallback error response: {json.dumps(response, indent=2)}")
                
#         except Exception as e:
#             logger.error(f"⚠️ Error during {side.upper()} order for {asset}: {str(e)}")
#             logger.error(f"⚠️ Error type: {type(e).__name__}")
#             import traceback
#             logger.error(f"⚠️ Stack trace: {traceback.format_exc()}")
        
#         if attempt < max_retries - 1:
#             delay = base_delay * (2 ** attempt)  # exponential backoff
#             logger.info(f"⏳ Waiting {delay}s before retry...")
#             time.sleep(delay)
    
#     return False

# def update_latest_trade(asset_id: str, opposite: str, trade_type: str, shares: float):
#     latest_active_trade[asset_id] = {
#         "asset_pair": f"{asset_id}:{opposite}",
#         "asset_id": asset_id,
#         "entry_time": time.time(),
#         "trade_type": trade_type,
#         "shares": shares
#     }

# def handle_spike_trade(asset_id: str, opposite: str, delta: float, current_price: float):

#     if delta < 0:  # Price decreased
#         # Sell spike asset but keep $1 worth
#         shares = get_actual_balance(asset_id)
#         keep_shares = round(1.0 / current_price, 4)
#         sell_shares = shares - keep_shares
        
#         if sell_shares > 0.001:
#             logger.info(f"💰 SELLing {sell_shares:.4f} shares of {asset_id}")
#             success = place_order("SELL", asset_id, sell_shares, "spike_decrease", fallback_to_sell=True)
#             if success:
#                 update_latest_trade(asset_id, opposite, "sell", sell_shares)

#         # Buy opposite asset
#         buy_shares = round(TRADE_UNIT / current_price, 4)
#         logger.info(f"💰 BUYing {buy_shares:.4f} shares of {opposite}")
#         success = place_order("BUY", opposite, buy_shares, "spike_decrease", fallback_to_buy=True)
#         if success:
#             update_latest_trade(opposite, asset_id, "buy", buy_shares)
#             add_active_trade(opposite, {
#                 "entry_price": current_price,
#                 "entry_time": datetime.now(UTC),
#                 "shares": buy_shares,
#                 "bot_triggered": True
#             })

#     elif delta > 0:  # Price increased
#         # Buy spike asset
#         buy_shares = round(TRADE_UNIT / current_price, 4)
#         logger.info(f"💰 BUYing {buy_shares:.4f} shares of {asset_id}")
#         success = place_order("BUY", asset_id, buy_shares, "spike_increase", fallback_to_buy=True)
#         if success:
#             update_latest_trade(asset_id, opposite, "buy", buy_shares)
#             add_active_trade(asset_id, {
#                 "entry_price": current_price,
#                 "entry_time": datetime.now(UTC),
#                 "shares": buy_shares,
#                 "bot_triggered": True
#             })

#         # Sell opposite asset but keep $1 worth
#         shares = get_actual_balance(opposite)
#         keep_shares = round(1.0 / current_price, 4)
#         sell_shares = shares - keep_shares
        
#         if sell_shares > 0.001:
#             logger.info(f"💰 SELLing {sell_shares:.4f} shares of {opposite}")
#             success = place_order("SELL", opposite, sell_shares, "spike_increase", fallback_to_sell=True)
#             if success:
#                 update_latest_trade(opposite, asset_id, "sell", sell_shares)

# Check if the asset was recently bought: It's a cooldown to prevent duplicate buys against the same spike
def is_recently_bought(asset_id: str) -> bool:
    now = datetime.now(UTC)
    time_since_buy = now - latest_active_trade[asset_id]["entry_time"]
    return time_since_buy.total_seconds() < 120 and latest_active_trade[asset_id]["trade_type"] == "buy"

# Check if the asset was recently sold: It's a cooldown to prevent duplicate sells against the same spike
def is_recently_sold(asset_id: str) -> bool:
    now = datetime.now(UTC)
    time_since_sell = now - latest_active_trade[asset_id]["entry_time"]
    return time_since_sell.total_seconds() < 120 and latest_active_trade[asset_id]["trade_type"] == "sell"

def detect_and_trade():
    logger.info("🔍 Scanning for spikes...")
    now = datetime.now(UTC)

    # Get all assets from price history
    all_assets = set()
    for asset_id in price_history.keys():
        all_assets.add(asset_id)

    for asset_id in all_assets:
        with price_history_lock:
            history = get_price_history(asset_id)
        if len(history) < 2:
            continue

        old_price = history[0][1]
        new_price = history[-1][1]
        eventslug = history[0][2]
        outcome = history[0][3]
        delta = (new_price - old_price) / old_price

        logger.info(f"📈 {outcome} share in {eventslug}: old={old_price:.4f}, new={new_price:.4f}, delta={delta:.2%}")

        if abs(delta) > 0.01:
            if old_price < 0.20 and old_price > 0.90:
                logger.info(f"🟨 Spike detected on {outcome} share in {eventslug} with delta {delta:.2%}: {old_price:.4f} -> {new_price:.4f} Skipping...")
                continue

            logger.info(f"🟨 Spike detected on {outcome} share in {eventslug} with delta {delta:.2%}: {old_price:.4f} -> {new_price:.4f} Entering...")
            opposite = get_asset_pair(asset_id)
            with positions_lock:
                positions = positions.copy()
            opposite_position = positions.get(opposite, None)
            opposite_outcome = opposite_position.get("outcome")
            opposite_eventslug = opposite_position.get("eventslug")
            if opposite and opposite_eventslug == eventslug:
                logger.info(f"🔄 Opposite asset: {opposite_outcome} share in {opposite_eventslug}")
            
            if not opposite:
                logger.warning(f"❌ No opposite found for {outcome} share in {eventslug}. if you don't buy {opposite} share in {opposite_eventslug} in 30min, this position will be sold!")
                counter += 1
                if counter >= SOLD_POSITION_TIME:
                    logger.info(f"🔴 {outcome} share in {eventslug} has been sold because the opposite asset was not bought in time.")

                    """Place sell order here"""
                    #TODO: change to actual order

                    counter = 0
                continue
            if delta > 0:
                if is_recently_bought(asset_id):
                    logger.info(f"🔴 {asset_id} was recently bought. Skipping...")
                    continue
                else:
                    logger.info(f"🔴 {asset_id} was not recently bought. Buying {outcome} share in {eventslug}...")
                    """Place buy asset it order here"""

                    #TODO: change to actual order

                    update_recent_trade(asset_id, "buy")
                    add_active_trade(asset_id, {
                        "entry_price": new_price,
                        "entry_time": datetime.now(UTC),
                        "shares": 2,  # TODO: change to actual shares
                        "bot_triggered": True
                    })
                    logger.info(f" Selling {opposite} share in {opposite_eventslug}...")

                    """Place sell opposite asset order here"""
                    # TODO: change to actual order

                    update_recent_trade(opposite, "sell")
                    remove_active_trade(opposite)
            elif delta < 0:
                if is_recently_sold(asset_id):
                    logger.info(f"🔴 {asset_id} was recently sold. Skipping...")
                    continue
                else:
                    logger.info(f"🔴 {asset_id} was not recently sold. Selling {outcome} share in {eventslug}...")

                    """Place sell asset it order here"""
                    # TODO: change to actual order

                    update_recent_trade(asset_id, "sell")
                    remove_active_trade(asset_id)
                    logger.info(f" Buying {opposite} share in {opposite_eventslug}...")

                    """Place buy opposite asset order here"""
                    # TODO: change to actual order

                    update_recent_trade(opposite, "buy")
                    add_active_trade(opposite, {
                        "entry_price": new_price,
                        "entry_time": datetime.now(UTC),
                        "shares": 2,  # TODO: change to actual shares
                        "bot_triggered": True
                    })

# Get current price
def get_current_price(asset_id: str) -> Optional[float]:

    try:
        history = get_price_history(asset_id)
        if history:
            return history[-1][1]
        else:
            logger.warning(f"⚠️ No price history found for {asset_id}")
    except Exception as e:
        logger.warning(f"⚠️ Live price fetch failed for {asset_id}: {e}")
    return None

# def check_trade_exits():
#     now = datetime.now(UTC)
#     active_trades = get_active_trades()
#     logger.info(f"📈 Active trades: {len(active_trades)}")

#     for asset_id in list(active_trades.keys()):
#         trade = active_trades[asset_id]
#         entry_price = trade["entry_price"]
#         entry_time = trade["entry_time"]
#         shares = trade["shares"]

#         history = get_price_history(asset_id)
#         if len(history) < 2:
#             continue
#         if history[-1][1] == entry_price:
#             continue

#         current_price = get_current_price(asset_id)
#         if current_price is None:
#             continue

#         profit = (current_price - entry_price) / entry_price
#         time_held = now - entry_time

#         if trade.get("pending_exit"):
#             cooldown = int(os.getenv("cooldown"))
#             max_retries = 3
#             retry_count = trade.get("retry_count", 0)
            
#             if retry_count >= max_retries:
#                 logger.error(f"❌ Max retries reached for {asset_id}, removing from active trades")
#                 remove_active_trade(asset_id)
#                 continue
                
#             if time.time() - trade.get("pending_exit_since", 0) < cooldown:
#                 logger.info(f"⏳ Waiting before retrying SELL for {asset_id}")
#                 continue
#             else:
#                 logger.info(f"🔁 Retrying SELL for {asset_id} after cooldown (attempt {retry_count + 1}/{max_retries})")
#                 trade["retry_count"] = retry_count + 1
#                 add_active_trade(asset_id, trade)

#         logger.info(f"🕒 {asset_id} held for {int(time_held.total_seconds() // 60)}m {int(time_held.total_seconds() % 60)}s")
#         logger.info(f"💰 PNL Check: entry={entry_price:.4f}, current={current_price:.4f}, profit={profit:.2%}")
#         logger.info(f"⏱ Checking exit logic for {asset_id} | Held: {time_held.total_seconds():.2f}s | Profit: {profit:.2%}")
#         logger.info(f"⏱ Entry Time: {entry_time}, Now: {now}, Held Seconds: {time_held.total_seconds():.2f}")

#         def handle_failed_sell(reason: str, asset_id: str):
#             with positions_lock:
#                 positions = positions.copy()
#             asset_still_held = False
#             for sides in positions.values():
#                 for pos in sides:
#                     if pos["asset"] == asset_id and float(pos["shares"]) > 0.001:
#                         asset_still_held = True
#                         break
#                 if asset_still_held:
#                     break

#             if asset_still_held:
#                 logger.warning(f"⚠️ SELL failed for {reason} — position still active, will retry later")
#                 trade["pending_exit"] = True
#                 trade["pending_exit_since"] = time.time()
#                 add_active_trade(asset_id, trade)
#             else:
#                 logger.info(f"✅ SELL likely succeeded (or dust) — clearing {asset_id}")
#                 remove_active_trade(asset_id)
#                 set_last_trade_time(time.time())

#         if profit >= float(os.getenv("take_profit")):
#             logger.info(f"🎯 TP hit for {asset_id}. Selling...")
#             success = place_order("SELL", asset_id, shares, "take_profit", fallback_to_sell=True)
#             if success:
#                 remove_active_trade(asset_id)
#                 set_last_trade_time(time.time())
#                 logger.info("✅ Trade closed — ready for next.")
#             else:
#                 handle_failed_sell("TP", asset_id)

#         elif profit <= float(os.getenv("stop_loss")):
#             logger.warning(f"🛑 SL hit for {asset_id}. Selling...")
#             success = place_order("SELL", asset_id, shares, "stop_loss", fallback_to_sell=True)
#             if success:
#                 remove_active_trade(asset_id)
#                 set_last_trade_time(time.time())
#                 logger.info("✅ Trade closed — ready for next.")
#             else:
#                 handle_failed_sell("SL", asset_id)

#         elif time_held >= timedelta(minutes=1):
#             logger.info(f"⌛ Time-based exit for {asset_id}. Selling...")
#             success = place_order("SELL", asset_id, shares, "timeout_exit", fallback_to_sell=True)
#             if success:
#                 remove_active_trade(asset_id)
#                 set_last_trade_time(time.time())
#                 logger.info("✅ Trade closed — ready for next.")
#             else:
#                 handle_failed_sell("timeout", asset_id)

# Update price history every 1 second
def update_price_history():
    try:
        while True:
            with positions_lock:
                now = datetime.now(UTC)
                global positions
                positions = fetch_positions()
        
            for event_id, assets in positions.items():
                for asset in assets:
                    eventslug = asset["eventslug"]
                    outcome = asset["outcome"]
                    asset_id = asset["asset"]
                    price = asset["current_price"]
                    add_price(asset_id, now, price, eventslug, outcome)
                    logger.info(f"💾 Updated price of {outcome} share in {eventslug}: asset_id: {asset_id}: price: ${price:.4f}")
            time.sleep(1)
    except Exception as e:
        logger.error(f"❌ Error updating price history: {e}")

# Wait for manual $1 entries on both sides of a market
def wait_for_initialization():
    max_retries = 60  # 1 minute total with 2-second sleep
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            positions = fetch_positions()
            for event_id, sides in positions.items():
                logger.info(f"🔎 Event ID {event_id}: {len(sides)}")
                if len(sides) % 2 == 0 and len(sides) > 1:
                    ids = [s["asset"] for s in sides]
                    add_asset_pair(ids[0], ids[1])
                    logger.info(f"✅ Initialized asset pair: {ids[0]} ↔ {ids[1]}")
            
            if is_initialized():
                logger.info(f"✅ Initialization complete with {len(initialized_assets)} assets.")
                return True
                
            retry_count += 1
            time.sleep(2)
            
        except Exception as e:
            logger.info(f"⚠️ Error during initialization: {e}")
            retry_count += 1
            time.sleep(2)
    
    logger.warning("❌ Initialization timed out after 2 minutes.")
    return False

# Print spikebot banner
def print_spikebot_banner():
    banner = r"""
╔════════════════════════════════════════════════════════════════════╗
║                                                                    ║
║   ███████╗██████╗ ██╗██╗  ██╗███████╗██████╗  ██████╗ ████████╗    ║
║   ██╔════╝██╔══██╗██║██║ ██╔╝██╔════╝██╔══██╗██╔═══██╗╚══██╔══╝    ║
║   ███████╗██████╔╝██║█████╔╝ █████╗  ██████╔╝██║   ██║   ██║       ║
║   ╚════██║██╔═══╝ ██║██╔═██╗ ██╔══╝  ██╔══██╗██║   ██║   ██║       ║
║   ███████║██║     ██║██║  ██╗███████╗██████╔╝╚██████╔╝   ██║       ║
║   ╚══════╝╚═╝     ╚═╝╚═╝  ╚═╝╚══════╝╚═════╝  ╚═════╝    ╚═╝       ║
║                                                                    ║
║                  🚀  P O L Y M A R K E T  B O T  🚀                ║
║                                                                    ║
╚════════════════════════════════════════════════════════════════════╝
    """
    print(banner)

if __name__ == "__main__":
    print_spikebot_banner()
    spinner = Halo(text="Waiting for manual $1 entries on both sides of a market...", spinner="dots")
    spinner.start()
    time.sleep(5)
    logger.info(f"🚀 Spike-detection bot started at {datetime.now(UTC)}")
    
    if not wait_for_initialization():
        spinner.fail("❌ Failed to initialize. Exiting.")
        exit(1)
    else:
        spinner.succeed("Initialized successfully")
        
    price_update_thread = threading.Thread(target=update_price_history, daemon=True)
    detect_and_trade_thread = threading.Thread(target=detect_and_trade, daemon=True)
    # check_trade_exits_thread = threading.Thread(target=check_trade_exits, daemon=True)
    
    price_update_thread.start()
    detect_and_trade_thread.start()
    # check_trade_exits_thread.start()

    last_refresh_time = time.time()
    refresh_interval = 3600
    
    while True:
        try:
            current_time = time.time()
            
            if current_time - last_refresh_time > refresh_interval:
                if refresh_api_credetials():
                    last_refresh_time = current_time
                else:
                    logger.warning("⚠️ Failed to refresh API credentials. Will retry in 5 minutes.")
                    time.sleep(300)
                    continue
        except KeyboardInterrupt:
            logger.info("👋 Shutting down gracefully...")
            break
        except Exception as e:
            logger.error(f"❌ Error in main loop: {e}")
            time.sleep(1)