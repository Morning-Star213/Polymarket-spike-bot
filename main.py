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
from collections import deque, defaultdict
from threading import Lock

# Load environment variables first
load_dotenv(".env")

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

class ThreadSafeState:
    def __init__(self, max_price_history_size: int = 120):
        self._price_history_lock = Lock()
        self._active_trades_lock = Lock()
        self._positions_lock = Lock()
        self._asset_pairs_lock = Lock()
        self._recent_trades_lock = Lock()
        self._last_trade_closed_at_lock = Lock()
        self._initialized_assets_lock = Lock()
        self._last_trade_closed_at_lock = Lock()
        self._last_spike_asset_lock = Lock()
        self._last_spike_price_lock = Lock()
        self._max_price_history_size = max_price_history_size
        self._positions_lock = Lock()

        self._price_history = defaultdict(lambda: deque(maxlen=max_price_history_size))
        self._active_trades = {}
        self._positions = {}
        self._asset_pairs = {}
        self._recent_trades = {}
        self._last_trade_closed_at = 0
        self._initialized_assets = set()
        self._last_spike_asset = None
        self._last_spike_price = None


# Global state variables

TRADE_UNIT = float(os.getenv("trade_unit"))  # Amount in USD for each trade
counter = 0
COOLDOWN_TIME = float(os.getenv("cooldown_time")) # Wait for 5 seconds to update positions
SLIPPAGE_TOLERANCE = float(os.getenv("slippage_tolerance")) # slippage tolerance
PCT_PROFIT = float(os.getenv("pct_profit"))
PCT_LOSS = float(os.getenv("pct_loss"))
CASH_PROFIT = float(os.getenv("cash_profit"))
CASH_LOSS = float(os.getenv("cash_loss"))


# Update global positions
def update_global_positions(state: ThreadSafeState, new_positions: dict):
    with state._positions_lock:
        state._positions = new_positions

# Get global positions
def get_global_positions(state: ThreadSafeState) -> dict:
    with state._positions_lock:
        return state._positions.copy()

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

# Add a threading.Event for synchronization
price_update_event = threading.Event()

# Get price history
def get_price_history(state: ThreadSafeState, asset_id: str) -> deque:
    with state._price_history_lock:
        return state._price_history.get(asset_id, deque())

# Add price to price history
def add_price(state: ThreadSafeState, asset_id: str, timestamp: datetime, price: float, eventslug: str, outcome: str):
    with state._price_history_lock:
        if not isinstance(asset_id, str):
            logger.error(f"Invalid asset_id type: {type(asset_id)}")
            return
        if asset_id not in state._price_history:
            state._price_history[asset_id] = deque(maxlen=state._max_price_history_size)
        state._price_history[asset_id].append((timestamp, price, eventslug, outcome))

# Get active trades
def get_active_trades(state: ThreadSafeState) -> Dict[str, dict]:
    with state._active_trades_lock:
        return dict(state._active_trades)

# Add active trade
def add_active_trade(state: ThreadSafeState, asset_id: str, trade_data: dict):
    with state._active_trades_lock:
        state._active_trades[asset_id] = trade_data

# Remove active trade
def remove_active_trade(state: ThreadSafeState, asset_id: str):
    with state._active_trades_lock:
        state._active_trades.pop(asset_id, None)

# Add recent trade
def update_recent_trade(state: ThreadSafeState, asset_id: str, type: str):
    with state._recent_trades_lock:
        # Initialize the dictionary for this asset_id if it doesn't exist
        if asset_id not in state._recent_trades:
            state._recent_trades[asset_id] = {"buy": None, "sell": None}
        
        # Update the specific trade type (buy or sell) with current timestamp
        state._recent_trades[asset_id][type] = time.time()

# Get last trade time
def get_last_trade_time(state: ThreadSafeState) -> float:
    with state._last_trade_closed_at_lock:
        return state._last_trade_closed_at

# Set last trade time
def set_last_trade_time(state: ThreadSafeState, timestamp: float):
    with state._last_trade_closed_at_lock:
        state._last_trade_closed_at = timestamp

# Get asset pair
def get_asset_pair(state: ThreadSafeState, asset_id: str) -> Optional[str]:
    with state._asset_pairs_lock:
        return state._asset_pairs.get(asset_id)

# Add asset pair
def add_asset_pair(state: ThreadSafeState, asset1: str, asset2: str):
    with state._asset_pairs_lock:
        state._asset_pairs[asset1] = asset2
        state._asset_pairs[asset2] = asset1
        state._initialized_assets.add(asset1)
        state._initialized_assets.add(asset2)

# Check if initialized
def is_initialized(state: ThreadSafeState) -> bool:
    with state._initialized_assets_lock:
        return len(state._initialized_assets) > 0

# Fetch positions
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
        logger.error(f"❌ Failed to fetch positions: {str(e)}")
        return {}

# Ensure USDC allowance
def ensure_usdc_allowance(required_amount: float) -> bool:
    max_retries = 3
    base_delay = 1  # seconds
    
    for attempt in range(max_retries):
        try:
            contract = web3.eth.contract(address=USDC_CONTRACT_ADDRESS, abi=[
                {"constant": True, "inputs": [{"name": "owner", "type": "address"}, {"name": "spender", "type": "address"}],
                 "name": "allowance", "outputs": [{"name": "", "type": "uint256"}],
                 "payable": False, "stateMutability": "view", "type": "function"},
                {"constant": False, "inputs": [{"name": "spender", "type": "address"}, {"name": "value", "type": "uint256"}],
                 "name": "approve", "outputs": [{"name": "", "type": "bool"}],
                 "payable": False, "stateMutability": "nonpayable", "type": "function"}
            ])

            wallet_address = BOT_TRADER_ADDRESS
            current_allowance = contract.functions.allowance(wallet_address, POLYMARKET_SETTLEMENT_CONTRACT).call()
            
            # Add 10% buffer to required amount
            required_amount_with_buffer = int(required_amount * 1.1 * 10**6)
            
            if current_allowance >= required_amount_with_buffer:
                return True

            logger.info(f"🔄 Approving USDC allowance... (attempt {attempt + 1}/{max_retries})")
            
            # Calculate new allowance: max of current and required
            new_allowance = max(current_allowance, required_amount_with_buffer)
            
            txn = contract.functions.approve(POLYMARKET_SETTLEMENT_CONTRACT, new_allowance).build_transaction({
                "from": wallet_address,
                "gas": 200000,
                "gasPrice": web3.eth.gas_price,
                "nonce": web3.eth.get_transaction_count(wallet_address),
                "chainId": 137
            })
            
            signed_txn = web3.eth.account.sign_transaction(txn, private_key=PRIVATE_KEY)
            tx_hash = web3.eth.send_raw_transaction(signed_txn.raw_transaction)
            receipt = web3.eth.wait_for_transaction_receipt(tx_hash)
            
            if receipt.status == 1:
                logger.info(f"✅ USDC allowance updated: {tx_hash.hex()}")
                return True
            else:
                logger.error(f"❌ USDC allowance update failed: {tx_hash.hex()}")
                
        except Exception as e:
            logger.error(f"⚠️ Error in USDC allowance update (attempt {attempt + 1}): {e}")
            
        if attempt < max_retries - 1:
            delay = base_delay * (2 ** attempt)  # exponential backoff
            logger.info(f"⏳ Waiting {delay}s before retry...")
            time.sleep(delay)
    
    return False

# Refresh API credentials for the trading client
def refresh_api_credetials() -> bool:
    try:
        global client, api_creds
        api_creds = client.create_or_derive_api_creds()
        client.set_api_creds(api_creds)
        logger.info("✅ API credentials refreshed successfully")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to refresh API credentials: {str(e)}")
        return False

# Get min ask data for an asset to minimize the slippage and get the best price
def get_min_ask_data(asset: str):
    try:
        order = client.get_order_book(asset)
        buy_price = client.get_price(asset, "BUY")
        min_ask_price = order.asks[-1].price
        min_ask_size = order.asks[-1].size
        return {
            "buy_price": buy_price,
            "min_ask_price": min_ask_price,
            "min_ask_size": min_ask_size
        }
    except Exception as e:
        logger.error(f"❌ Failed to get bid data for {asset}: {str(e)}")
        return None

# Get max bid data for an asset to minimize the slippage and get the best price
def get_max_bid_data(asset: str):
    try:
        order = client.get_order_book(asset)
        sell_price = client.get_price(asset, "SELL")
        max_bid_price = order.bids[-1].price
        max_bid_size = order.bids[-1].size
        return {
            "sell_price": sell_price,
            "max_bid_price": max_bid_price,
            "max_bid_size": max_bid_size
        }
    except Exception as e:
        logger.error(f"❌ Failed to get bid data for {asset}: {str(e)}")
        return None    

# Check USDC balance and ensure allowance before proceeding
def check_usdc_balance( usdc_needed: float):                   # Check USDC balance before proceeding
    try:
        usdc_contract = web3.eth.contract(address=USDC_CONTRACT_ADDRESS, abi=[
            {"constant": True, "inputs": [{"name": "account", "type": "address"}],
                "name": "balanceOf", "outputs": [{"name": "", "type": "uint256"}],
                "payable": False, "stateMutability": "view", "type": "function"}
        ])
        usdc_balance = usdc_contract.functions.balanceOf(YOUR_PROXY_WALLET).call() / 10**6
        
        logger.info(f"💵 USDC Balance: ${usdc_balance:.2f}, Required: ${usdc_needed:.2f}")
        
        if usdc_balance < usdc_needed:
            logger.warning(f"❌ Insufficient USDC balance. Required: ${usdc_needed:.2f}, Available: ${usdc_balance:.2f}")
            return False

    except Exception as e:
        logger.error(f"❌ Failed to check USDC balance: {str(e)}")
        return False

 # Post an order
def post_order(order_args: MarketOrderArgs, order_type: OrderType) -> dict:
    signed_order = client.create_market_order(order_args)
    response = client.post_order(signed_order, order_type)
    return response

# Place an order according to the side and asset
def place_buy_order(asset: str, reason: str, fallback_to_buy: bool = False) -> bool:
    max_retries = 3
    base_delay = 1  # seconds

    for attempt in range(max_retries):
        logger.info(f"🔄 Order attempt {attempt + 1}/{max_retries} for SELL {asset}")
        try:
            current_price = get_current_price(asset)
            min_ask_data = get_min_ask_data(asset)
            min_ask_price = min_ask_data["min_ask_price"]
            min_ask_size = min_ask_data["min_ask_size"]
            if min_ask_price - current_price > SLIPPAGE_TOLERANCE:
                logger.warning(f"🔐 Slippage tolerance exceeded for {asset}. Skipping order.")
                return False
            # If there is not enough bid size, buy as much as max bid size
            elif TRADE_UNIT/min_ask_price > min_ask_size:
                logger.warning(f"😏 Insufficient bid size for min price. Trying to buy {min_ask_size} shares...")
                amount_in_dallor = min_ask_size * min_ask_price
                if not check_usdc_balance(amount_in_dallor) and ensure_usdc_allowance(amount_in_dallor):
                    return False
                else:
                    order_args = MarketOrderArgs(
                        token_id=str(asset),
                        amount=float(amount_in_dallor),
                        side=BUY,
                    )
                    response = post_order(order_args, OrderType.FOK)
                    if response.get("success"):
                        filled = response.get("data", {}).get("filledAmount", amount_in_dallor)
                        logger.info(f"✅ [{reason}] Order placed: BUY {filled:.4f} shares of {asset}")
                        update_recent_trade(asset, "BUY")
                        add_active_trade(asset, {
                            "entry_price": min_ask_price,
                            "entry_time": datetime.now(UTC),
                            "amount": amount_in_dallor,
                            "bot_triggered": True
                        })
                        return True
                    else:
                        logger.error(f"❌ Failed to place BUY order for {asset}: {str(response)}")
                        continue
            # If there is enough bid size, buy as much as possible
            elif TRADE_UNIT/min_ask_price < min_ask_size:
                logger.warning(f"😎 You are lucky. You can buy as much as possible for min price.")
                if not check_usdc_balance(TRADE_UNIT) and ensure_usdc_allowance(TRADE_UNIT):
                    return False
                else:
                    order_args = MarketOrderArgs(
                        token_id=str(asset),
                        amount=float(TRADE_UNIT),
                        side=BUY,
                    )
                    response = post_order(order_args, OrderType.FOK)
                    if response.get("success"):
                        filled = response.get("data", {}).get("filledAmount", TRADE_UNIT)
                        logger.info(f"✅ [{reason}] Order placed: BUY {filled:.4f} shares of {asset}")
                        update_recent_trade(asset,  "BUY")
                        add_active_trade(asset, {
                            "entry_price": min_ask_price,
                            "entry_time": datetime.now(UTC),
                            "amount": TRADE_UNIT,
                            "bot_triggered": True
                        })
                        return True
                    else:
                        logger.error(f"❌ Failed to place BUY order for {asset}: {str(response)}")
                        continue

        except Exception as e:
            logger.error(f"❌ Failed to process BUY order for {asset}: {str(e)}")
            return False
        time.sleep(base_delay)

# Place Sell order
def place_sell_order(asset: str, reason: str, fallback_to_sell: bool = False) -> bool:
    max_retries = 3
    base_delay = 1  # seconds

    for attempt in range(max_retries):
        logger.info(f"🔄 Order attempt {attempt + 1}/{max_retries} for SELL {asset}")
        try:
            current_price = get_current_price(asset)
            max_bid_data = get_max_bid_data(asset)
            max_bid_price = max_bid_data["max_bid_price"]
            max_bid_size = max_bid_data["max_bid_size"]
            # if current_price - max_bid_price > SLIPPAGE_TOLERANCE:
                # return False
            # If there is not enough ask size, sell as much as max ask size
            position = get_global_positions()
            balance = position[asset]["shares"]
            avg_Price = position[asset]["avg_price"]
            sell_amount_in_shares = balance -1
            slippage = current_price - max_bid_price
            if avg_Price > max_bid_price:
                profit_amount = sell_amount_in_shares * (avg_Price - max_bid_price)
                logger.info(f"balance: {balance}, slippage: {slippage}----You will earn ${profit_amount} ")
            else:
                loss_amount = sell_amount_in_shares * (max_bid_price - avg_Price) 
                logger.info(f"balance: {balance}, slippage: {slippage}----You will loss ${loss_amount} ")
            order_args = MarketOrderArgs(
                token_id=str(asset),
                amount=float(sell_amount_in_shares),
                side=SELL,
            )
            response = post_order(order_args, OrderType.FOK)
            if response.get("success"):
                filled = response.get("data", {}).get("filledAmount", sell_amount_in_shares)
                logger.info(f"✅ [{reason}] Order placed: SELL {filled:.4f} shares of {asset}")
                update_recent_trade(asset, "SELL")
                remove_active_trade(asset)
                return True
            else:
                logger.error(f"❌ Failed to place SELL order for {asset}: {str(response)}")
                return False
        except Exception as e:
            logger.error(f"❌ Failed to process SELL order for {asset}: {str(e)}")
            return False
    time.sleep(base_delay)

# def place_order(side: str, asset: str, amount_in_shares: float, reason: str, 
#                 fallback_to_sell: bool = False, fallback_to_buy: bool = False) -> bool:
#     with state._positions_lock:
#         positions = state._positions
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
#                 amount_in_shares = round(TRADE_UNIT / current_price, 4)
#                 usdc_needed = current_price * amount_in_shares
                

#             else:
#                 actual_shares = positions[asset]["shares"]
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
#                     actual_shares = positions[asset]["shares"]
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
def is_recently_bought(state: ThreadSafeState, asset_id: str) -> bool:
    """Check if the asset was recently bought."""
    if asset_id not in state._recent_trades:
        return False
    now = datetime.now(UTC)
    time_since_buy = now - state._recent_trades[asset_id]["buy"]
    return time_since_buy.total_seconds() < 120 and state._recent_trades[asset_id]["buy"] == "buy"

# Check if the asset was recently sold: It's a cooldown to prevent duplicate sells against the same spike
def is_recently_sold(state: ThreadSafeState, asset_id: str) -> bool:
    """Check if the asset was recently sold."""
    if asset_id not in state._recent_trades:
        return False
    now = datetime.now(UTC)
    time_since_sell = now - state._recent_trades[asset_id]["sell"]
    return time_since_sell.total_seconds() < 120 and state._recent_trades[asset_id]["sell"] == "sell"

def find_position_by_asset(positions: dict, asset_id: str) -> Optional[dict]:
    for event_positions in positions.values():
        for position in event_positions:
            if position["asset"] == asset_id:
                return position
    return None

def detect_and_trade(state: ThreadSafeState):
    try:
        while True:
            # Wait for price update to complete
            price_update_event.wait()
            price_update_event.clear()
            
            positions_copy = get_global_positions()  # Get thread-safe copy of positions
            logger.info("🔍 Scanning for spikes...")
            now = datetime.now(UTC)

            # Get all assets from price history
            for asset_id in state._price_history.keys():
                history = state._price_history[asset_id]
                if len(history) < 2:
                    continue

                old_price = history[0][1]
                new_price = history[-1][1]
                eventslug = history[0][2]
                outcome = history[0][3]
                delta = (new_price - old_price) / old_price

                logger.info(f"📈 {outcome} share in {eventslug}: old={old_price:.4f}, new={new_price:.4f}, delta={delta:.2%}, entry = {len(history)}")
                
                if abs(delta) > float(os.getenv("spike_threshold")):
                    if old_price < 0.20 and old_price > 0.90:
                        logger.info(f"🟨 Spike detected on {outcome} share in {eventslug} with delta {delta:.2%}: {old_price:.4f} -> {new_price:.4f} Skipping...")
                        continue

                    logger.info(f"🟨 Spike detected on {outcome} share in {eventslug} with delta {delta:.2%}: {old_price:.4f} -> {new_price:.4f} Entering...")
                    opposite = get_asset_pair(state, asset_id)
                    opposite_position = find_position_by_asset(positions_copy, opposite)
                    if opposite_position:
                        opposite_outcome = opposite_position["outcome"]
                        opposite_eventslug = opposite_position["eventslug"]
                        if opposite_eventslug == eventslug:
                            logger.info(f"🔄 Opposite asset: {opposite_outcome} share in {opposite_eventslug}")
                        
                    if not opposite:
                        logger.warning(f"❌ No opposite found for {outcome} share in {eventslug}. if you don't buy {opposite} share in {opposite_eventslug} in 30min, this position will be sold!")
                        counter += 1
                        if counter >= float(os.getenv("sold_position_time")):
                            logger.info(f"🔴 {outcome} share in {eventslug} has been sold because the opposite asset was not bought in time.")
                            counter = 0
                        continue
                    if delta > 0:
                        if is_recently_bought(state, asset_id):
                            logger.info(f"🔴 {asset_id} was recently bought. Skipping...")
                            continue
                        else:
                            logger.info(f"🔴 {asset_id} was not recently bought. Buying {outcome} share in {eventslug}...")
                            place_buy_order(asset_id, "Take Profit", fallback_to_buy=False)
                            logger.info(f" Selling {opposite} share in {opposite_eventslug}...")
                            place_sell_order(opposite, "Spike detected", fallback_to_sell=False )
                    elif delta < 0:
                        if is_recently_sold(state, asset_id):
                            logger.info(f"🔴 {asset_id} was recently sold. Skipping...")
                            continue
                        else:
                            logger.info(f"🔴 {asset_id} was not recently sold. Selling {outcome} share in {eventslug}...")
                            place_sell_order(asset_id, "Spike detected", fallback_to_sell=False)
                            logger.info(f" Buying {opposite} share in {opposite_eventslug}...")
                            place_buy_order(opposite, "Take Profit", fallback_to_buy=False)

            time.sleep(1)
    except Exception as e:
        logger.error(f"❌ Error in detect_and_trade: {str(e)}")
        time.sleep(1)

# Get current price
def get_current_price(state: ThreadSafeState, asset_id: str) -> Optional[float]:
    try:
        history = get_price_history(state, asset_id)
        if history:
            return history[-1][1]
        else:
            logger.warning(f"⚠️ No price history found for {asset_id}")
    except Exception as e:
        logger.error(f"❌ Error getting current price for {asset_id}: {str(e)}")
    return None

def check_trade_exits():
    while True:
        now = datetime.now(UTC)
        active_trades = get_active_trades()
        logger.info(f"📈 Active trades: {len(active_trades)}")

        for asset_id in list(active_trades.keys()):
            positions_copy = get_global_positions(state)
            with state._active_trades_lock:
                trade = active_trades[asset_id]
            entry_price = trade["entry_price"]
            entry_time = trade["entry_time"]
            amount = trade["amount"]

            history = get_price_history(asset_id)
            if len(history) < 2:
                continue
            
            current_price = get_current_price(asset_id)
            if current_price is None:
                continue
            avg_price = positions_copy[asset_id]["avg_price"]
            remaining_shares = positions_copy[asset_id]["shares"] 
            max_bid_data = get_max_bid_data(asset_id)
            max_bid_size = max_bid_data["max_bid_size"]
            sell_price = max_bid_data["sell_price"]
            slippage = current_price - sell_price
            cash_profit = ( sell_price - avg_price ) * remaining_shares
            pct_profit = (sell_price - avg_price)/avg_price
            if cash_profit >= 2 or pct_profit > os.getenv("take_profit"): 
                place_sell_order(asset_id)
            time_held = now - entry_time

            if trade.get("pending_exit"):
                cooldown = int(os.getenv("cooldown"))
                max_retries = 3
                retry_count = trade.get("retry_count", 0)
                
                if retry_count >= max_retries:
                    logger.error(f"❌ Max retries reached for {asset_id}, removing from active trades")
                    remove_active_trade(asset_id)
                    continue
                    
                if time.time() - trade.get("pending_exit_since", 0) < cooldown:
                    logger.info(f"⏳ Waiting before retrying SELL for {asset_id}")
                    continue
                else:
                    logger.info(f"🔁 Retrying SELL for {asset_id} after cooldown (attempt {retry_count + 1}/{max_retries})")
                    trade["retry_count"] = retry_count + 1
                    add_active_trade(asset_id, trade)

            logger.info(f"🕒 {asset_id} held for {int(time_held.total_seconds() // 60)}m {int(time_held.total_seconds() % 60)}s")
            logger.info(f"💰 PNL Check: entry={entry_price:.4f}, current={current_price:.4f}, profit={profit:.2%}")
            logger.info(f"⏱ Checking exit logic for {asset_id} | Held: {time_held.total_seconds():.2f}s | Profit: {profit:.2%}")
            logger.info(f"⏱ Entry Time: {entry_time}, Now: {now}, Held Seconds: {time_held.total_seconds():.2f}")

            def handle_failed_sell(reason: str, asset_id: str):
                with positions_lock:
                    positions = positions.copy()
                asset_still_held = False
                for sides in positions.values():
                    for pos in sides:
                        if pos["asset"] == asset_id and float(pos["shares"]) > 0.001:
                            asset_still_held = True
                            break
                    if asset_still_held:
                        break

                if asset_still_held:
                    logger.warning(f"⚠️ SELL failed for {reason} — position still active, will retry later")
                    trade["pending_exit"] = True
                    trade["pending_exit_since"] = time.time()
                    add_active_trade(asset_id, trade)
                else:
                    logger.info(f"✅ SELL likely succeeded (or dust) — clearing {asset_id}")
                    remove_active_trade(asset_id)
                    set_last_trade_time(time.time())

            if profit >= float(os.getenv("take_profit")):
                logger.info(f"🎯 TP hit for {asset_id}. Selling...")
                success = place_order("SELL", asset_id, shares, "take_profit", fallback_to_sell=True)
                if success:
                    remove_active_trade(asset_id)
                    set_last_trade_time(time.time())
                    logger.info("✅ Trade closed — ready for next.")
                else:
                    handle_failed_sell("TP", asset_id)

            elif profit <= float(os.getenv("stop_loss")):
                logger.warning(f"🛑 SL hit for {asset_id}. Selling...")
                success = place_order("SELL", asset_id, shares, "stop_loss", fallback_to_sell=True)
                if success:
                    remove_active_trade(asset_id)
                    set_last_trade_time(time.time())
                    logger.info("✅ Trade closed — ready for next.")
                else:
                    handle_failed_sell("SL", asset_id)

            elif time_held >= timedelta(minutes=1):
                logger.info(f"⌛ Time-based exit for {asset_id}. Selling...")
                success = place_order("SELL", asset_id, shares, "timeout_exit", fallback_to_sell=True)
                if success:
                    remove_active_trade(asset_id)
                    set_last_trade_time(time.time())
                    logger.info("✅ Trade closed — ready for next.")
                else:
                    handle_failed_sell("timeout", asset_id)
        time.sleep(1)
# Update price history every 1 second
def update_price_history(state: ThreadSafeState):

    error_count = 0
    max_errors = 5
    while True:
        try:
            with state._positions_lock:
                now = datetime.now(UTC)
                positions = fetch_positions()
                update_global_positions(positions)  # Update global positions
        
            for event_id, assets in positions.items():
                for asset in assets:
                    eventslug = asset["eventslug"]
                    outcome = asset["outcome"]
                    asset_id = asset["asset"]
                    price = asset["current_price"]
                    add_price(state, asset_id, now, price, eventslug, outcome)
                    logger.info(f"💾 Updated price of {outcome} share in {eventslug}: asset_id: {asset_id}: price: ${price:.4f}")
            error_count = 0  # Reset error count on success
            price_update_event.set()  # Signal that price update is complete
            time.sleep(1)
        except Exception as e:
            error_count += 1
            logger.error(f"❌ Error updating price history: {e}")
            if error_count >= max_errors:
                logger.error("❌ Too many errors in price history update. Exiting thread.")
                break
            time.sleep(5)  # Longer sleep on error

# Wait for manual $1 entries on both sides of a market
def wait_for_initialization(state: ThreadSafeState):
    max_retries = 60  # 1 minute total with 2-second sleep
    retry_count = 0
    while retry_count < max_retries:
        try:
            positions = fetch_positions()
            for event_id, sides in positions.items():
                logger.info(f"🔎 Event ID {event_id}: {len(sides)}")
                if len(sides) % 2 == 0 and len(sides) > 1:
                    ids = [s["asset"] for s in sides]
                    add_asset_pair(state, ids[0], ids[1])
                    logger.info(f"✅ Initialized asset pair: {ids[0]} ↔ {ids[1]}")
            
            if is_initialized(state):
                logger.info(f"✅ Initialization complete with {len(state._initialized_assets)} assets.")
                return True
                
            retry_count += 1
            time.sleep(2)
            
        except Exception as e:
            logger.error(f"❌ Error during initialization: {str(e)}")
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
    try:
        state = ThreadSafeState()
        print_spikebot_banner()
        spinner = Halo(text="Waiting for manual $1 entries on both sides of a market...", spinner="dots")
        spinner.start()
        time.sleep(5)
        logger.info(f"🚀 Spike-detection bot started at {datetime.now(UTC)}")
        
        if not wait_for_initialization(state):
            spinner.fail("❌ Failed to initialize. Exiting.")
            exit(1)
        else:
            spinner.succeed("Initialized successfully")
        
        price_update_thread = threading.Thread(target=update_price_history, args=(state,), daemon=True)
        detect_and_trade_thread = threading.Thread(target=detect_and_trade, args=(state,), daemon=True)
        check_trade_exits_thread = threading.Thread(target=check_trade_exits, daemon=True)
    
        price_update_thread.start()
        detect_and_trade_thread.start()
        check_trade_exits_thread.start()

        last_refresh_time = time.time()
        refresh_interval = 3600
    
        while True:
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
    finally:
        logger.info("✅ Cleanup complete")