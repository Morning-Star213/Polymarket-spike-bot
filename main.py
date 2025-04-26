import json
import os
import time
import requests
import threading
import logging
import colorlog
from halo import Halo
from datetime import datetime, timedelta, UTC
from dotenv import load_dotenv
from web3 import Web3
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import MarketOrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY, SELL
from typing import Dict, List, Tuple, Optional, Any
from collections import deque, defaultdict
from threading import Lock, Event
from concurrent.futures import ThreadPoolExecutor
import signal
import sys

# Load and validate environment variables
load_dotenv(".env")

# Configuration validation
def validate_config() -> None:
    required_vars = {
        "trade_unit": float,
        "cooldown_time": float,
        "slippage_tolerance": float,
        "pct_profit": float,
        "pct_loss": float,
        "cash_profit": float,
        "cash_loss": float,
        "spike_threshold": float,
        "sold_position_time": float,
        "YOUR_PROXY_WALLET": str,
        "BOT_TRADER_ADDRESS": str,
        "USDC_CONTRACT_ADDRESS": str,
        "POLYMARKET_SETTLEMENT_CONTRACT": str,
        "PK": str
    }
    
    missing = []
    invalid = []
    
    for var, var_type in required_vars.items():
        value = os.getenv(var)
        if not value:
            missing.append(var)
            continue
        try:
            if var_type == float:
                float(value)
            elif var_type == str:
                str(value)
        except ValueError:
            invalid.append(var)
    
    if missing or invalid:
        error_msg = []
        if missing:
            error_msg.append(f"Missing variables: {', '.join(missing)}")
        if invalid:
            error_msg.append(f"Invalid values for: {', '.join(invalid)}")
        raise ValueError(" | ".join(error_msg))

# Setup logging
def setup_logging() -> logging.Logger:
    file_handler = logging.FileHandler('polymarket_spike_bot.log', mode='a', encoding='utf-8')
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    
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
    return logger

logger = setup_logging()

# Add threading event for price updates
price_update_event = threading.Event()

class ThreadSafeState:
    def __init__(self, max_price_history_size: int = 120):
        self._price_history_lock = Lock()
        self._active_trades_lock = Lock()
        self._positions_lock = Lock()
        self._asset_pairs_lock = Lock()
        self._recent_trades_lock = Lock()
        self._last_trade_closed_at_lock = Lock()
        self._initialized_assets_lock = Lock()
        self._last_spike_asset_lock = Lock()
        self._last_spike_price_lock = Lock()
        self._counter_lock = Lock()
        self._max_price_history_size = max_price_history_size
        
        self._price_history = defaultdict(lambda: deque(maxlen=max_price_history_size))
        self._active_trades = {}
        self._positions = {}
        self._asset_pairs = {}
        self._recent_trades = {}
        self._last_trade_closed_at = 0
        self._initialized_assets = set()
        self._last_spike_asset = None
        self._last_spike_price = None
        self._counter = 0
        self._shutdown_event = Event()

    def increment_counter(self) -> int:
        with self._counter_lock:
            self._counter += 1
            return self._counter

    def reset_counter(self) -> None:
        with self._counter_lock:
            self._counter = 0

    def get_counter(self) -> int:
        with self._counter_lock:
            return self._counter

    def shutdown(self) -> None:
        self._shutdown_event.set()

    def is_shutdown(self) -> bool:
        return self._shutdown_event.is_set()

# Global configuration
validate_config()

TRADE_UNIT = float(os.getenv("trade_unit"))
COOLDOWN_TIME = float(os.getenv("cooldown_time"))
SLIPPAGE_TOLERANCE = float(os.getenv("slippage_tolerance"))
PCT_PROFIT = float(os.getenv("pct_profit"))
PCT_LOSS = float(os.getenv("pct_loss"))
CASH_PROFIT = float(os.getenv("cash_profit"))
CASH_LOSS = float(os.getenv("cash_loss"))
SPIKE_THRESHOLD = float(os.getenv("spike_threshold"))
SOLD_POSITION_TIME = float(os.getenv("sold_position_time"))

# Web3 and API setup
WEB3_PROVIDER = "https://polygon-rpc.com"
YOUR_PROXY_WALLET = Web3.to_checksum_address(os.getenv("YOUR_PROXY_WALLET"))
BOT_TRADER_ADDRESS = Web3.to_checksum_address(os.getenv("BOT_TRADER_ADDRESS"))
USDC_CONTRACT_ADDRESS = os.getenv("USDC_CONTRACT_ADDRESS")
POLYMARKET_SETTLEMENT_CONTRACT = os.getenv("POLYMARKET_SETTLEMENT_CONTRACT")
PRIVATE_KEY = os.getenv("PK")

web3 = Web3(Web3.HTTPProvider(WEB3_PROVIDER))

# Initialize ClobClient with retry mechanism
def initialize_clob_client(max_retries: int = 3) -> ClobClient:
    for attempt in range(max_retries):
        try:
            client = ClobClient(
                host="https://clob.polymarket.com",
                key=PRIVATE_KEY,
                chain_id=137,
                signature_type=1,
                funder=YOUR_PROXY_WALLET
            )
            api_creds = client.create_or_derive_api_creds()
            client.set_api_creds(api_creds)
            return client
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            logger.warning(f"Failed to initialize ClobClient (attempt {attempt + 1}/{max_retries}): {e}")
            time.sleep(2 ** attempt)
    raise RuntimeError("Failed to initialize ClobClient after maximum retries")

client = initialize_clob_client()

# Thread-safe state management functions
def update_global_positions(state: ThreadSafeState, new_positions: dict) -> None:
    with state._positions_lock:
        state._positions = new_positions

def get_global_positions(state: ThreadSafeState) -> dict:
    with state._positions_lock:
        return state._positions.copy()

def get_price_history(state: ThreadSafeState, asset_id: str) -> deque:
    with state._price_history_lock:
        return state._price_history.get(asset_id, deque())

def add_price(state: ThreadSafeState, asset_id: str, timestamp: datetime, price: float, eventslug: str, outcome: str) -> None:
    with state._price_history_lock:
        if not isinstance(asset_id, str):
            logger.error(f"Invalid asset_id type: {type(asset_id)}")
            return
        if asset_id not in state._price_history:
            state._price_history[asset_id] = deque(maxlen=state._max_price_history_size)
        state._price_history[asset_id].append((timestamp, price, eventslug, outcome))

def get_active_trades(state: ThreadSafeState) -> Dict[str, dict]:
    with state._active_trades_lock:
        return dict(state._active_trades)

def add_active_trade(state: ThreadSafeState, asset_id: str, trade_data: dict) -> None:
    with state._active_trades_lock:
        state._active_trades[asset_id] = trade_data

def remove_active_trade(state: ThreadSafeState, asset_id: str) -> None:
    with state._active_trades_lock:
        state._active_trades.pop(asset_id, None)

def update_recent_trade(state: ThreadSafeState, asset_id: str, trade_type: str) -> None:
    with state._recent_trades_lock:
        if asset_id not in state._recent_trades:
            state._recent_trades[asset_id] = {"buy": None, "sell": None}
        state._recent_trades[asset_id][trade_type] = time.time()

def get_last_trade_time(state: ThreadSafeState) -> float:
    with state._last_trade_closed_at_lock:
        return state._last_trade_closed_at

def set_last_trade_time(state: ThreadSafeState, timestamp: float) -> None:
    with state._last_trade_closed_at_lock:
        state._last_trade_closed_at = timestamp

def get_asset_pair(state: ThreadSafeState, asset_id: str) -> Optional[str]:
    with state._asset_pairs_lock:
        return state._asset_pairs.get(asset_id)

def add_asset_pair(state: ThreadSafeState, asset1: str, asset2: str) -> None:
    with state._asset_pairs_lock:
        state._asset_pairs[asset1] = asset2
        state._asset_pairs[asset2] = asset1
        state._initialized_assets.add(asset1)
        state._initialized_assets.add(asset2)

def is_initialized(state: ThreadSafeState) -> bool:
    with state._initialized_assets_lock:
        return len(state._initialized_assets) > 0

# API functions with retry mechanism
def fetch_positions_with_retry(max_retries: int = 3) -> dict:
    for attempt in range(max_retries):
        try:
            url = f"https://data-api.polymarket.com/positions?user={YOUR_PROXY_WALLET}"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if not isinstance(data, list):
                return {}
                
            positions = {}
            for pos in data:
                event_id = pos.get("conditionId") or pos.get("eventId") or pos.get("marketId")
                if not event_id:
                    continue
                    
                if event_id not in positions:
                    positions[event_id] = []
                    
                positions[event_id].append({
                    "eventslug": pos.get("eventSlug"),
                    "outcome": pos.get("outcome"),
                    "asset": pos.get("asset"),
                    "avg_price": pos.get("avgPrice"),
                    "shares": pos.get("size"),
                    "current_price": pos.get("curPrice"),
                    "initial_value": pos.get("initialValue"),
                    "current_value": pos.get("currentValue"),
                    "pnl": pos.get("cashPnl"),
                    "percent_pnl": pos.get("percentPnl"),
                    "realized_pnl": pos.get("realizedPnl")
                })
                
            return positions
            
        except (requests.RequestException, ValueError) as e:
            if attempt == max_retries - 1:
                logger.error(f"Failed to fetch positions after {max_retries} attempts: {e}")
                return {}
            logger.warning(f"Failed to fetch positions (attempt {attempt + 1}/{max_retries}): {e}")
            time.sleep(2 ** attempt)
    return {}

def ensure_usdc_allowance(required_amount: float) -> bool:
    max_retries = 3
    base_delay = 1
    
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

            current_allowance = contract.functions.allowance(BOT_TRADER_ADDRESS, POLYMARKET_SETTLEMENT_CONTRACT).call()
            required_amount_with_buffer = int(required_amount * 1.1 * 10**6)
            
            if current_allowance >= required_amount_with_buffer:
                return True

            logger.info(f"🔄 Approving USDC allowance... (attempt {attempt + 1}/{max_retries})")
            
            new_allowance = max(current_allowance, required_amount_with_buffer)
            
            txn = contract.functions.approve(POLYMARKET_SETTLEMENT_CONTRACT, new_allowance).build_transaction({
                "from": BOT_TRADER_ADDRESS,
                "gas": 200000,
                "gasPrice": web3.eth.gas_price,
                "nonce": web3.eth.get_transaction_count(BOT_TRADER_ADDRESS),
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
            delay = base_delay * (2 ** attempt)
            logger.info(f"⏳ Waiting {delay}s before retry...")
            time.sleep(delay)
    
    return False

def refresh_api_credentials() -> bool:
    try:
        global client
        api_creds = client.create_or_derive_api_creds()
        client.set_api_creds(api_creds)
        logger.info("✅ API credentials refreshed successfully")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to refresh API credentials: {str(e)}")
        return False

def get_min_ask_data(asset: str) -> Optional[Dict[str, Any]]:
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
        logger.error(f"❌ Failed to get ask data for {asset}: {str(e)}")
        return None

def get_max_bid_data(asset: str) -> Optional[Dict[str, Any]]:
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

def check_usdc_balance(usdc_needed: float) -> bool:
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
        return True

    except Exception as e:
        logger.error(f"❌ Failed to check USDC balance: {str(e)}")
        return False

def post_order(order_args: MarketOrderArgs, order_type: OrderType) -> dict:
    try:
        signed_order = client.create_market_order(order_args)
        response = client.post_order(signed_order, order_type)
        return response
    except Exception as e:
        logger.error(f"❌ Failed to post order: {str(e)}")
        return {"success": False, "error": str(e)}

def place_buy_order(state: ThreadSafeState, asset: str, reason: str, fallback_to_buy: bool = False) -> bool:
    max_retries = 3
    base_delay = 1

    for attempt in range(max_retries):
        logger.info(f"🔄 Order attempt {attempt + 1}/{max_retries} for BUY {asset}")
        try:
            current_price = get_current_price(state, asset)
            if current_price is None:
                continue

            min_ask_data = get_min_ask_data(asset)
            if min_ask_data is None:
                continue

            min_ask_price = min_ask_data["min_ask_price"]
            min_ask_size = min_ask_data["min_ask_size"]

            if min_ask_price - current_price > SLIPPAGE_TOLERANCE:
                logger.warning(f"🔐 Slippage tolerance exceeded for {asset}. Skipping order.")
                return False

            amount_in_dollars = min(TRADE_UNIT, min_ask_size * min_ask_price)
            
            if not check_usdc_balance(amount_in_dollars) or not ensure_usdc_allowance(amount_in_dollars):
                return False

            order_args = MarketOrderArgs(
                token_id=str(asset),
                amount=float(amount_in_dollars),
                side=BUY,
            )
            
            response = post_order(order_args, OrderType.FOK)
            if response.get("success"):
                filled = response.get("data", {}).get("filledAmount", amount_in_dollars)
                logger.info(f"✅ [{reason}] Order placed: BUY {filled:.4f} shares of {asset}")
                update_recent_trade(state, asset, "buy")
                add_active_trade(state, asset, {
                    "entry_price": min_ask_price,
                    "entry_time": datetime.now(UTC),
                    "amount": amount_in_dollars,
                    "bot_triggered": True
                })
                return True
            else:
                logger.error(f"❌ Failed to place BUY order for {asset}: {str(response)}")
                continue

        except Exception as e:
            logger.error(f"❌ Failed to process BUY order for {asset}: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(base_delay * (2 ** attempt))
            continue

    return False

def place_sell_order(state: ThreadSafeState, asset: str, reason: str, fallback_to_sell: bool = False) -> bool:
    max_retries = 3
    base_delay = 1

    for attempt in range(max_retries):
        logger.info(f"🔄 Order attempt {attempt + 1}/{max_retries} for SELL {asset}")
        try:
            current_price = get_current_price(state, asset)
            if current_price is None:
                continue

            max_bid_data = get_max_bid_data(asset)
            if max_bid_data is None:
                continue

            max_bid_price = max_bid_data["max_bid_price"]
            max_bid_size = max_bid_data["max_bid_size"]

            positions = get_global_positions(state)
            if asset not in positions:
                logger.warning(f"❌ No position found for {asset}")
                return False

            position = positions[asset]
            balance = position["shares"]
            avg_price = position["avg_price"]
            sell_amount_in_shares = balance - 1

            slippage = current_price - max_bid_price
            if avg_price > max_bid_price:
                profit_amount = sell_amount_in_shares * (avg_price - max_bid_price)
                logger.info(f"balance: {balance}, slippage: {slippage}----You will earn ${profit_amount}")
            else:
                loss_amount = sell_amount_in_shares * (max_bid_price - avg_price)
                logger.info(f"balance: {balance}, slippage: {slippage}----You will lose ${loss_amount}")

            order_args = MarketOrderArgs(
                token_id=str(asset),
                amount=float(sell_amount_in_shares),
                side=SELL,
            )

            response = post_order(order_args, OrderType.FOK)
            if response.get("success"):
                filled = response.get("data", {}).get("filledAmount", sell_amount_in_shares)
                logger.info(f"✅ [{reason}] Order placed: SELL {filled:.4f} shares of {asset}")
                update_recent_trade(state, asset, "sell")
                remove_active_trade(state, asset)
                return True
            else:
                logger.error(f"❌ Failed to place SELL order for {asset}: {str(response)}")
                return False

        except Exception as e:
            logger.error(f"❌ Failed to process SELL order for {asset}: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(base_delay * (2 ** attempt))
            continue

    return False

def is_recently_bought(state: ThreadSafeState, asset_id: str) -> bool:
    with state._recent_trades_lock:
        if asset_id not in state._recent_trades or state._recent_trades[asset_id]["buy"] is None:
            return False
        now = time.time()
        time_since_buy = now - state._recent_trades[asset_id]["buy"]
        return time_since_buy < 120

def is_recently_sold(state: ThreadSafeState, asset_id: str) -> bool:
    with state._recent_trades_lock:
        if asset_id not in state._recent_trades or state._recent_trades[asset_id]["sell"] is None:
            return False
        now = time.time()
        time_since_sell = now - state._recent_trades[asset_id]["sell"]
        return time_since_sell < 120

def find_position_by_asset(positions: dict, asset_id: str) -> Optional[dict]:
    for event_positions in positions.values():
        for position in event_positions:
            if position["asset"] == asset_id:
                return position
    return None

def detect_and_trade(state: ThreadSafeState) -> None:
    try:
        while not state.is_shutdown():
            price_update_event.wait()
            price_update_event.clear()
            
            positions_copy = get_global_positions(state)
            logger.info("🔍 Scanning for spikes...")
            now = datetime.now(UTC)

            for asset_id in list(state._price_history.keys()):
                history = get_price_history(state, asset_id)
                if len(history) < 2:
                    continue

                old_price = history[0][1]
                new_price = history[-1][1]
                eventslug = history[0][2]
                outcome = history[0][3]
                delta = (new_price - old_price) / old_price

                logger.info(f"📈 {outcome} share in {eventslug}: old={old_price:.4f}, new={new_price:.4f}, delta={delta:.2%}, entry = {len(history)}")
                
                if abs(delta) > SPIKE_THRESHOLD:
                    if old_price < 0.20 or old_price > 0.90:
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
                        counter = state.increment_counter()
                        if counter >= SOLD_POSITION_TIME:
                            logger.info(f"🔴 {outcome} share in {eventslug} has been sold because the opposite asset was not bought in time.")
                            state.reset_counter()
                        continue

                    if delta > 0:
                        if is_recently_bought(state, asset_id):
                            logger.info(f"🔴 {asset_id} was recently bought. Skipping...")
                            continue
                        else:
                            logger.info(f"🔴 {asset_id} was not recently bought. Buying {outcome} share in {eventslug}...")
                            if place_buy_order(state, asset_id, "Take Profit", fallback_to_buy=False):
                                logger.info(f" Selling {opposite} share in {opposite_eventslug}...")
                                place_sell_order(state, opposite, "Spike detected", fallback_to_sell=False)
                    elif delta < 0:
                        if is_recently_sold(state, asset_id):
                            logger.info(f"🔴 {asset_id} was recently sold. Skipping...")
                            continue
                        else:
                            logger.info(f"🔴 {asset_id} was not recently sold. Selling {outcome} share in {eventslug}...")
                            if place_sell_order(state, asset_id, "Spike detected", fallback_to_sell=False):
                                logger.info(f" Buying {opposite} share in {opposite_eventslug}...")
                                place_buy_order(state, opposite, "Take Profit", fallback_to_buy=False)

            time.sleep(1)
    except Exception as e:
        logger.error(f"❌ Error in detect_and_trade: {str(e)}")
        time.sleep(1)

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

def check_trade_exits(state: ThreadSafeState) -> None:
    while not state.is_shutdown():
        try:
            now = datetime.now(UTC)
            active_trades = get_active_trades(state)
            logger.info(f"📈 Active trades: {len(active_trades)}")

            for asset_id in list(active_trades.keys()):
                positions_copy = get_global_positions(state)
                trade = active_trades[asset_id]
                entry_price = trade["entry_price"]
                entry_time = trade["entry_time"]
                latest_traded_time = get_last_trade_time(state)

                if now.timestamp() - latest_traded_time > COOLDOWN_TIME:
                    continue

                history = get_price_history(state, asset_id)
                if len(history) < 2:
                    continue
                
                current_price = get_current_price(state, asset_id)
                if current_price is None:
                    continue

                if asset_id not in positions_copy:
                    continue

                position = positions_copy[asset_id]
                avg_price = position["avg_price"]
                remaining_shares = position["shares"]
                
                max_bid_data = get_max_bid_data(asset_id)
                if max_bid_data is None:
                    continue

                sell_price = max_bid_data["sell_price"]
                cash_profit = (sell_price - avg_price) * (remaining_shares - 1)
                pct_profit = (sell_price - avg_price) / avg_price
                cash_loss = (current_price - avg_price) * (remaining_shares - 1)
                pct_loss = (current_price - avg_price) / avg_price
                holding_time = now - entry_time

                if holding_time.total_seconds() > COOLDOWN_TIME:
                    if cash_profit > 0:
                        logger.info(f"⏱ Entry Time: {entry_time}, Now: {now}, Held Seconds: {holding_time.total_seconds():.2f}, profitable:{cash_profit}")
                        place_sell_order(state, asset_id, "Exceed holding time", fallback_to_sell=False)
                        remove_active_trade(state, asset_id)
                        set_last_trade_time(state, time.time())
                    else:
                        logger.info(f"⏱ Entry Time: {entry_time}, Now: {now}, Held Seconds: {holding_time.total_seconds():.2f}, lossy:{cash_profit}")
                        place_sell_order(state, asset_id, "Exceed holding time", fallback_to_sell=False)
                        remove_active_trade(state, asset_id)
                        set_last_trade_time(state, time.time())

                elif cash_profit >= CASH_PROFIT or pct_profit > PCT_PROFIT:
                    logger.info(f"🎯 TP hit for {asset_id}. Selling...")
                    logger.info(f"💰 PNL Check: entry={sell_price:.4f}, current={current_price:.4f}, profit={cash_profit}: {pct_profit:.2%}")
                    place_sell_order(state, asset_id, "take_profit", fallback_to_sell=False)
                    remove_active_trade(state, asset_id)
                    set_last_trade_time(state, time.time())

                elif cash_loss < CASH_LOSS or pct_loss < PCT_LOSS:
                    logger.warning(f"🛑 SL hit for {asset_id}. Selling...")
                    logger.info(f"💰 PNL Check: entry={sell_price:.4f}, current={current_price:.4f}, loss={cash_profit}: {pct_profit:.2%}")
                    place_sell_order(state, asset_id, "Stop Loss", fallback_to_sell=False)
                    remove_active_trade(state, asset_id)
                    set_last_trade_time(state, time.time())

            time.sleep(1)
        except Exception as e:
            logger.error(f"❌ Error in check_trade_exits: {str(e)}")
            time.sleep(1)

def update_price_history(state: ThreadSafeState) -> None:
    error_count = 0
    max_errors = 5
    while not state.is_shutdown():
        try:
            with state._positions_lock:
                now = datetime.now(UTC)
                positions = fetch_positions_with_retry()
                update_global_positions(state, positions)
        
            for event_id, assets in positions.items():
                for asset in assets:
                    eventslug = asset["eventslug"]
                    outcome = asset["outcome"]
                    asset_id = asset["asset"]
                    price = asset["current_price"]
                    add_price(state, asset_id, now, price, eventslug, outcome)
                    logger.info(f"💾 Updated price of {outcome} share in {eventslug}: asset_id: {asset_id}: price: ${price:.4f}")
            
            error_count = 0
            price_update_event.set()
            time.sleep(1)
        except Exception as e:
            error_count += 1
            logger.error(f"❌ Error updating price history: {e}")
            if error_count >= max_errors:
                logger.error("❌ Too many errors in price history update. Exiting thread.")
                break
            time.sleep(5)

def wait_for_initialization(state: ThreadSafeState) -> bool:
    max_retries = 60
    retry_count = 0
    while retry_count < max_retries and not state.is_shutdown():
        try:
            positions = fetch_positions_with_retry()
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

def print_spikebot_banner() -> None:
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

def cleanup(state: ThreadSafeState) -> None:
    """Cleanup function to properly shut down the bot"""
    logger.info("🔄 Starting cleanup...")
    state.shutdown()
    
    # Wait for threads to finish
    for thread in threading.enumerate():
        if thread != threading.current_thread():
            thread.join(timeout=5)
    
    # Close any open connections
    try:
        client.close()
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
    
    logger.info("✅ Cleanup complete")

def signal_handler(signum, frame, state: ThreadSafeState) -> None:
    """Handle shutdown signals"""
    logger.info(f"Received signal {signum}. Initiating shutdown...")
    cleanup(state)
    sys.exit(0)

if __name__ == "__main__":
    try:
        state = ThreadSafeState()
        print_spikebot_banner()
        
        # Set up signal handlers
        signal.signal(signal.SIGINT, lambda s, f: signal_handler(s, f, state))
        signal.signal(signal.SIGTERM, lambda s, f: signal_handler(s, f, state))
        
        spinner = Halo(text="Waiting for manual $1 entries on both sides of a market...", spinner="dots")
        spinner.start()
        time.sleep(5)
        logger.info(f"🚀 Spike-detection bot started at {datetime.now(UTC)}")
        
        if not wait_for_initialization(state):
            spinner.fail("❌ Failed to initialize. Exiting.")
            cleanup(state)
            exit(1)
        else:
            spinner.succeed("Initialized successfully")
        
        # Create and start threads
        price_update_thread = threading.Thread(target=update_price_history, args=(state,), daemon=True)
        detect_and_trade_thread = threading.Thread(target=detect_and_trade, args=(state,), daemon=True)
        check_trade_exits_thread = threading.Thread(target=check_trade_exits, args=(state,), daemon=True)
        
        price_update_thread.start()
        detect_and_trade_thread.start()
        check_trade_exits_thread.start()
        
        last_refresh_time = time.time()
        refresh_interval = 3600
        
        # Main loop
        while not state.is_shutdown():
            current_time = time.time()
            
            if current_time - last_refresh_time > refresh_interval:
                if refresh_api_credentials():
                    last_refresh_time = current_time
                else:
                    logger.warning("⚠️ Failed to refresh API credentials. Will retry in 5 minutes.")
                    time.sleep(300)
                    continue
            
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("👋 Shutting down gracefully...")
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
    finally:
        cleanup(state)