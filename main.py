import json
import os
import time
import requests
import threading
import logging
import logging.handlers
import colorlog
from halo import Halo
from datetime import datetime, timedelta
from dotenv import load_dotenv
from web3 import Web3
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import MarketOrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY, SELL
from typing import Dict, List, Tuple, Optional, Any, Union
from collections import deque, defaultdict
from threading import Lock, Event
from concurrent.futures import ThreadPoolExecutor
import signal
import sys
from dataclasses import dataclass
from enum import Enum
import functools
from queue import Queue

# Custom Exceptions
class BotError(Exception):
    pass

class ConfigurationError(BotError):
    pass

class NetworkError(BotError):
    pass

class TradingError(BotError):
    pass

class ValidationError(BotError):
    pass

# Custom Types
@dataclass
class TradeInfo:
    entry_price: float
    entry_time: float
    amount: float
    bot_triggered: bool

@dataclass
class PositionInfo:
    eventslug: str
    outcome: str
    asset: str
    avg_price: float
    shares: float
    current_price: float
    initial_value: float
    current_value: float
    pnl: float
    percent_pnl: float
    realized_pnl: float

class TradeType(Enum):
    BUY = "buy"
    SELL = "sell"

# Constants
MAX_RETRIES = 3                 # Number of retries for API calls
BASE_DELAY = 1                  # Base delay for retries
MAX_ERRORS = 5                  # Maximum number of errors before shutting down
API_TIMEOUT = 10                # Timeout for API requests
REFRESH_INTERVAL = 3600         # Refresh interval for API credentials
COOLDOWN_PERIOD = 30            # Cooldown period for trades
THREAD_POOL_SIZE = 3            # Number of threads in the thread pool 
MAX_QUEUE_SIZE = 1000           # Maximum number of items in the queue
THREAD_CHECK_INTERVAL = 5       # Interval for checking thread status
THREAD_RESTART_DELAY = 2        # Delay before restarting a thread

# Load and validate environment variables
load_dotenv(".env")

# Configuration validation
def validate_config() -> None:
    required_vars = {
        "trade_unit": float,
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
        "PK": str,
        "holding_time_limit": float,
        "max_concurrent_trades": int,
        "min_liquidity_requirement": float
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

# Global configuration
validate_config()

TRADE_UNIT = float(os.getenv("trade_unit"))
SLIPPAGE_TOLERANCE = float(os.getenv("slippage_tolerance"))
PCT_PROFIT = float(os.getenv("pct_profit"))
PCT_LOSS = float(os.getenv("pct_loss"))
CASH_PROFIT = float(os.getenv("cash_profit"))
CASH_LOSS = float(os.getenv("cash_loss"))
SPIKE_THRESHOLD = float(os.getenv("spike_threshold"))
SOLD_POSITION_TIME = float(os.getenv("sold_position_time"))
HOLDING_TIME_LIMIT = float(os.getenv("holding_time_limit"))
PRICE_HISTORY_SIZE = int(os.getenv("price_history_size"))
COOLDOWN_PERIOD = int(os.getenv("cooldown_period"))
KEEP_MIN_SHARES = int(os.getenv("keep_min_shares"))
MAX_CONCURRENT_TRADES = int(os.getenv("max_concurrent_trades"))
MIN_LIQUIDITY_REQUIREMENT = float(os.getenv("min_liquidity_requirement"))
# Web3 and API setup
WEB3_PROVIDER = "https://polygon-rpc.com"
YOUR_PROXY_WALLET = Web3.to_checksum_address(os.getenv("YOUR_PROXY_WALLET"))
BOT_TRADER_ADDRESS = Web3.to_checksum_address(os.getenv("BOT_TRADER_ADDRESS"))
USDC_CONTRACT_ADDRESS = os.getenv("USDC_CONTRACT_ADDRESS")
POLYMARKET_SETTLEMENT_CONTRACT = os.getenv("POLYMARKET_SETTLEMENT_CONTRACT")
PRIVATE_KEY = os.getenv("PK")

web3 = Web3(Web3.HTTPProvider(WEB3_PROVIDER))
# Setup logging
def setup_logging() -> logging.Logger:
    """Setup enhanced logging configuration with both file and console handlers"""
    # Create logs directory if it doesn't exist
    os.makedirs('logs', exist_ok=True)
    
    # Create a logger
    logger = logging.getLogger('polymarket_bot')
    logger.setLevel(logging.INFO)
    
    # Clear any existing handlers
    logger.handlers = []
    
    # Create formatters
    file_formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(threadName)-12s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    console_formatter = colorlog.ColoredFormatter(
        '%(log_color)s%(asctime)s | %(levelname)-8s | %(threadName)-12s | %(name)s | %(message)s%(reset)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        log_colors={
            'DEBUG': 'cyan',
            'INFO': 'green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'red,bg_white'
        }
    )
    
    # File handler - Rotating file handler with size limit
    file_handler = logging.handlers.RotatingFileHandler(
        'logs/polymarket_bot.log',
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(logging.INFO)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(logging.INFO)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

# Initialize logger
logger = setup_logging()

# Add logging decorator for function entry/exit
def log_function_call(logger: logging.Logger):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            func_name = func.__name__
            logger.debug(f"Entering {func_name}")
            try:
                result = func(*args, **kwargs)
                logger.debug(f"Exiting {func_name} successfully")
                return result
            except Exception as e:
                logger.error(f"Error in {func_name}: {str(e)}", exc_info=True)
                raise
        return wrapper
    return decorator

# Add logging context manager
class LoggingContext:
    def __init__(self, logger, level=None, handler=None, close=True):
        self.logger = logger
        self.level = level
        self.handler = handler
        self.close = close

    def __enter__(self):
        if self.level is not None:
            self.old_level = self.logger.level
            self.logger.setLevel(self.level)
        if self.handler:
            self.logger.addHandler(self.handler)

    def __exit__(self, et, ev, tb):
        if self.level is not None:
            self.logger.setLevel(self.old_level)
        if self.handler:
            self.logger.removeHandler(self.handler)
        if self.handler and self.close:
            self.handler.close()

# Add threading event for price updates
price_update_event = threading.Event()

class ThreadSafeState:
    def __init__(self, max_price_history_size: int = PRICE_HISTORY_SIZE, keep_min_shares: int = KEEP_MIN_SHARES):
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
        self._shutdown_event = Event()
        self._cleanup_complete = Event()
        self._circuit_breaker_lock = Lock()
        # self._daily_pnl = 0.0
        self._max_daily_loss = -100.0  # Maximum daily loss in USDC
        self._max_drawdown = -200.0    # Maximum drawdown in USDC
        # self._trading_enabled = True
        self._max_price_history_size = max_price_history_size
        
        self._price_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=max_price_history_size))
        self._active_trades: Dict[str, TradeInfo] = {}
        self._positions: Dict[str, List[PositionInfo]] = {}
        self._asset_pairs: Dict[str, str] = {}
        self._recent_trades: Dict[str, Dict[str, Optional[float]]] = {}
        self._last_trade_closed_at: float = 0
        self._initialized_assets: set = set()
        self._last_spike_asset: Optional[str] = None
        self._last_spike_price: Optional[float] = None
        self._counter: int = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()

    def cleanup(self) -> None:
        if not self._cleanup_complete.is_set():
            self.shutdown()
            with self._price_history_lock:
                self._price_history.clear()
            with self._active_trades_lock:
                self._active_trades.clear()
            with self._positions_lock:
                self._positions.clear()
            with self._asset_pairs_lock:
                self._asset_pairs.clear()
            with self._recent_trades_lock:
                self._recent_trades.clear()
            self._cleanup_complete.set()

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

    def wait_for_cleanup(self, timeout: Optional[float] = None) -> bool:
        return self._cleanup_complete.wait(timeout)

    def get_price_history(self, asset_id: str) -> deque:
        with self._price_history_lock:
            return self._price_history.get(asset_id, deque())

    def add_price(self, asset_id: str, timestamp: float, price: float, eventslug: str, outcome: str) -> None:
        with self._price_history_lock:
            if not isinstance(asset_id, str):
                raise ValidationError(f"Invalid asset_id type: {type(asset_id)}")
            if asset_id not in self._price_history:
                self._price_history[asset_id] = deque(maxlen=self._max_price_history_size)
            self._price_history[asset_id].append((timestamp, price, eventslug, outcome))

    def get_active_trades(self) -> Dict[str, TradeInfo]:
        with self._active_trades_lock:
            return dict(self._active_trades)

    def add_active_trade(self, asset_id: str, trade_info: TradeInfo) -> None:
        with self._active_trades_lock:
            self._active_trades[asset_id] = trade_info

    def remove_active_trade(self, asset_id: str) -> None:
        with self._active_trades_lock:
            self._active_trades.pop(asset_id, None)

    def get_positions(self) -> Dict[str, List[PositionInfo]]:
        with self._positions_lock:
            return dict(self._positions)

    def update_positions(self, new_positions: Dict[str, List[PositionInfo]]) -> None:
        """Update positions with proper validation and error handling"""
        if new_positions is None:
            logger.warning("⚠️ Attempted to update positions with None")
            return
        
        if not isinstance(new_positions, dict):
            logger.error(f"❌ Invalid positions type: {type(new_positions)}")
            return
        
        try:
            with self._positions_lock:
                # Validate each position before updating
                valid_positions = {}
                for event_id, positions in new_positions.items():
                    if not isinstance(positions, list):
                        logger.warning(f"⚠️ Invalid positions list for event {event_id}")
                        continue
                        
                    valid_positions[event_id] = []
                    for pos in positions:
                        if not isinstance(pos, PositionInfo):
                            logger.warning(f"⚠️ Invalid position type for event {event_id}")
                            continue
                            
                        # Validate position data
                        if not pos.asset or not pos.eventslug or not pos.outcome:
                            logger.warning(f"⚠️ Missing required fields in position for event {event_id}")
                            continue
                            
                        if pos.shares < 0 or pos.avg_price < 0 or pos.current_price < 0:
                            logger.warning(f"⚠️ Invalid numeric values in position for event {event_id}")
                            continue
                            
                        valid_positions[event_id].append(pos)
                
                # Only update if we have valid positions
                if valid_positions:
                    self._positions = valid_positions
                    logger.info(f"✅ Updated positions: {len(valid_positions)} events")
                else:
                    logger.warning("⚠️ No valid positions to update")
                
        except Exception as e:
            logger.error(f"❌ Error updating positions: {str(e)}")
            # Keep old positions if update fails
            return

    def get_asset_pair(self, asset_id: str) -> Optional[str]:
        with self._asset_pairs_lock:
            return self._asset_pairs.get(asset_id)

    def add_asset_pair(self, asset1: str, asset2: str) -> None:
        with self._asset_pairs_lock:
            self._asset_pairs[asset1] = asset2
            self._asset_pairs[asset2] = asset1
            self._initialized_assets.add(asset1)
            self._initialized_assets.add(asset2)

    def is_initialized(self) -> bool:
        with self._initialized_assets_lock:
            return len(self._initialized_assets) > 0

    def update_recent_trade(self, asset_id: str, trade_type: TradeType) -> None:
        with self._recent_trades_lock:
            if asset_id not in self._recent_trades:
                self._recent_trades[asset_id] = {"buy": None, "sell": None}
            self._recent_trades[asset_id][trade_type.value] = time.time()

    def get_last_trade_time(self) -> float:
        with self._last_trade_closed_at_lock:
            return self._last_trade_closed_at

    def set_last_trade_time(self, timestamp: float) -> None:
        with self._last_trade_closed_at_lock:
            self._last_trade_closed_at = timestamp

    def get_last_spike_info(self) -> Tuple[Optional[str], Optional[float]]:
        with self._last_spike_asset_lock, self._last_spike_price_lock:
            return self._last_spike_asset, self._last_spike_price

    def set_last_spike_info(self, asset: str, price: float) -> None:
        with self._last_spike_asset_lock, self._last_spike_price_lock:
            self._last_spike_asset = asset
            self._last_spike_price = price

    # def update_daily_pnl(self, pnl: float) -> None:
    #     with self._circuit_breaker_lock:
    #         self._daily_pnl += pnl
    #         logger.info(f"📊 Daily PnL updated: ${self._daily_pnl:.2f}")
            
    #         # Check circuit breaker conditions
    #         if self._daily_pnl < self._max_daily_loss:
    #             logger.error(f"🔴 Circuit breaker triggered: Daily loss limit reached (${self._daily_pnl:.2f})")
    #             self._trading_enabled = False
    #         elif self._daily_pnl < self._max_drawdown:
    #             logger.error(f"🔴 Circuit breaker triggered: Maximum drawdown reached (${self._daily_pnl:.2f})")
    #             self._trading_enabled = False

    # def is_trading_enabled(self) -> bool:
    #     with self._circuit_breaker_lock:
    #         return self._trading_enabled

    # def reset_daily_pnl(self) -> None:
    #     with self._circuit_breaker_lock:
    #         self._daily_pnl = 0.0
    #         self._trading_enabled = True
    #         logger.info("🔄 Daily PnL reset and trading enabled")

# Initialize ClobClient with retry mechanism
def initialize_clob_client(max_retries: int = 3) -> ClobClient:
    for attempt in range(max_retries):
        try:
            client = ClobClient(
                host="https://clob.polymarket.com",
                key=PRIVATE_KEY,
                chain_id=137,
                signature_type=2,
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

# API functions with retry mechanism
def fetch_positions_with_retry(max_retries: int = MAX_RETRIES) -> Dict[str, List[PositionInfo]]:
    for attempt in range(max_retries):
        try:
            url = f"https://data-api.polymarket.com/positions?user={YOUR_PROXY_WALLET}"
            logger.info(f"🔄 Fetching positions from {url} (attempt {attempt + 1}/{max_retries})")
            
            response = requests.get(url, timeout=API_TIMEOUT)
            logger.info(f"📡 API Response Status: {response.status_code}")
            
            if response.status_code != 200:
                logger.error(f"❌ API Error: {response.status_code} - {response.text}")
                raise NetworkError(f"API returned status code {response.status_code}")
            
            response.raise_for_status()
            data = response.json()
            
            if not isinstance(data, list):
                logger.error(f"❌ Invalid response format: {type(data)}")
                logger.error(f"Response content: {data}")
                raise ValidationError(f"Invalid response format from API: {type(data)}")
            
            if not data:
                logger.warning("⚠️ No positions found in API response. Waiting for positions...")
                return {}
                
            positions: Dict[str, List[PositionInfo]] = {}
            for pos in data:
                event_id = pos.get("conditionId") or pos.get("eventId") or pos.get("marketId")
                if not event_id:
                    logger.warning(f"⚠️ Skipping position with no event ID: {pos}")
                    continue
                    
                if event_id not in positions:
                    positions[event_id] = []
                    
                try:
                    position_info = PositionInfo(
                        eventslug=pos.get("eventSlug", ""),
                        outcome=pos.get("outcome", ""),
                        asset=pos.get("asset", ""),
                        avg_price=float(pos.get("avgPrice", 0)),
                        shares=float(pos.get("size", 0)),
                        current_price=float(pos.get("curPrice", 0)),
                        initial_value=float(pos.get("initialValue", 0)),
                        current_value=float(pos.get("currentValue", 0)),
                        pnl=float(pos.get("cashPnl", 0)),
                        percent_pnl=float(pos.get("percentPnl", 0)),
                        realized_pnl=float(pos.get("realizedPnl", 0))
                    )
                    positions[event_id].append(position_info)
                    logger.debug(f"✅ Added position: {position_info}")
                except (ValueError, TypeError) as e:
                    logger.error(f"❌ Error parsing position data: {e}")
                    logger.error(f"Problematic position data: {pos}")
                    continue
            
            logger.info(f"✅ Successfully fetched {len(positions)} positions")
            return positions
            
        except requests.RequestException as e:
            logger.error(f"❌ Network error in fetch_positions (attempt {attempt + 1}/{max_retries}): {str(e)}")
            if attempt == max_retries - 1:
                raise NetworkError(f"Failed to fetch positions after {max_retries} attempts: {e}")
            time.sleep(2 ** attempt)
        except (ValueError, ValidationError) as e:
            logger.error(f"❌ Validation error in fetch_positions (attempt {attempt + 1}/{max_retries}): {str(e)}")
            if attempt == max_retries - 1:
                raise ValidationError(f"Invalid data received from API: {e}")
            time.sleep(2 ** attempt)
        except Exception as e:
            logger.error(f"❌ Unexpected error in fetch_positions (attempt {attempt + 1}/{max_retries}): {str(e)}")
            if attempt == max_retries - 1:
                raise NetworkError(f"Failed to fetch positions after {max_retries} attempts: {e}")
            time.sleep(2 ** attempt)
    
    raise NetworkError("Failed to fetch positions after maximum retries")

def ensure_usdc_allowance(required_amount: float) -> bool:
    """Ensure USDC allowance with proper error handling"""
    max_retries = MAX_RETRIES
    base_delay = BASE_DELAY
    
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

            current_allowance = contract.functions.allowance(BOT_TRADER_ADDRESS , POLYMARKET_SETTLEMENT_CONTRACT).call()
            logger.info(f"current_allowance: {current_allowance}")
            required_amount_with_buffer = int(required_amount * 1.1 * 10**6)
            
            if current_allowance >= required_amount_with_buffer:
                return True

            logger.info(f"🔄 Approving USDC allowance... (attempt {attempt + 1}/{max_retries})")
            
            new_allowance = max(current_allowance, required_amount_with_buffer)
            logger.info(f"new_allowance: {new_allowance}")
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
                raise TradingError(f"USDC allowance update failed: {tx_hash.hex()}")
                
        except Exception as e:
            if attempt == max_retries - 1:
                raise TradingError(f"Failed to update USDC allowance: {e}")
            logger.error(f"⚠️ Error in USDC allowance update (attempt {attempt + 1}): {e}")
            time.sleep(base_delay * (2 ** attempt))
    
    return False

def refresh_api_credentials() -> bool:
    """Refresh API credentials with proper error handling"""
    try:
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
        if order.asks:
            buy_price = client.get_price(asset, "BUY")
            min_ask_price = order.asks[-1].price
            min_ask_size = order.asks[-1].size
            logger.info(f"min_ask_price: {min_ask_price}, min_ask_size: {min_ask_size}")
            return {
                "buy_price": buy_price,
                "min_ask_price": min_ask_price,
                "min_ask_size": min_ask_size
            }
        else:
            logger.error(f"❌ No ask data found for {asset}")
            return None
    except Exception as e:
        logger.error(f"❌ Failed to get ask data for {asset}: {str(e)}")
        return None

def get_max_bid_data(asset: str) -> Optional[Dict[str, Any]]:
    try:
        order = client.get_order_book(asset)
        if order.bids:
            sell_price = client.get_price(asset, "SELL")
            max_bid_price = order.bids[-1].price
            max_bid_size = order.bids[-1].size
            logger.info(f"max_bid_price: {max_bid_price}, max_bid_size: {max_bid_size}")
            return {
                "sell_price": sell_price,
                "max_bid_price": max_bid_price,
                "max_bid_size": max_bid_size
            }
        else:
            logger.error(f"❌ No bid data found for {asset}")
            return None
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

@log_function_call(logger)
def place_buy_order(state: ThreadSafeState, asset: str, reason: str) -> bool:
    try:
        # # Check circuit breaker
        # if not state.is_trading_enabled():
        #     logger.warning("🔒 Trading disabled due to circuit breaker")
        #     return False

        # Check maximum concurrent trades
        active_trades = state.get_active_trades()
        logger.info(f"active_trades----------------------------------------------->{active_trades}")
        if len(active_trades) >= MAX_CONCURRENT_TRADES:
            logger.warning(f"🔒 Maximum concurrent trades limit reached ({len(active_trades)}/{MAX_CONCURRENT_TRADES})")
            return False

        # Check USDC balance and calculate position size
        usdc_contract = web3.eth.contract(address=USDC_CONTRACT_ADDRESS, abi=[
            {"constant": True, "inputs": [{"name": "account", "type": "address"}],
             "name": "balanceOf", "outputs": [{"name": "", "type": "uint256"}],
             "payable": False, "stateMutability": "view", "type": "function"}
        ])
        usdc_balance = usdc_contract.functions.balanceOf(YOUR_PROXY_WALLET).call() / 10**6
        if not usdc_balance:
            return False
            
        
        max_retries = MAX_RETRIES
        base_delay = BASE_DELAY

        for attempt in range(max_retries):
            try:
                current_price = get_current_price(state, asset)
                if current_price is None:
                    raise TradingError(f"Failed to get current price for {asset}")

                min_ask_data = get_min_ask_data(asset)
                if min_ask_data is None:
                    logger.warning(f"❌ The {asset} is not tradable, Skipping...")
                    return False

                min_ask_price = float(min_ask_data["min_ask_price"])
                min_ask_size = float(min_ask_data["min_ask_size"])
                
                # Check liquidity requirement
                if min_ask_size * min_ask_price < MIN_LIQUIDITY_REQUIREMENT:
                    logger.warning(f"🔒 Insufficient liquidity for {asset}. Required: ${MIN_LIQUIDITY_REQUIREMENT}, Available: ${min_ask_size * min_ask_price:.2f}")
                    return False

                if min_ask_price - current_price > SLIPPAGE_TOLERANCE:
                    logger.warning(f"🔐 Slippage tolerance exceeded for {asset}. Skipping order.")
                    return False

                # Calculate position size based on account balance
                amount_in_dollars = min(TRADE_UNIT, min_ask_size * min_ask_price)
                
                if not check_usdc_balance(amount_in_dollars):
                    raise TradingError(f"Insufficient USDC balance for {asset}")
                
                if not ensure_usdc_allowance(amount_in_dollars):
                    raise TradingError(f"Failed to ensure USDC allowance for {asset}")

                order_args = MarketOrderArgs(
                    token_id=str(asset),
                    amount=float(amount_in_dollars),
                    side=BUY,
                )
                signed_order = client.create_market_order(order_args)
                response = client.post_order(signed_order, OrderType.FOK)
                if response.get("success"):
                    filled = response.get("data", {}).get("filledAmount", amount_in_dollars)
                    logger.info(f"🛒 [{reason}] Order placed: BUY {filled:.4f} shares of {asset} at ${min_ask_price:.4f}")
                    
                    trade_info = TradeInfo(
                        entry_price=min_ask_price,
                        entry_time=time.time(),
                        amount=amount_in_dollars,
                        bot_triggered=True
                    )
                    
                    state.update_recent_trade(asset, TradeType.BUY)
                    state.add_active_trade(asset, trade_info)
                    state.set_last_trade_time(time.time())
                    return True
                else:
                    error_msg = response.get("error", "Unknown error")
                    raise TradingError(f"Failed to place BUY order for {asset}: {error_msg}")

            except TradingError as e:
                logger.error(f"❌ Trading error in BUY order for {asset}: {str(e)}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(base_delay * (2 ** attempt))
            except Exception as e:
                logger.error(f"❌ Unexpected error in BUY order for {asset}: {str(e)}")
                if attempt == max_retries - 1:
                    raise TradingError(f"Failed to process BUY order after {max_retries} attempts: {e}")
                time.sleep(base_delay * (2 ** attempt))

        return False
    except Exception as e:
        logger.error(f"❌ Error placing BUY order for {asset}: {str(e)}", exc_info=True)
        raise

def place_sell_order(state: ThreadSafeState, asset: str, reason: str) -> bool:
    try:
        # # Check circuit breaker
        # if not state.is_trading_enabled():
        #     logger.warning("🔒 Trading disabled due to circuit breaker")
        #     return False

        max_retries = MAX_RETRIES
        base_delay = BASE_DELAY

        for attempt in range(max_retries):
            try:
                logger.info(f"🔄 Order attempt {attempt + 1}/{max_retries} for SELL {asset}")
                
                current_price = get_current_price(state,asset)
                if current_price is None:
                    raise TradingError(f"Failed to get current price for {asset}")

                max_bid_data = get_max_bid_data(asset)
                if max_bid_data is None:
                    logger.warning(f"❌ The {asset} is not tradable, Skipping...")
                    return False

                max_bid_price = float(max_bid_data["max_bid_price"])
                max_bid_size = float(max_bid_data["max_bid_size"])

                positions = state.get_positions()
                for event_id, item in positions.items():
                    for position in item:
                        if position.asset == asset:
                            balance = position.shares
                            avg_price = position.avg_price
                            sell_amount_in_shares = balance - KEEP_MIN_SHARES

                if sell_amount_in_shares < 1:
                    logger.warning(f"🙄 No shares to sell for {asset}, Skipping...")
                    continue

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
                signed_order = client.create_market_order(order_args)
                response = client.post_order(signed_order, OrderType.FOK)
                if response.get("success"):
                    filled = response.get("data", {}).get("filledAmount", sell_amount_in_shares)
                    logger.info(f"🛒 [{reason}] Order placed: SELL {filled:.4f} shares of {asset}")
                    state.update_recent_trade(asset, TradeType.SELL)
                    state.remove_active_trade(asset)
                    state.set_last_trade_time(time.time())
                    return True
                else:
                    error_msg = response.get("error", "Unknown error")
                    raise TradingError(f"Failed to place SELL order for {asset}: {error_msg}")

            except TradingError as e:
                logger.error(f"❌ Trading error in SELL order for {asset}: {str(e)}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(base_delay * (2 ** attempt))
            except Exception as e:
                logger.error(f"❌ Unexpected error in SELL order for {asset}: {str(e)}")
                if attempt == max_retries - 1:
                    raise TradingError(f"Failed to process SELL order after {max_retries} attempts: {e}")
                time.sleep(base_delay * (2 ** attempt))

        return False
    except Exception as e:
        logger.error(f"❌ Error placing SELL order for {asset}: {str(e)}")
        raise

def is_recently_bought(state: ThreadSafeState, asset_id: str) -> bool:
    with state._recent_trades_lock:
        if asset_id not in state._recent_trades or state._recent_trades[asset_id]["buy"] is None:
            return False
        now = time.time()
        time_since_buy = now - state._recent_trades[asset_id]["buy"]
        return time_since_buy < COOLDOWN_PERIOD

def is_recently_sold(state: ThreadSafeState, asset_id: str) -> bool:
    with state._recent_trades_lock:
        if asset_id not in state._recent_trades or state._recent_trades[asset_id]["sell"] is None:
            return False
        now = time.time()
        time_since_sell = now - state._recent_trades[asset_id]["sell"]
        return time_since_sell < COOLDOWN_PERIOD

def find_position_by_asset(positions: dict, asset_id: str) -> Optional[PositionInfo]:
    for event_positions in positions.values():
        for position in event_positions:
            if position.asset == asset_id:
                return position
    return None

class ThreadManager:
    def __init__(self, state: ThreadSafeState):
        self.state = state
        self.threads = {}
        self.thread_queues = {}
        self.executor = ThreadPoolExecutor(max_workers=THREAD_POOL_SIZE)
        self.running = True
        
    def start_thread(self, name: str, target: callable) -> None:
        if name in self.threads and self.threads[name].is_alive():
            return
            
        queue = Queue(maxsize=MAX_QUEUE_SIZE)
        self.thread_queues[name] = queue
        
        def thread_wrapper():
            error_count = 0
            consecutive_errors = 0
            while self.running and not self.state.is_shutdown():
                try:
                    target(self.state)
                    error_count = 0  # Reset error count on successful iteration
                    consecutive_errors = 0  # Reset consecutive errors
                    time.sleep(0.1)  # Small sleep to prevent CPU spinning
                except Exception as e:
                    error_count += 1
                    consecutive_errors += 1
                    logger.error(f"❌ Error in {name} thread: {str(e)}")
                    
                    if consecutive_errors >= MAX_ERRORS:
                        logger.error(f"❌ Too many consecutive errors in {name} thread. Restarting...")
                        time.sleep(THREAD_RESTART_DELAY)
                        consecutive_errors = 0  # Reset after restart delay
                    else:
                        time.sleep(1)  # Sleep between retries
        
        thread = threading.Thread(
            target=thread_wrapper,
            daemon=True,
            name=name
        )
        thread.start()
        self.threads[name] = thread
        logger.info(f"✅ Started thread: {name}")
        
    def stop(self) -> None:
        """Stop all threads gracefully"""
        self.running = False
        for thread in self.threads.values():
            if thread.is_alive():
                thread.join(timeout=5)
        self.executor.shutdown(wait=True)

def update_price_history(state: ThreadSafeState) -> None:
    last_log_time = time.time()
    update_count = 0
    initial_update = True
    
    while not state.is_shutdown():
        try:
            logger.info("🔄 Updating price history")
            start_time = time.time()
            
            now = time.time()
            positions = fetch_positions_with_retry()
            
            if not positions:
                time.sleep(5)
                continue
                
            state.update_positions(positions)
            
            price_updated = False
            current_time = time.time()
            price_updates = []
            
            for event_id, assets in positions.items():
                for asset in assets:
                    try:
                        eventslug = asset.eventslug
                        outcome = asset.outcome
                        asset_id = asset.asset
                        price = asset.current_price
                        
                        if not asset_id:
                            continue
                            
                        state.add_price(asset_id, now, price, eventslug, outcome)
                        update_count += 1
                        price_updated = True
                        
                        # Only log significant price changes
                        price_updates.append(f"                                               💸 {outcome} in {eventslug}: ${price:.4f}")
                            
                    except IndexError as e:
                        # Handle deque index out of range error
                        logger.debug(f"⏳ Building price history for {asset_id} - {eventslug}")
                        continue
                    except Exception as e:
                        logger.error(f"❌ Error updating price for asset {asset_id}: {str(e)}")
                        continue
            
            # Log price updates every 5 seconds
            if current_time - last_log_time >= 5:
                logger.info("📊 Price Updates:\n" + "\n".join(price_updates))
                last_log_time = current_time
                    
            if price_updated:
                price_update_event.set()
                if initial_update:
                    initial_update = False
                    logger.info("✅ Initial price data population complete")
            
            # Log summary every 1 minute
            if update_count >= 60:
                logger.info(f"📊 Price Update Summary | Updates: {update_count} | Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
                update_count = 0
            
            # Ensure we don't run too fast
            elapsed = time.time() - start_time
            if elapsed < 1.0:
                time.sleep(1.0 - elapsed)
                
        except Exception as e:
            logger.error(f"❌ Error in price update: {str(e)}")
            time.sleep(1)

def detect_and_trade(state: ThreadSafeState) -> None:
    last_log_time = time.time()
    scan_count = 0
    
    while not state.is_shutdown():
        try:
            # Wait for price update with timeout
            if price_update_event.wait(timeout=1.0):
                price_update_event.clear()
                
                # Ensure we have some price history before proceeding
                if not any(state.get_price_history(asset_id) for asset_id in state._price_history.keys()):
                    logger.debug("⏳ Waiting for price history to be populated...")
                    continue
                
                positions_copy = state.get_positions()
                scan_count += 1
                
                # Log scan progress every 5 seconds
                current_time = time.time()
                if current_time - last_log_time >= 5:
                    logger.info(f"🔍 Scanning Markets | Scan #{scan_count} | Active Positions: {len(positions_copy)}")
                    last_log_time = current_time
                
                for asset_id in list(state._price_history.keys()):
                    try:
                        history = state.get_price_history(asset_id)
                        if len(history) < 2:
                            continue

                        old_price = history[0][1]
                        new_price = history[-1][1]
                        
                        # Skip if either price is zero to prevent division by zero
                        if old_price == 0 or new_price == 0:
                            logger.warning(f"⚠️ Skipping asset {asset_id} due to zero price - Old: ${old_price:.4f}, New: ${new_price:.4f}")
                            continue
                            
                        delta = (new_price - old_price) / old_price

                        if abs(delta) > SPIKE_THRESHOLD:
                            if new_price < 0.20 or new_price > 0.80:
                                continue

                            
                            opposite = state.get_asset_pair(asset_id)
                            if not opposite:
                                continue

                            if delta > 0 and not is_recently_bought(state, asset_id):
                                logger.info(f"🟨 Spike Detected | Asset: {asset_id} | Delta: {delta:.2%} | Price: ${new_price:.4f}")
                                logger.info(f"🟢 Buy Signal | Asset: {asset_id} | Price: ${new_price:.4f}")
                                if place_buy_order(state, asset_id, "Spike detected"):
                                    place_sell_order(state, opposite, "Opposite trade")
                            elif delta < 0 and not is_recently_sold(state, asset_id):
                                logger.info(f"🟨 Spike Detected | Asset: {asset_id} | Delta: {delta:.2%} | Price: ${new_price:.4f}")
                                logger.info(f"🔴 Sell Signal | Asset: {asset_id} | Price: ${new_price:.4f}")
                                if place_sell_order(state, asset_id, "Spike detected"):
                                    place_buy_order(state, opposite, "Opposite trade")

                    except IndexError:
                        logger.debug(f"⏳ Building price history for {asset_id}")
                        continue
                    except Exception as e:
                        logger.error(f"❌ Error processing asset {asset_id}: {str(e)}")
                        continue
                        
        except Exception as e:
            logger.error(f"❌ Error in detect_and_trade: {str(e)}")
            time.sleep(1)

def check_trade_exits(state: ThreadSafeState) -> None:
    last_log_time = time.time()
    
    while not state.is_shutdown():
        try:
            active_trades = state.get_active_trades()
            if active_trades:
                # Log active trades every 30 seconds instead of 5
                current_time = time.time()
                if current_time - last_log_time >= 30:
                    logger.info(f"📈 Active Trades | Count: {len(active_trades)} | Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
                    last_log_time = current_time
            
            for asset_id, trade in active_trades.items():
                try:
                    positions_copy = state.get_positions()
                    position = find_position_by_asset(positions_copy, asset_id)
                    if not position:
                        continue
                        
                    current_price = get_current_price(state, asset_id)
                    if current_price is None:
                        continue
                        
                    current_time = time.time()
                    last_traded = trade.entry_time  # entry_time is now a float timestamp
                    avg_price = position.avg_price
                    remaining_shares = position.shares
                    cash_profit = (current_price - avg_price) * remaining_shares
                    pct_profit = (current_price - avg_price) / avg_price

                    if current_time - last_traded > HOLDING_TIME_LIMIT:
                        logger.info(f"⏰ Holding Time Limit Hit | Asset: {asset_id} | Holding Time: {current_time - last_traded:.2f} seconds | Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
                        place_sell_order(state, asset_id, "Holding time limit")
                        state.remove_active_trade(asset_id)
                        state.set_last_trade_time(time.time())
                    
                    if cash_profit >= CASH_PROFIT or pct_profit > PCT_PROFIT:
                        logger.info(f"🎯 Take Profit Hit | Asset: {asset_id} | Profit: ${cash_profit:.2f} ({pct_profit:.2%}) | Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
                        place_sell_order(state, asset_id, "Take profit")
                        state.remove_active_trade(asset_id)
                        state.set_last_trade_time(time.time())

                    if cash_profit <= CASH_LOSS or pct_profit < PCT_LOSS:
                        logger.info(f"🔴 Stop Loss Hit | Asset: {asset_id} | Loss: ${cash_profit:.2f} ({pct_profit:.2%}) | Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
                        place_sell_order(state, asset_id, "Stop loss")
                        state.remove_active_trade(asset_id)
                        state.set_last_trade_time(time.time())


                except Exception as e:
                    logger.error(f"❌ Error checking trade exit for {asset_id}: {str(e)}")
                    continue

            time.sleep(1)
            
        except Exception as e:
            logger.error(f"❌ Error in check_trade_exits: {str(e)}")
            time.sleep(1)

def get_current_price(state: ThreadSafeState, asset_id: str) -> Optional[float]:
    try:
        history = state.get_price_history(asset_id)
        if not history:
            logger.debug(f"⏳ No price history available for {asset_id}")
            return None
        return history[-1][1]
    except IndexError:
        logger.debug(f"⏳ Building price history for {asset_id}")
        return None
    except Exception as e:
        logger.error(f"❌ Error getting current price for {asset_id}: {str(e)}")
        return None

def wait_for_initialization(state: ThreadSafeState) -> bool:
    max_retries = 60
    retry_count = 0
    while retry_count < max_retries and not state.is_shutdown():
        try:
            positions = fetch_positions_with_retry()
            for event_id, sides in positions.items():
                logger.info(f"🔎 Event ID {event_id}: {len(sides)}")
                if len(sides) % 2 == 0 and len(sides) > 1:
                    ids = [s.asset for s in sides]
                    state.add_asset_pair(ids[0], ids[1])
                    logger.info(f"✅ Initialized asset pair: {ids[0]} ↔ {ids[1]}")
            
            if state.is_initialized():
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
    logger.info("🔄 Starting cleanup...")
    
    try:
        # Initiate shutdown
        state.shutdown()
        
        # Wait for threads to finish with timeout
        for thread in threading.enumerate():
            if thread != threading.current_thread():
                thread.join(timeout=5)
                if thread.is_alive():
                    logger.warning(f"Thread {thread.name} did not finish in time")
                    # Force terminate the thread if it's still alive
                    if hasattr(thread, '_stop'):
                        thread._stop()
        
        # Close any open connections
        try:
            # The ClobClient doesn't have a close method, so we just set it to None
            global client
            client = None
        except Exception as e:
            logger.error(f"Error closing client connection: {e}")
        
        # Wait for cleanup to complete
        if not state.wait_for_cleanup(timeout=10):
            logger.warning("Cleanup did not complete in time")
        
        logger.info("✅ Cleanup complete")
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
        raise

def signal_handler(signum: int, frame: Any, state: ThreadSafeState) -> None:
    logger.info(f"Received signal {signum}. Initiating shutdown...")
    cleanup(state)
    sys.exit(0)

def main() -> None:
    state = None
    thread_manager = None
    try:
        state = ThreadSafeState()
        thread_manager = ThreadManager(state)
        print_spikebot_banner()
        
        # Set up signal handlers
        signal.signal(signal.SIGINT, lambda s, f: signal_handler(s, f, state))
        signal.signal(signal.SIGTERM, lambda s, f: signal_handler(s, f, state))
        
        # Initialize
        spinner = Halo(text="Waiting for manual $1 entries on both sides of a market...", spinner="dots")
        spinner.start()
        time.sleep(5)
        logger.info(f"🚀 Spike-detection bot started at {time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        if not wait_for_initialization(state):
            spinner.fail("❌ Failed to initialize. Exiting.")
            raise ConfigurationError("Failed to initialize bot")
        
        spinner.succeed("Initialized successfully")
        
        # Start price update thread first and wait for initial data
        logger.info("🔄 Starting price update thread...")
        thread_manager.start_thread("price_update", update_price_history)
        
        # Wait for initial price data
        logger.info("⏳ Waiting for initial price data...")
        initial_data_wait = 0
        while initial_data_wait < 30:  # Wait up to 30 seconds for initial data
            if any(state.get_price_history(asset_id) for asset_id in state._price_history.keys()):
                logger.info("✅ Initial price data received")
                break
            time.sleep(1)
            initial_data_wait += 1
            if initial_data_wait % 5 == 0:
                logger.info(f"⏳ Still waiting for initial price data... ({initial_data_wait}/30 seconds)")
        
        if initial_data_wait >= 30:
            logger.warning("⚠️ No initial price data received after 30 seconds")
        
        # Start trading threads
        logger.info("🔄 Starting trading threads...")
        thread_manager.start_thread("detect_trade", detect_and_trade)
        thread_manager.start_thread("check_exits", check_trade_exits)
        
        last_refresh_time = time.time()
        refresh_interval = REFRESH_INTERVAL
        last_status_time = time.time()
        last_daily_reset = time.time()
        
        # Main loop
        while not state.is_shutdown():
            try:
                current_time = time.time()
                
                # Daily reset at midnight UTC
                # if current_time - last_daily_reset >= 86400:  # 24 hours
                #     logger.info("🔄 Performing daily reset...")
                #     state.reset_daily_pnl()
                #     last_daily_reset = current_time
                
                # Log status every 30 seconds
                if current_time - last_status_time >= 30:
                    active_threads = sum(1 for t in thread_manager.threads.values() if t.is_alive())
                    logger.info(f"📊 Bot Status | Active Threads: {active_threads}/3 | Price Updates: {len(state._price_history)}")
                    last_status_time = current_time
                
                # Refresh API credentials
                if current_time - last_refresh_time > refresh_interval:
                    if refresh_api_credentials():
                        last_refresh_time = current_time
                    else:
                        logger.warning("⚠️ Failed to refresh API credentials. Will retry in 5 minutes.")
                        time.sleep(300)
                        continue
                
                # Check if any threads have died
                for name, thread in thread_manager.threads.items():
                    if not thread.is_alive():
                        logger.warning(f"⚠️ Thread {name} has died. Restarting...")
                        thread_manager.start_thread(name, globals()[name.replace(" ", "_")])
                
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                time.sleep(1)
                
    except KeyboardInterrupt:
        logger.info("👋 Shutting down gracefully...")
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
    finally:
        if thread_manager:
            thread_manager.stop()
        if state:
            cleanup(state)

if __name__ == "__main__":
    main()