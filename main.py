import json
import os
import time
import requests
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
logging.basicConfig(
    filename='bot_log.txt',
    filemode='a',  # 'w' to overwrite every run
    format='%(asctime)s - %(message)s',
    level=logging.INFO
)

# Global state variables
price_history = {}  # {asset_id: deque[(timestamp, price)]}
active_trades = {}  # {asset_id: {"entry_price": ..., "entry_time": ..., "shares": ..., "bot_triggered": True}}
latest_active_trade = {}  # {asset_id: {"asset_pair": "asset_id:opposite", "asset_id": "...", "entry_time": "...", "trade_type": "buy/sell", "shares": ...}}
last_trade_closed_at = 0
last_spike_asset = None
last_spike_price = None
initialized_assets = set()
asset_pairs = {}  # asset_id -> opposite_id
recent_trades = {}  # asset_id -> timestamp of last spike trade
max_price_history_age = 120  # seconds
max_price_history_size = 1000  # entries per asset
TRADE_UNIT = 3.0  # Amount in USD for each trade
MIN_TRADE_INTERVAL = 60  # Minimum seconds between trades for the same pair

def log(msg):
    print(msg)
    logging.info(msg)

load_dotenv(".env")

WEB3_PROVIDER = "https://polygon-rpc.com"
YOUR_PROXY_WALLET = Web3.to_checksum_address("0xAD25B950d4b2FE8e5cD67aAfF8Ea07dc32A7Ba7c")
BOT_TRADER_ADDRESS = Web3.to_checksum_address("0x57e8701477723F013C4a890F00a27c9F4b83e8F6")
USDC_CONTRACT_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
POLYMARKET_SETTLEMENT_CONTRACT = "0x56C79347e95530c01A2FC76E732f9566dA16E113"

PRIVATE_KEY = os.getenv("PK")
if not PRIVATE_KEY:
    raise ValueError("❌ Private key not found. Please check your .env file.")

web3 = Web3(Web3.HTTPProvider(WEB3_PROVIDER))

client = ClobClient(
    host="https://clob.polymarket.com",
    key=PRIVATE_KEY,
    chain_id=137,
    signature_type=2, # Replace with 1 if you sign in with Gmail.
    funder=YOUR_PROXY_WALLET
)
api_creds = client.create_or_derive_api_creds()
client.set_api_creds(api_creds)

def get_price_history(asset_id: str) -> List[Tuple[datetime, float]]:
    return list(price_history.get(asset_id, deque()))

def add_price(asset_id: str, timestamp: datetime, price: float):
    if asset_id not in price_history:
        price_history[asset_id] = deque(maxlen=max_price_history_size)
    
    # Remove old entries more efficiently
    while (price_history[asset_id] and 
           (timestamp - price_history[asset_id][0][0]).total_seconds() > max_price_history_age):
        price_history[asset_id].popleft()
        if len(price_history[asset_id]) == 0:  # Prevent infinite loop if deque is empty
            break
    
    price_history[asset_id].append((timestamp, price))

def get_active_trades() -> Dict[str, dict]:
    return dict(active_trades)

def add_active_trade(asset_id: str, trade_data: dict):
    active_trades[asset_id] = trade_data

def remove_active_trade(asset_id: str):
    active_trades.pop(asset_id, None)

def get_last_trade_time() -> float:
    return last_trade_closed_at

def set_last_trade_time(timestamp: float):
    global last_trade_closed_at
    last_trade_closed_at = timestamp

def get_asset_pair(asset_id: str) -> Optional[str]:
    return asset_pairs.get(asset_id)

def add_asset_pair(asset1: str, asset2: str):
    global initialized_assets
    asset_pairs[asset1] = asset2
    asset_pairs[asset2] = asset1
    initialized_assets.add(asset1)
    initialized_assets.add(asset2)

def is_initialized() -> bool:
    return len(initialized_assets) > 0

def update_recent_trade(asset_id: str):
    recent_trades[asset_id] = time.time()

def is_recently_traded(asset_id: str, cooldown_seconds: int = int(os.getenv("recent_traded"))) -> bool:
    last_trade = recent_trades.get(asset_id, 0)
    return time.time() - last_trade < cooldown_seconds

def fetch_positions():
    url = f"https://data-api.polymarket.com/positions?user={YOUR_PROXY_WALLET}"
    try:
        response = requests.get(url)
        data = response.json()
        if not isinstance(data, list):
            return {}
        positions = {}
        for pos in data:
            asset = pos.get("asset")
            avg_price = pos.get("avgPrice")
            shares = pos.get("size")
            current_price = pos.get("curPrice")
            event_id = pos.get("conditionId") or pos.get("eventId") or pos.get("marketId") or pos.get("asset")
            if asset and avg_price is not None and shares and current_price is not None and event_id:
                if event_id not in positions:
                    positions[event_id] = []
                positions[event_id].append({
                    "asset": asset,
                    "avg_price": avg_price,
                    "shares": shares,
                    "current_price": current_price
                })
        log(f"💰 Fetched positions: {positions}")
        return positions
    except Exception as e:
        log(f"⚠️ Failed to fetch positions: {e}")
        return {}

def get_actual_balance(asset_id):
    positions = fetch_positions()
    for sides in positions.values():
        for pos in sides:
            if pos["asset"] == asset_id:
                return float(pos["shares"])
    return 0.0

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

            log(f"🔄 Approving USDC allowance... (attempt {attempt + 1}/{max_retries})")
            
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
                log(f"✅ USDC allowance updated: {tx_hash.hex()}")
                return True
            else:
                log(f"❌ USDC allowance update failed: {tx_hash.hex()}")
                
        except Exception as e:
            log(f"⚠️ Error in USDC allowance update (attempt {attempt + 1}): {e}")
            
        if attempt < max_retries - 1:
            delay = base_delay * (2 ** attempt)  # exponential backoff
            log(f"⏳ Waiting {delay}s before retry...")
            time.sleep(delay)
    
    return False

def refresh_api_credetials() -> bool:
    """Refresh API credentials for the trading client"""
    try:
        global client, api_creds
        api_creds = client.create_or_derive_api_creds()
        client.set_api_creds(api_creds)
        log("✅ API credentials refreshed successfully")
        return True
    except Exception as e:
        log(f"❌ Failed to refresh API credentials: {str(e)}")
        return False

def place_order(side: str, asset: str, amount_in_shares: float, reason: str, 
                fallback_to_sell: bool = False, fallback_to_buy: bool = False) -> bool:
    if not asset:
        log(f"⚠️ Invalid asset. Skipping order for {asset}")
        return False
    
    log(f"🔍 Order parameters - Side: {side}, Asset: {asset}, Amount: {amount_in_shares}, Reason: {reason}")
    log(f"🔄 Fallback settings - fallback_to_sell: {fallback_to_sell}, fallback_to_buy: {fallback_to_buy}")
    
    max_retries = 3
    base_delay = 1  # seconds
    
    for attempt in range(max_retries):
        log(f"🔄 Order attempt {attempt + 1}/{max_retries} for {side} {asset}")
        try:
            current_price = get_current_price(asset)
            log(f"💰 Current price for {asset}: {current_price}")
            if not current_price:
                log(f"⚠️ Could not fetch price for {asset}. Skipping order.")
                return False

            # 💵 Enforce $10 cap on BUY orders
            if side.upper() == "BUY":
                log("🛒 Processing BUY order...")
                max_usd = 3.0
                amount_in_shares = round(max_usd / current_price, 4)
                usdc_needed = current_price * amount_in_shares
                
                # Check USDC balance before proceeding
                try:
                    usdc_contract = web3.eth.contract(address=USDC_CONTRACT_ADDRESS, abi=[
                        {"constant": True, "inputs": [{"name": "account", "type": "address"}],
                         "name": "balanceOf", "outputs": [{"name": "", "type": "uint256"}],
                         "payable": False, "stateMutability": "view", "type": "function"}
                    ])
                    usdc_balance = usdc_contract.functions.balanceOf(YOUR_PROXY_WALLET).call() / 10**6
                    
                    log(f"💵 USDC Balance: ${usdc_balance:.2f}, Required: ${usdc_needed:.2f}")
                    
                    if usdc_balance < usdc_needed:
                        log(f"❌ Insufficient USDC balance. Required: ${usdc_needed:.2f}, Available: ${usdc_balance:.2f}")
                        return False
                        
                    if not ensure_usdc_allowance(usdc_needed):
                        log(f"❌ Failed to ensure USDC allowance for {asset}")
                        return False
                except Exception as e:
                    log(f"❌ Failed to check USDC balance: {str(e)}")
                    return False
            else:
                actual_shares = get_actual_balance(asset)
                current_price = get_current_price(asset)
                amount_in_shares = actual_shares - round(1.0 / current_price, 4)
            if amount_in_shares <= 0:
                log(f"⚠️ Invalid share amount. Skipping SELL order for {asset}")
                return False
            side_formatted = BUY if side.upper() == "BUY" else SELL
            log(f"📤 Preparing {side.upper()} order: {amount_in_shares:.4f} shares of {asset} ({reason})")

            order_args = MarketOrderArgs(
                token_id=str(asset),
                amount=float(amount_in_shares),
                side=side_formatted,
            )

            log(f"🔑 Creating signed order for {asset}...")
            signed_order = client.create_market_order(order_args)
            
            # Log order details without JSON serialization
            log(f"📝 Order details: {str(signed_order)}")
            
            order_type = OrderType.FOK if (fallback_to_sell or fallback_to_buy) else OrderType.FOK
            log(f"📨 Posting order with type: {order_type}")
            
            response = client.post_order(signed_order, order_type)
            # Log response without JSON serialization
            log(f"📥 Order response: {str(response)}")

            if response.get("success"):
                filled = response.get("data", {}).get("filledAmount", amount_in_shares)
                log(f"✅ [{reason}] Order placed: {side.upper()} {filled:.4f} shares of {asset}")
                update_recent_trade(asset)
                return True
            else:
                error_msg = response.get('errorMsg', 'Unknown error')
                error_code = response.get('errorCode', 'No error code')
                log(f"❌ [{reason}] Order failed with code {error_code}: {error_msg}")
                log(f"❌ Full error response: {json.dumps(response, indent=2)}")
                
                # Handle SELL fallback without recursion
                if side.upper() == "SELL" and fallback_to_sell and not reason.endswith("_fallback"):
                    log(f"⚠️ SELL failed — attempting fallback with actual balance...")
                    actual_shares = get_actual_balance(asset)
                    log(f"📊 Actual balance for {asset}: {actual_shares}")
                    
                    if actual_shares and actual_shares > 0.001:
                        # Create new order args with actual shares
                        order_args = MarketOrderArgs(
                            token_id=str(asset),
                            amount=float(actual_shares),
                            side=SELL,
                        )
                        log(f"🔄 Retrying with actual shares: {actual_shares}")
                        signed_order = client.create_market_order(order_args)
                        response = client.post_order(signed_order, OrderType.FOK)
                        
                        log(f"📥 Fallback order response: {json.dumps(response, indent=2)}")
                        
                        if response.get("success"):
                            filled = response.get("data", {}).get("filledAmount", actual_shares)
                            log(f"✅ [{reason}_fallback] Order placed: SELL {filled:.4f} shares of {asset}")
                            update_recent_trade(asset)
                            return True
                        else:
                            error_msg = response.get('errorMsg', 'Unknown error')
                            error_code = response.get('errorCode', 'No error code')
                            log(f"❌ [{reason}_fallback] Order failed with code {error_code}: {error_msg}")
                            log(f"❌ Full fallback error response: {json.dumps(response, indent=2)}")
                
        except Exception as e:
            log(f"⚠️ Error during {side.upper()} order for {asset}: {str(e)}")
            log(f"⚠️ Error type: {type(e).__name__}")
            import traceback
            log(f"⚠️ Stack trace: {traceback.format_exc()}")
        
        if attempt < max_retries - 1:
            delay = base_delay * (2 ** attempt)  # exponential backoff
            log(f"⏳ Waiting {delay}s before retry...")
            time.sleep(delay)
    
    return False

def can_trade_asset(asset_id: str) -> bool:
    """Check if we can trade this asset based on latest trade time"""
    if asset_id not in latest_active_trade:
        return True
    
    last_trade_time = latest_active_trade[asset_id].get("entry_time", 0)
    return time.time() - last_trade_time >= MIN_TRADE_INTERVAL

def update_latest_trade(asset_id: str, opposite: str, trade_type: str, shares: float):
    """Update the latest trade record for an asset pair"""
    latest_active_trade[asset_id] = {
        "asset_pair": f"{asset_id}:{opposite}",
        "asset_id": asset_id,
        "entry_time": time.time(),
        "trade_type": trade_type,
        "shares": shares
    }
    latest_active_trade[opposite] = {
        "asset_pair": f"{asset_id}:{opposite}",
        "asset_id": opposite,
        "entry_time": time.time(),
        "trade_type": "sell" if trade_type == "buy" else "buy",
        "shares": shares
    }

def handle_spike_trade(asset_id: str, opposite: str, delta: float, current_price: float):
    """Handle trading logic for a detected spike"""
    if not can_trade_asset(asset_id):
        log(f"⏳ Trade cooldown active for {asset_id}")
        return

    if delta < 0:  # Price decreased
        # Sell spike asset but keep $1 worth
        shares = get_actual_balance(asset_id)
        keep_shares = round(1.0 / current_price, 4)
        sell_shares = shares - keep_shares
        
        if sell_shares > 0.001:
            log(f"💰 SELLing {sell_shares:.4f} shares of {asset_id}")
            success = place_order("SELL", asset_id, sell_shares, "spike_decrease", fallback_to_sell=True)
            if success:
                update_latest_trade(asset_id, opposite, "sell", sell_shares)

        # Buy opposite asset
        buy_shares = round(TRADE_UNIT / current_price, 4)
        log(f"💰 BUYing {buy_shares:.4f} shares of {opposite}")
        success = place_order("BUY", opposite, buy_shares, "spike_decrease", fallback_to_buy=True)
        if success:
            update_latest_trade(opposite, asset_id, "buy", buy_shares)
            add_active_trade(opposite, {
                "entry_price": current_price,
                "entry_time": datetime.now(UTC),
                "shares": buy_shares,
                "bot_triggered": True
            })

    elif delta > 0:  # Price increased
        # Buy spike asset
        buy_shares = round(TRADE_UNIT / current_price, 4)
        log(f"💰 BUYing {buy_shares:.4f} shares of {asset_id}")
        success = place_order("BUY", asset_id, buy_shares, "spike_increase", fallback_to_buy=True)
        if success:
            update_latest_trade(asset_id, opposite, "buy", buy_shares)
            add_active_trade(asset_id, {
                "entry_price": current_price,
                "entry_time": datetime.now(UTC),
                "shares": buy_shares,
                "bot_triggered": True
            })

        # Sell opposite asset but keep $1 worth
        shares = get_actual_balance(opposite)
        keep_shares = round(1.0 / current_price, 4)
        sell_shares = shares - keep_shares
        
        if sell_shares > 0.001:
            log(f"💰 SELLing {sell_shares:.4f} shares of {opposite}")
            success = place_order("SELL", opposite, sell_shares, "spike_increase", fallback_to_sell=True)
            if success:
                update_latest_trade(opposite, asset_id, "sell", sell_shares)

def detect_and_trade():
    log("🔍 Scanning for spikes...")
    now = datetime.now(UTC)

    # Get all assets from price history
    all_assets = set()
    for asset_id in price_history.keys():
        all_assets.add(asset_id)

    for asset_id in all_assets:
        if is_recently_traded(asset_id):
            log(f"🕒 Recently traded {asset_id} — skipping.")
            continue

        history = get_price_history(asset_id)
        if len(history) < 2:
            continue

        old_price = history[0][1]
        new_price = history[-1][1]
        delta = (new_price - old_price) / old_price

        log(f"📈 {asset_id}: old={old_price:.4f}, new={new_price:.4f}, delta={delta:.2%}")

        if old_price < 0.20:
            continue

        if abs(delta) >= 0.01:
            log(f"🟨 Spike detected on {asset_id} with delta {delta:.2%}")
            opposite = get_asset_pair(asset_id)
            log(f"🔄 Opposite asset: {opposite}")
            
            if not opposite:
                log(f"❌ No opposite found for {asset_id}")
                continue

            handle_spike_trade(asset_id, opposite, delta, new_price)

def update_price_history():
    """Updates price history with thread-safe state management."""
    now = datetime.now(UTC)
    positions = fetch_positions()
    
    for event_id, assets in positions.items():
        for outcome in assets:
            asset_id = outcome["asset"]
            price = outcome["current_price"]
            add_price(asset_id, now, price)
            log(f"💾 Updated price for {asset_id}: {price:.4f}")

def get_current_price(asset_id: str) -> Optional[float]:
    """Gets the current price with proper error handling."""
    try:
        # First try to get from price history
        history = get_price_history(asset_id)
        if history:
            return history[-1][1]
            
        # Fallback to live price
        positions = fetch_positions()
        for sides in positions.values():
            for side in sides:
                if side["asset"] == asset_id:
                    return side["current_price"]
    except Exception as e:
        log(f"⚠️ Live price fetch failed for {asset_id}: {e}")
    return None

def check_trade_exits():
    """Checks and handles trade exits with proper state management."""
    now = datetime.now(UTC)
    active_trades = get_active_trades()
    log(f"📈 Active trades: {len(active_trades)}")

    for asset_id in list(active_trades.keys()):
        trade = active_trades[asset_id]
        entry_price = trade["entry_price"]
        entry_time = trade["entry_time"]
        shares = trade["shares"]

        history = get_price_history(asset_id)
        if len(history) < 2:
            continue
        if history[-1][1] == entry_price:
            continue

        current_price = get_current_price(asset_id)
        if current_price is None:
            continue

        profit = (current_price - entry_price) / entry_price
        time_held = now - entry_time

        if trade.get("pending_exit"):
            cooldown = int(os.getenv("cooldown"))
            max_retries = 3
            retry_count = trade.get("retry_count", 0)
            
            if retry_count >= max_retries:
                log(f"❌ Max retries reached for {asset_id}, removing from active trades")
                remove_active_trade(asset_id)
                continue
                
            if time.time() - trade.get("pending_exit_since", 0) < cooldown:
                log(f"⏳ Waiting before retrying SELL for {asset_id}")
                continue
            else:
                log(f"🔁 Retrying SELL for {asset_id} after cooldown (attempt {retry_count + 1}/{max_retries})")
                trade["retry_count"] = retry_count + 1
                add_active_trade(asset_id, trade)

        log(f"🕒 {asset_id} held for {int(time_held.total_seconds() // 60)}m {int(time_held.total_seconds() % 60)}s")
        log(f"💰 PNL Check: entry={entry_price:.4f}, current={current_price:.4f}, profit={profit:.2%}")
        log(f"⏱ Checking exit logic for {asset_id} | Held: {time_held.total_seconds():.2f}s | Profit: {profit:.2%}")
        log(f"⏱ Entry Time: {entry_time}, Now: {now}, Held Seconds: {time_held.total_seconds():.2f}")

        def handle_failed_sell(reason: str, asset_id: str):
            positions = fetch_positions()
            asset_still_held = False
            for sides in positions.values():
                for pos in sides:
                    if pos["asset"] == asset_id and float(pos["shares"]) > 0.001:
                        asset_still_held = True
                        break
                if asset_still_held:
                    break

            if asset_still_held:
                log(f"⚠️ SELL failed for {reason} — position still active, will retry later")
                trade["pending_exit"] = True
                trade["pending_exit_since"] = time.time()
                add_active_trade(asset_id, trade)
            else:
                log(f"✅ SELL likely succeeded (or dust) — clearing {asset_id}")
                remove_active_trade(asset_id)
                set_last_trade_time(time.time())

        if profit >= float(os.getenv("take_profit")):
            log(f"🎯 TP hit for {asset_id}. Selling...")
            success = place_order("SELL", asset_id, shares, "take_profit", fallback_to_sell=True)
            if success:
                remove_active_trade(asset_id)
                set_last_trade_time(time.time())
                log("✅ Trade closed — ready for next.")
            else:
                handle_failed_sell("TP", asset_id)

        elif profit <= float(os.getenv("stop_loss")):
            log(f"🛑 SL hit for {asset_id}. Selling...")
            success = place_order("SELL", asset_id, shares, "stop_loss", fallback_to_sell=True)
            if success:
                remove_active_trade(asset_id)
                set_last_trade_time(time.time())
                log("✅ Trade closed — ready for next.")
            else:
                handle_failed_sell("SL", asset_id)

        elif time_held >= timedelta(minutes=1):
            log(f"⌛ Time-based exit for {asset_id}. Selling...")
            success = place_order("SELL", asset_id, shares, "timeout_exit", fallback_to_sell=True)
            if success:
                remove_active_trade(asset_id)
                set_last_trade_time(time.time())
                log("✅ Trade closed — ready for next.")
            else:
                handle_failed_sell("timeout", asset_id)

def wait_for_initialization():
    log("🕓 Waiting for manual $1 entries on both sides of a market...")
    max_retries = 60  # 1 minute total with 2-second sleep
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            positions = fetch_positions()
            for event_id, sides in positions.items():
                log(f"🔎 Event ID {event_id}: {len(sides)}")
                if len(sides) % 2 == 0 and len(sides) > 1:
                    ids = [s["asset"] for s in sides]
                    add_asset_pair(ids[0], ids[1])
                    log(f"✅ Initialized asset pair: {ids[0]} ↔ {ids[1]}")
            
            if is_initialized():
                log(f"✅ Initialization complete with {len(initialized_assets)} assets.")
                return True
                
            retry_count += 1
            time.sleep(2)
            
        except Exception as e:
            log(f"⚠️ Error during initialization: {e}")
            retry_count += 1
            time.sleep(2)
    
    log("❌ Initialization timed out after 2 minutes.")
    return False

if __name__ == "__main__":
    log(f"🚀 Spike-detection bot started at {datetime.now(UTC)}")
    
    if not wait_for_initialization():
        log("❌ Failed to initialize. Exiting.")
        exit(1)
        
    last_refresh_time = time.time()
    refresh_interval = 3600
    last_price_update = time.time()
    price_update_interval = 1  # Update prices every 1 seconds
    
    while True:
        try:
            current_time = time.time()
            
            if current_time - last_refresh_time > refresh_interval:
                if refresh_api_credetials():
                    last_refresh_time = current_time
                else:
                    log("⚠️ Failed to refresh API credentials. Will retry in 5 minutes.")
                    time.sleep(300)
                    continue

            if current_time - last_price_update >= price_update_interval:
                update_price_history()
                last_price_update = current_time
            
            detect_and_trade()
            check_trade_exits()
            
            sleep_time = max(0, price_update_interval - (time.time() - current_time))
            if sleep_time > 0:
                time.sleep(sleep_time)
            
        except Exception as e:
            log(f"❌ Error in main loop: {e}")
            time.sleep(5)