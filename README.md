# PolySpike Trader

An automated trading bot for Polymarket that detects price spikes and executes trades on the Polygon network.

## Features

- Multi-pair trading support
- Automatic price spike detection
- Configurable take-profit and stop-loss levels
- USDC balance management
- Automatic API credential refresh
- Comprehensive logging

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Create a `keys.env` file with your configuration:
```env
PK=  \\Replace your private key of metamask
YOUR_PROXY_WALLET=0xAD25B950d4b2FE8e5cD67aAfF8Ea07dc32A7Ba7c  \\Replace your polymarket proxy wallet address.
BOT_TRADER_ADDRESS=0x57e8701477723F013C4a890F00a27c9F4b83e8F6  \\Replace your metamask wallet address
USDC_CONTRACT_ADDRESS=0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174
POLYMARKET_SETTLEMENT_CONTRACT=0x56C79347e95530c01A2FC76E732f9566dA16E113
cooldown=10
take_profit=0.03
stop_loss=-0.025
recent_traded=120
buy_unit_size=3   \\Adjust as you want
```

## Configuration Parameters

- `PK`: Your wallet's private key
- `recent_traded`: Cooldown period between trades (in seconds)
- `cooldown`: Retry cooldown for failed orders (in seconds)
- `take_profit`: Take profit threshold (e.g., 0.03 for 3%)
- `stop_loss`: Stop loss threshold (e.g., -0.025 for -2.5%)

## Bot Structure

![Bot Architecture Diagram](diagram.png)

1. **State Management**
   - Global variables for tracking trades and prices
   - Price history management
   - Active trade tracking

2. **Trading Logic**
   - Price spike detection
   - Order placement with retries
   - Take-profit and stop-loss management
   - USDC allowance management

3. **Main Loop**
   - Price updates
   - Trade detection
   - Position management
   - API credential refresh

## Running the Bot

```bash
python main.py
```

## Important Notes

- Ensure sufficient USDC balance in your wallet
- Monitor the bot's logs in `bot_log.txt`
- The bot maintains $1 worth of shares when selling
- Trades are executed with a $3 unit size(you can adjust it)
- API credentials are refreshed hourly

## Safety Features

- Automatic retry mechanism for failed orders
- USDC balance checks before trades
- Error handling and logging
- Transaction receipt verification 

## Contact Me

