# Polymarket Trading Bot

A Python-based trading bot for Polymarket that automatically detects price spikes and executes trades based on configurable parameters.

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
PK=your_private_key
recent_traded=60
cooldown=5
take_profit=0.03
stop_loss=-0.025
```

## Configuration Parameters

- `PK`: Your wallet's private key
- `recent_traded`: Cooldown period between trades (in seconds)
- `cooldown`: Retry cooldown for failed orders (in seconds)
- `take_profit`: Take profit threshold (e.g., 0.03 for 3%)
- `stop_loss`: Stop loss threshold (e.g., -0.025 for -2.5%)

## Bot Structure

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
python test.py
```

## Important Notes

- Ensure sufficient USDC balance in your wallet
- Monitor the bot's logs in `bot_log.txt`
- The bot maintains $1 worth of shares when selling
- Trades are executed with a $3 unit size
- API credentials are refreshed hourly

## Safety Features

- Automatic retry mechanism for failed orders
- USDC balance checks before trades
- Error handling and logging
- Transaction receipt verification 