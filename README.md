# 🚀 Alts-Scraper

**Professional cryptocurrency futures & derivatives data collection pipeline using the Coinalyze API.**

Collect historical and real-time data for the top crypto tokens across multiple exchanges, including Open Interest, Funding Rates, Liquidations, Long/Short Ratios, and OHLCV data.

---

## 📊 Features

- **Multi-Exchange Support**: Aggregated data + 9 individual exchanges (Binance, Bybit, OKX, Deribit, Bitget, Gate.io, Huobi, Kraken, BitMEX)
- **Comprehensive Metrics**: 6 data endpoints per token
  - Open Interest (OHLC in USD)
  - Funding Rate (OHLC)
  - Predicted Funding Rate (OHLC)
  - Long/Short Ratio (ratio + quantities)
  - Liquidations (longs/shorts in USD)
  - OHLCV (price, volume, transactions)
- **Normalized Architecture**: Asset narratives and filtering status are decoupled from time-series data into a dedicated cache.
- **Top 50 Tokens**: Automatically fetches top tokens by market cap, filtering out stablecoins and wrapped assets via official CoinGecko taxonomies.
- **Persistent Metadata**: Shared cache (`data/asset_metadata.csv`) ensures high performance and consistent narrative selection.
- **Database Ready**: Complete PostgreSQL/Supabase schema for Spot, Futures, and Metadata.

---

## 🛠️ Installation

### Prerequisites
- Python 3.9+
- Coinalyze API key (free at [coinalyze.net/api](https://coinalyze.net/api/))

### Setup

```bash
# Clone the repository
git clone https://github.com/your-username/Alts-scraper.git
cd Alts-scraper

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env and add your COINALYZE_API_KEY
```

---

## 🚀 Usage

### Basic Usage

```bash
# Default: All exchanges, top 50 tokens
python alts_scraper.py

# Specific exchanges
python alts_scraper.py --exchanges binance,bybit,okx

# Limited tokens for testing
python alts_scraper.py --top 10 --exchanges binance

# Custom date range
python alts_scraper.py --start 2023-01-01 --end-days-ago 1

# Skip OHLCV data (if you have price data elsewhere)
python alts_scraper.py --skip-ohlcv
```

### CLI Options

| Option | Default | Description |
|--------|---------|-------------|
| `--top` | 50 | Number of top tokens by market cap |
| `--exchanges` | all | Comma-separated exchanges or 'all' |
| `--start` | 2017-01-01 | Start date (YYYY-MM-DD) |
| `--end-days-ago` | 1 | End date as N days ago |
| `--output-dir` | data | Output directory |
| `--skip-ohlcv` | false | Skip OHLCV price data |
| `--skip-merge` | false | Skip merging into existing files |

### Available Exchanges

| ID | Exchange | Symbol Format (Spot) | Symbol Format (Futures) |
|----|----------|----------------------|-------------------------|
| `binance` | Binance | `BTCUSDT` | `BTCUSDT_PERP.A` |
| `bybit` | Bybit | `BTCUSDT` | `BTCUSDT_PERP.3` |
| `okx` | OKX | `BTC-USDT` | `BTC-USDT-SWAP.6` |
| `aggregated`| All-in-one| - | `BTCUSDT_PERP` |

---

## 📁 Output Structure

```
data/
├── asset_metadata.csv        # Global cache for narratives & filtering
├── spot/                     # Spot OHLCV data
│   ├── binance/
│   └── bybit/
└── coinalyze/                # Futures metrics data
    ├── aggregated/
    ├── binance/
    └── bybit/
```

### CSV Columns

#### Asset Metadata (`asset_metadata.csv`)
| Column | Description |
|--------|-------------|
| `symbol` | Base asset symbol (e.g. BTC) |
| `narrative` | Selected significant category from CoinGecko |
| `is_filtered` | 1 if the asset is a stablecoin/wrapped/staked token |

#### Market Data (Spot & Futures)
| Category | Columns |
|----------|---------|
| **Metadata** | `date`, `symbol`, `exchange` |
| **Open Interest** | `oi_usd_open`, `oi_usd_high`, `oi_usd_low`, `oi_usd_close` (Futures) |
| **Funding Rate**| `funding_open`, `funding_high`, `funding_low`, `funding_close` (Futures) |
| **Liquidations**| `liq_longs`, `liq_shorts`, `liq_total` (Futures) |
| **OHLCV** | `price_open`, `price_high`, `price_low`, `price_close`, `volume_base`, `volume_usd` |
| **Microstructure**| `buy_volume_base`, `sell_volume_base`, `volume_delta`, `txn_count`, `buy_txn_count` |

---

## 🗄️ Database Schema

The project is designed to integrate with **Supabase (PostgreSQL)**.

### Architecture Visualization

```mermaid
erDiagram
    EXCHANGES ||--o{ SYMBOLS : hosts
    EXCHANGES ||--o{ FUTURES_METRICS : contains
    EXCHANGES ||--o{ SPOT_DATA : contains
    ASSET_METADATA ||--o{ FUTURES_METRICS : categorizes
    ASSET_METADATA ||--o{ SPOT_DATA : categorizes

    EXCHANGES {
        int id PK
        string name "binance, bybit, okx"
        string code "A, 6, 3"
        string display_name
    }

    SYMBOLS {
        int id PK
        string base_asset "BTC, ETH"
        string symbol "BTCUSDT_PERP.A"
        int exchange_id FK
    }

    ASSET_METADATA {
        string symbol PK "Base Asset (BTC)"
        string narrative "DeFi, AI, L1"
        boolean is_filtered "Stable/Wrapped"
    }

    FUTURES_METRICS {
        date date PK
        string symbol PK
        string exchange PK
        decimal oi_usd_close
        decimal funding_close
        decimal ls_ratio
        decimal liq_total
        decimal volume_delta
        bigint txn_count
    }

    SPOT_DATA {
        date date PK
        string symbol PK
        string exchange PK
        decimal price_close
        decimal volume_usd
        decimal volume_delta
        bigint txn_count
    }
```

### Tables Reference

| Table | Description | Key Features |
|-------|-------------|--------------|
| `exchanges` | Exchange metadata | Coinalyze/Native mapping codes. |
| `asset_metadata` | Asset categorization | Normalized narratives & filtering status. |
| `futures_daily_metrics` | Hybrid Futures data | 31 columns, Smart Sourcing (Native + Coinalyze). |
| `spot_daily_ohlcv` | Spot market data | Includes CVD Delta and transaction counts. |

### Schema Features
- **Smart Upsert**: Handles incremental updates without duplication via `ON CONFLICT`.
- **Materialized Views**: Includes `mv_aggregated_by_asset` for cross-exchange analysis.
- **Trading Analytics**: `mv_trading_metrics` provides derived data like OI Change % and Range %.
- **Supabase Optimized**: Pre-configured for RLS and authenticated read access.

---

### Key Data Columns (Futures & Spot)

| Category | Columns |
|----------|---------|
| **Metadata** | `date`, `symbol`, `exchange` |
| **Open Interest** | `oi_usd_open`, `oi_usd_high`, `oi_usd_low`, `oi_usd_close` |
| **Funding Rate**| `funding_open`, `funding_high`, `funding_low`, `funding_close` |
| **Liquidations**| `liq_longs`, `liq_shorts`, `liq_total` |
| **OHLCV** | `price_open`, `price_high`, `price_low`, `price_close`, `volume_base`, `volume_usd` |
| **Metrics** | `ls_ratio`, `txn_count`, `buy_txn_count`, **`volume_delta`** (Buy-Sell) |

---

## 🔧 Configuration

### Environment Variables

```env
# Required
COINALYZE_API_KEY=your_api_key_here

# Optional (for database integration)
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your_service_role_key
DATABASE_URL=postgresql://user:password@host:5432/database
```

---

## 📜 License

MIT License - see [LICENSE](./LICENSE) for details.

---

## 🤝 Contributing

Contributions are welcome! Please open an issue or submit a pull request.

---

## 📞 Support

- **Issues**: [GitHub Issues](https://github.com/your-username/Alts-scraper/issues)
- **Documentation**: [Coinalyze API Docs](https://coinalyze.net/api-docs/)
