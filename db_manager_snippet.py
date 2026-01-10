class DatabaseManager:
    """Handles communication with Supabase (PostgreSQL)."""
    def __init__(self, db_url: Optional[str] = None):
        if not db_url:
            db_url = os.getenv("DATABASE_URL")
        self.db_url = db_url
        self.enabled = bool(db_url)
        if self.enabled:
            print("[DB] Supabase Integration Enabled.")
        else:
            print("[DB] Supabase Integration Disabled (DATABASE_URL missing).")

    def upsert_futures_metrics(self, df: pd.DataFrame):
        """Batch upsert futures metrics into DB."""
        if not self.enabled or df.empty: return
        
        conn = None
        try:
            conn = psycopg2.connect(self.db_url)
            cur = conn.cursor()
            
            # Prepare data tuples
            # Function signature:
            # upsert_futures_daily_metrics(date, symbol, exchange, oi_open, oi_high, oi_low, oi_close,
            # funding_open, funding_high, funding_low, funding_close,
            # pred_funding_open, pred_funding_high, pred_funding_low, pred_funding_close,
            # liq_longs, liq_shorts, long_short_ratio, longs_qty, shorts_qty,
            # ls_acc_global, ls_acc_top, ls_pos_top)

            for _, row in df.iterrows():
                cur.execute("""
                    SELECT upsert_futures_daily_metrics(
                        %s, %s, %s, 
                        %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, 
                        %s, %s, %s,
                        %s, %s, %s
                    )
                """, (
                    row.get('date'), row.get('symbol'), row.get('exchange'),
                    row.get('oi_usd_open'), row.get('oi_usd_high'), row.get('oi_usd_low'), row.get('oi_usd_close'),
                    row.get('funding_open'), row.get('funding_high'), row.get('funding_low'), row.get('funding_close'),
                    row.get('pred_funding_open'), row.get('pred_funding_high'), row.get('pred_funding_low'), row.get('pred_funding_close'),
                    row.get('liq_longs'), row.get('liq_shorts'),
                    row.get('ls_ratio'), row.get('longs_qty'), row.get('shorts_qty'),
                    row.get('ls_acc_global'), row.get('ls_acc_top'), row.get('ls_pos_top')
                ))
            
            conn.commit()
            print(f"    [DB] Upserted {len(df)} rows.")
            
        except Exception as e:
            print(f"    [DB ERROR] Upsert failed: {e}")
            if conn: conn.rollback()
        finally:
            if conn: conn.close()

    def get_last_data_date(self, symbol: str, exchange: str) -> Optional[datetime]:
        """Get the last stored date for a symbol/exchange in the DB."""
        if not self.enabled: return None
        conn = None
        try:
            conn = psycopg2.connect(self.db_url)
            cur = conn.cursor()
            cur.execute("SELECT MAX(date) FROM futures_daily_metrics WHERE symbol = %s AND exchange = %s", (symbol, exchange))
            res = cur.fetchone()
            if res and res[0]: return res[0]
            return None
        except Exception as e:
            print(f"    [DB INFO] Could not fetch last date for {symbol}: {e}")
            return None
        finally:
             if conn: conn.close()
             
    def get_all_asset_metadata(self) -> pd.DataFrame:
        """Fetch all asset metadata from DB."""
        if not self.enabled: return pd.DataFrame()
        conn = None
        try:
            conn = psycopg2.connect(self.db_url)
            query = "SELECT symbol, narrative, is_filtered FROM asset_metadata"
            df = pd.read_sql(query, conn)
            return df
        except Exception as e:
            print(f"    [DB INFO] Could not fetch metadata: {e}")
            return pd.DataFrame()
        finally:
            if conn: conn.close()

    def upsert_asset_metadata(self, symbol: str, narrative: str, is_filtered: int):
        """Upsert asset metadata into asset_metadata table."""
        if not self.enabled: return
        conn = None
        try:
            conn = psycopg2.connect(self.db_url)
            cur = conn.cursor()
            cur.execute("SELECT upsert_asset_metadata(%s, %s, %s)", (symbol, narrative, bool(is_filtered)))
            conn.commit()
        except Exception as e:
            print(f"    [DB ERROR] Metadata upsert failed for {symbol}: {e}")
            if conn: conn.rollback()
        finally:
            if conn: conn.close()

