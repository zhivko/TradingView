# Technical indicator calculations

import numpy as np
import pandas as pd
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
import time
import pandas_ta as ta  # Required for technical analysis indicators
import logging
from config import AVAILABLE_INDICATORS, session

logger = logging.getLogger(__name__)

def get_timeframe_seconds(timeframe: str) -> int:
    multipliers = {"1m": 60, "5m": 300, "1h": 3600, "1d": 86400, "1w": 604800}
    return multipliers.get(timeframe, 3600)

def _prepare_dataframe(klines: List[Dict[str, Any]], open_interest_data: List[Dict[str, Any]]) -> Optional[pd.DataFrame]:
    if not klines:
        return None

    oi_count = len(open_interest_data) if open_interest_data else 0
    logger.debug(f"_prepare_dataframe: Starting with {len(klines)} klines and {oi_count} OI entries")

    # 🚨 CRITICAL: Check data freshness at input
    if klines:
        current_time = datetime.now(timezone.utc)
        latest_kline_timestamp = datetime.fromtimestamp(klines[-1]['time'], timezone.utc)
        earliest_kline_timestamp = datetime.fromtimestamp(klines[0]['time'], timezone.utc)
        time_diff_minutes = (current_time - latest_kline_timestamp).total_seconds() / 60

        logger.warning(f"🚨 DATA INPUT LAG: Latest kline is {time_diff_minutes:.1f} minutes old!")
        logger.warning(f"🚨 DATA INPUT LAG: Current time: {current_time.strftime('%H:%M:%S')}, Latest data: {latest_kline_timestamp.strftime('%H:%M:%S')}")
        logger.warning(f"🚨 DATA INPUT LAG: Data range: {earliest_kline_timestamp.strftime('%H:%M:%S')} to {latest_kline_timestamp.strftime('%H:%M:%S')}")

        if time_diff_minutes > 60:  # More than 1 hour lag
            logger.error(f"🚨 CRITICAL DATA LAG AT INPUT: {time_diff_minutes:.1f} minutes! Data pipeline issue detected!")

    # De-duplicate klines by time before DataFrame creation
    unique_klines_map: Dict[int, Dict[str, Any]] = {}
    for k in klines:
        unique_klines_map[k['time']] = k
    klines_deduplicated = sorted(list(unique_klines_map.values()), key=lambda x: x['time'])

    df_klines = pd.DataFrame(klines_deduplicated)
    df_klines['time'] = pd.to_datetime(df_klines['time'], unit='s')
    df_klines = df_klines.set_index('time')
    # Ensure correct column names for pandas_ta
    df_klines.rename(columns={'vol': 'volume'}, inplace=True)
    # pandas_ta expects lowercase column names for ohlcv
    df_klines.columns = [col.lower() for col in df_klines.columns]

    # Count NaN values in klines data
    nan_counts_klines = df_klines.isnull().sum()
    logger.debug(f"_prepare_dataframe: Klines DataFrame shape: {df_klines.shape}, NaN counts: {nan_counts_klines.to_dict()}")
    logger.debug(f"_prepare_dataframe: Klines timestamp range: {df_klines.index.min()} to {df_klines.index.max()}")

    # Process Open Interest data - but not for BTCDOM
    logger.debug(f"[DEBUG OI] Processing OI data: received {len(open_interest_data) if open_interest_data else 0} entries")
    df_oi = pd.DataFrame()
    if open_interest_data and len(open_interest_data) > 0:
        logger.debug(f"[DEBUG OI] Sample OI entries: {open_interest_data[:3]}")
        # De-duplicate OI data by time before DataFrame creation
        unique_oi_map: Dict[int, Dict[str, Any]] = {}
        for oi_entry in open_interest_data:
            unique_oi_map[oi_entry['time']] = oi_entry
        oi_data_deduplicated = sorted(list(unique_oi_map.values()), key=lambda x: x['time'])
        logger.debug(f"[DEBUG OI] After deduplication: {len(oi_data_deduplicated)} unique entries")

        df_oi = pd.DataFrame(oi_data_deduplicated)
        if 'time' in df_oi.columns:
            df_oi['time'] = pd.to_datetime(df_oi['time'], unit='s')
            df_oi = df_oi.set_index('time')
            df_oi.rename(columns={'open_interest': 'open_interest'}, inplace=True)

            # Count NaN values in OI data
            nan_counts_oi = df_oi.isnull().sum()
            logger.debug(f"_prepare_dataframe: OI DataFrame shape: {df_oi.shape}, NaN counts: {nan_counts_oi.to_dict()}")
            logger.debug(f"_prepare_dataframe: OI timestamp range: {df_oi.index.min()} to {df_oi.index.max()}")
            logger.debug(f"[DEBUG OI] OI DataFrame columns: {df_oi.columns.tolist()}")
            logger.debug(f"[DEBUG OI] OI values sample: {df_oi['open_interest'].head(3).tolist()}")

    # Merge klines and Open Interest data
    if not df_oi.empty:
        logger.debug(f"[DEBUG OI] Merging OI data with klines data")
        df_merged = pd.merge(df_klines, df_oi[['open_interest']], left_index=True, right_index=True, how='left')
        logger.debug(f"[DEBUG OI] Before fillna: OI null count = {df_merged['open_interest'].isnull().sum()}")
        df_merged['open_interest'] = df_merged['open_interest'].ffill().bfill().fillna(0)
        logger.debug(f"[DEBUG OI] After fillna: OI null count = {df_merged['open_interest'].isnull().sum()}")
        logger.debug(f"[DEBUG OI] Final OI values sample: {df_merged['open_interest'].head(3).tolist()}")

        # Count NaN values after merge
        nan_counts_merged = df_merged.isnull().sum()
        logger.debug(f"_prepare_dataframe: Merged DataFrame shape: {df_merged.shape}, NaN counts: {nan_counts_merged.to_dict()}")
        logger.debug(f"_prepare_dataframe: Merged timestamp range: {df_merged.index.min()} to {df_merged.index.max()}")

        return df_merged
    else:
        logger.debug(f"[DEBUG OI] No OI data available, setting open_interest to 0.0")
        df_klines['open_interest'] = 0.0

        logger.debug(f"_prepare_dataframe: Final DataFrame shape: {df_klines.shape}, NaN counts: {nan_counts_klines.to_dict()}")
        logger.debug(f"_prepare_dataframe: Final timestamp range: {df_klines.index.min()} to {df_klines.index.max()}")

        return df_klines

def _extract_results(df: pd.DataFrame, columns: List[str], original_time_index: pd.Series) -> Dict[str, Any]:
    """Extracts specified columns and aligns with original time index, preserving ALL timestamps."""
    data_dict: Dict[str, Any] = {"t": []}

    # CRITICAL FIX: Ensure we preserve ALL timestamps from original_time_index
    # Create a DataFrame that maintains the exact same index as original_time_index
    aligned_df = pd.DataFrame(index=original_time_index)
    aligned_df.index.name = 'time'

    # Add the calculated columns, filling missing timestamps with NaN
    for col in columns:
        if col in df.columns:
            # Check nulls before reindexing
            before_reindex_nulls = df[col].isnull().sum()
            logger.debug(f"_extract_results: Column '{col}' has {before_reindex_nulls} nulls before reindexing (out of {len(df[col])} total)")

            # Reindex to match original_time_index exactly, filling gaps with NaN
            aligned_df[col] = df[col].reindex(original_time_index, method=None)

            # Check nulls after reindexing
            after_reindex_nulls = aligned_df[col].isnull().sum()
            logger.debug(f"_extract_results: Column '{col}' has {after_reindex_nulls} nulls after reindexing (out of {len(aligned_df[col])} total)")
            if after_reindex_nulls > before_reindex_nulls:
                logger.warning(f"_extract_results: Column '{col}' gained {after_reindex_nulls - before_reindex_nulls} nulls during reindexing!")
        else:
            # Column doesn't exist, fill with NaN
            aligned_df[col] = np.nan
            logger.warning(f"Column '{col}' not found in DataFrame, filling with NaN values")

    logger.debug(f"Indicator extraction: Original time index length: {len(original_time_index)}")
    logger.debug(f"Indicator extraction: Aligned DataFrame shape: {aligned_df.shape}")
    logger.debug(f"Indicator extraction: Columns: {columns}")

    # Count NaN values in aligned data
    nan_counts_aligned = aligned_df[columns].isnull().sum()
    logger.debug(f"Indicator extraction: NaN counts in aligned columns: {nan_counts_aligned.to_dict()}")
    logger.debug(f"Indicator extraction: Time range: {original_time_index.min()} to {original_time_index.max()}")

    # Extract timestamps - now guaranteed to be all original timestamps
    data_dict["t"] = (original_time_index.astype('int64') // 10**9).tolist()  # Convert ns to s

    # Count non-null values for each output key
    output_keys_info = {}
    for col in columns:
        # Ensure the key in data_dict is simplified (e.g., 'macd' instead of 'MACD_12_26_9')
        simple_col_name = col.lower()  # Start with the full lowercase name
        if "macdh" in col.lower():
            simple_col_name = "histogram"
        elif "macds" in col.lower():
            simple_col_name = "signal"
        elif "mac" in col.lower() and "macdh" not in col.lower() and "macds" not in col.lower():
            simple_col_name = "macd"  # Ensure 'macd' is prioritized
        elif "stochrsik" in col.lower():
            simple_col_name = "stoch_k"
        elif "stochrsid" in col.lower():
            simple_col_name = "stoch_d"
        elif "_sma14" in col.lower() and "rsi" in col.lower():
            simple_col_name = "rsi_sma14"
        elif "rsi" in col.lower() and "stoch" not in col.lower() and "_sma" not in col.lower():
            simple_col_name = "rsi"
        elif "open_interest" in col.lower():
            simple_col_name = "open_interest"
        elif "jma_up" in col.lower():
            simple_col_name = "jma_up"
        elif "jma_down" in col.lower():
            simple_col_name = "jma_down"
        elif "jma" in col.lower() and "jma_up" not in col.lower() and "jma_down" not in col.lower():
            simple_col_name = "jma"  # Add JMA
        elif "cto_upper" in col.lower():
            simple_col_name = "cto_upper"
        elif "cto_lower" in col.lower():
            simple_col_name = "cto_lower"
        elif "cto_trend" in col.lower():
            simple_col_name = "cto_trend"

        # Convert NaN to None for JSON compatibility, but PRESERVE all values including NaN
        if col in aligned_df.columns:
            raw_values = aligned_df[col].tolist()
            processed_values = [None if pd.isna(val) else val for val in raw_values]
            non_null_count = sum(1 for v in processed_values if v is not None)
            output_keys_info[simple_col_name] = f"{len(processed_values)} total, {non_null_count} non-null"
        else:
            # This shouldn't happen with our new logic, but handle it just in case
            processed_values = [None] * len(aligned_df)
            output_keys_info[simple_col_name] = f"{len(processed_values)} total, 0 non-null"

        data_dict[simple_col_name] = processed_values

    logger.debug(f"Indicator extraction: Final output structure - timestamps: {len(data_dict['t'])}, keys: {list(output_keys_info.keys())}")
    logger.debug(f"Indicator extraction: Output data info: {output_keys_info}")

    return data_dict

def calculate_open_interest(df_input: pd.DataFrame) -> Dict[str, Any]:
    logger.debug(f"[DEBUG OI] calculate_open_interest called with DataFrame shape: {df_input.shape}")
    logger.debug(f"[DEBUG OI] DataFrame columns: {df_input.columns.tolist()}")
    df = df_input.copy()
    original_time_index = df.index.to_series()
    oi_col = 'open_interest'  # This column should already exist from _prepare_dataframe
    if oi_col not in df.columns:
        logger.warning(f"Open Interest column '{oi_col}' not found in DataFrame. Cannot calculate. Returning aligned data with NAs.")
        logger.warning(f"[DEBUG OI] Available columns: {df.columns.tolist()}")
        # Return placeholder data aligned with ALL original timestamps with expected keys
        # Add empty column to df to ensure _extract_results includes the expected keys
        df[oi_col] = np.nan
        return _extract_results(df, [oi_col], original_time_index)

    logger.debug(f"[DEBUG OI] OI column exists, checking values...")
    logger.debug(f"[DEBUG OI] OI values sample: {df[oi_col].head(5).tolist()}")
    logger.debug(f"[DEBUG OI] OI null count: {df[oi_col].isnull().sum()}")
    logger.debug(f"[DEBUG OI] OI non-zero count: {(df[oi_col] != 0).sum()}")

    result = _extract_results(df, [oi_col], original_time_index)
    logger.debug(f"[DEBUG OI] calculate_open_interest result: {len(result.get('t', []))} timestamps, {len(result.get('open_interest', []))} values")
    logger.debug(f"[DEBUG OI] Result sample values: {result.get('open_interest', [])[:5]}")

    return result

def calculate_macd(df_input: pd.DataFrame, short_period: int, long_period: int, signal_period: int) -> Dict[str, Any]:
    df = df_input.copy()
    original_time_index = df.index.to_series()  # Keep original timestamps for alignment

    # Count NaN values in input data
    nan_counts_input = df.isnull().sum()
    logger.debug(f"MACD calculation: Input DataFrame shape: {df.shape}, NaN counts: {nan_counts_input.to_dict()}")
    logger.debug(f"MACD calculation: Input timestamp range: {df.index.min()} to {df.index.max()}")

    # CRITICAL: Ensure DataFrame maintains the exact same index throughout processing
    df_processed = df.copy()

    # Ensure volume is not None (replace with 0 if needed) - but keep NaN in OHLC for proper handling
    if 'volume' in df_processed.columns:
        df_processed['volume'] = df_processed['volume'].fillna(0)

    logger.debug(f"MACD calculation: Processing {len(df_processed)} points")
    logger.debug(f"MACD calculation: Processing timestamp range: {df_processed.index.min()} to {df_processed.index.max()}")

    # Check for nulls in input data before calculation
    input_nulls = df_processed.isnull().sum()
    logger.debug(f"MACD calculation: Input data null counts: {input_nulls.to_dict()}")

    if len(df_processed) < long_period + signal_period:
        logger.warning(f"🔍 MACD GAP CAUSE: Insufficient data for MACD calculation. Need at least {long_period + signal_period} points, got {len(df_processed)}. This creates a {((original_time_index.min() + pd.Timedelta(seconds=(long_period + signal_period) * get_timeframe_seconds('1h'))) - original_time_index.min()).total_seconds() / 86400:.1f} day gap.")
        # Return placeholder data aligned with ALL original timestamps with expected keys
        macd_col = f'MACD_{short_period}_{long_period}_{signal_period}'
        signal_col = f'MACDs_{short_period}_{long_period}_{signal_period}'
        hist_col = f'MACDh_{short_period}_{long_period}_{signal_period}'
        # Add empty columns to df_processed to ensure _extract_results includes the expected keys
        df_processed[macd_col] = np.nan
        df_processed[signal_col] = np.nan
        df_processed[hist_col] = np.nan
        return _extract_results(df_processed, [macd_col, signal_col, hist_col], original_time_index)

    try:
        logger.debug(f"MACD calculation: About to call pandas-ta MACD with fast={short_period}, slow={long_period}, signal={signal_period}")
        # Calculate MACD while preserving the DataFrame index
        df_processed.ta.macd(fast=short_period, slow=long_period, signal=signal_period, append=True)
        logger.debug(f"MACD calculation: pandas-ta MACD completed successfully")

        # Check for nulls immediately after pandas-ta calculation
        after_calc_nulls = df_processed.isnull().sum()
        logger.debug(f"MACD calculation: Null counts after pandas-ta: {after_calc_nulls.to_dict()}")

        # Verify the DataFrame still has the correct index after pandas-ta operation
        if not df_processed.index.equals(df.index):
            logger.warning("MACD calculation: DataFrame index changed during pandas-ta operation, realigning...")
            df_processed = df_processed.reindex(df.index)
            # Check nulls after reindexing
            after_reindex_nulls = df_processed.isnull().sum()
            logger.debug(f"MACD calculation: Null counts after reindexing: {after_reindex_nulls.to_dict()}")

    except Exception as e:
        logger.error(f"pandas-ta MACD calculation failed: {e}")
        logger.error(f"DataFrame info: shape={df_processed.shape}, columns={df_processed.columns.tolist()}")
        logger.error(f"Sample data: {df_processed.head(3).to_dict() if len(df_processed) > 0 else 'No data'}")
        # Return placeholder data aligned with ALL original timestamps with expected keys
        macd_col = f'MACD_{short_period}_{long_period}_{signal_period}'
        signal_col = f'MACDs_{short_period}_{long_period}_{signal_period}'
        hist_col = f'MACDh_{short_period}_{long_period}_{signal_period}'
        # Add empty columns to df_processed to ensure _extract_results includes the expected keys
        df_processed[macd_col] = np.nan
        df_processed[signal_col] = np.nan
        df_processed[hist_col] = np.nan
        return _extract_results(df_processed, [macd_col, signal_col, hist_col], original_time_index)

    macd_col = f'MACD_{short_period}_{long_period}_{signal_period}'
    signal_col = f'MACDs_{short_period}_{long_period}_{signal_period}'
    hist_col = f'MACDh_{short_period}_{long_period}_{signal_period}'

    # Check if columns were actually created by pandas_ta
    if not all(col in df_processed.columns for col in [macd_col, signal_col, hist_col]):
        logger.warning(f"MACD columns not found in DataFrame. Expected: {macd_col}, {signal_col}, {hist_col}. "
                      f"This might be due to insufficient data for the indicator periods. Available columns: {df_processed.columns.tolist()}")
        # Return placeholder data aligned with ALL original timestamps with expected keys
        # Add empty columns to df_processed to ensure _extract_results includes the expected keys
        df_processed[macd_col] = np.nan
        df_processed[signal_col] = np.nan
        df_processed[hist_col] = np.nan
        return _extract_results(df_processed, [macd_col, signal_col, hist_col], original_time_index)

    # DEBUG: Check DataFrame columns and lengths before extraction
    logger.debug(f"MACD DEBUG: DataFrame columns: {df_processed.columns.tolist()}")
    logger.debug(f"MACD DEBUG: MACD column '{macd_col}' exists: {macd_col in df_processed.columns}")
    logger.debug(f"MACD DEBUG: Signal column '{signal_col}' exists: {signal_col in df_processed.columns}")
    logger.debug(f"MACD DEBUG: Histogram column '{hist_col}' exists: {hist_col in df_processed.columns}")

    if macd_col in df_processed.columns:
        macd_non_null = df_processed[macd_col].notna().sum()
        logger.debug(f"MACD DEBUG: MACD column length: {len(df_processed[macd_col])}, non-null: {macd_non_null}")

    if signal_col in df_processed.columns:
        signal_non_null = df_processed[signal_col].notna().sum()
        logger.debug(f"MACD DEBUG: Signal column length: {len(df_processed[signal_col])}, non-null: {signal_non_null}")

    if hist_col in df_processed.columns:
        hist_non_null = df_processed[hist_col].notna().sum()
        logger.debug(f"MACD DEBUG: Histogram column length: {len(df_processed[hist_col])}, non-null: {hist_non_null}")

    # Check for length mismatch before extraction
    if all(col in df_processed.columns for col in [macd_col, signal_col, hist_col]):
        macd_length = len(df_processed[macd_col])
        signal_length = len(df_processed[signal_col])
        hist_length = len(df_processed[hist_col])

        lengths = [macd_length, signal_length, hist_length]
        if len(set(lengths)) > 1:  # Not all lengths are equal
            logger.warning(f"MACD LENGTH MISMATCH: MACD={macd_length}, Signal={signal_length}, Histogram={hist_length}")

            # Find the minimum length and truncate all to match
            min_length = min(lengths)
            logger.warning(f"MACD FIX: Truncating all series to {min_length} values")

            # Truncate the longer series to match the shortest one
            if macd_length > min_length:
                df_processed[macd_col] = df_processed[macd_col].iloc[:min_length]
            if signal_length > min_length:
                df_processed[signal_col] = df_processed[signal_col].iloc[:min_length]
            if hist_length > min_length:
                df_processed[hist_col] = df_processed[hist_col].iloc[:min_length]

            # Also truncate the DataFrame index to match
            df_processed = df_processed.iloc[:min_length]

    result = _extract_results(df_processed, [macd_col, signal_col, hist_col], original_time_index)

    # INFO LOGGING FOR TROUBLESHOOTING - Show actual values being returned
    # logger.info(f"🔍 MACD CALCULATION RESULT: short={short_period}, long={long_period}, signal={signal_period}, total_points={len(result.get('t', []))}")
    # logger.info(f"🔍 MACD VALUES SAMPLE (last 5): {[f'{v:.4f}' if v is not None else 'None' for v in result.get('macd', [])[-5:]]}")
    # logger.info(f"🔍 MACD SIGNAL VALUES SAMPLE (last 5): {[f'{v:.4f}' if v is not None else 'None' for v in result.get('signal', [])[-5:]]}")
    # logger.info(f"🔍 MACD HISTOGRAM VALUES SAMPLE (last 5): {[f'{v:.4f}' if v is not None else 'None' for v in result.get('histogram', [])[-5:]]}")
    # logger.info(f"🔍 MACD TIMESTAMP SAMPLE (last 5): {[datetime.fromtimestamp(ts, timezone.utc).strftime('%Y-%m-%d %H:%M:%S') for ts in result.get('t', [])[-5:]]}")

    # Count non-null values
    macd_values = result.get('macd', [])
    signal_values = result.get('signal', [])
    histogram_values = result.get('histogram', [])
    macd_non_null = sum(1 for v in macd_values if v is not None)
    signal_non_null = sum(1 for v in signal_values if v is not None)
    histogram_non_null = sum(1 for v in histogram_values if v is not None)
    logger.info(f"🔍 MACD NON-NULL VALUES: macd={macd_non_null}/{len(macd_values)}, signal={signal_non_null}/{len(signal_values)}, histogram={histogram_non_null}/{len(histogram_values)}")

    return result

def calculate_rsi(df_input: pd.DataFrame, period: int = 14) -> Dict[str, Any]:
    """
    Compute RSI + SMA‑14 of RSI in the same response dictionary.

    Returns
    -------
    Dict[str, Any]
        Dictionary with keys:
            * ``t`` – timestamps
            * ``rsi`` – raw RSI values
            * ``rsi_sma14`` – 14‑period moving average of RSI
    """
    df = df_input.copy()
    original_time_index = df.index.to_series()

    # Count NaN values in input data
    nan_counts_input = df.isnull().sum()
    logger.debug(f"RSI calculation: Input DataFrame shape: {df.shape}, NaN counts: {nan_counts_input.to_dict()}")
    logger.debug(f"RSI calculation: Input timestamp range: {df.index.min()} to {df.index.max()}")

    # CRITICAL: Ensure DataFrame maintains the exact same index throughout processing
    df_processed = df.copy()

    # Ensure volume is not None (replace with 0 if needed) - but keep NaN in OHLC for proper handling
    if 'volume' in df_processed.columns:
        df_processed['volume'] = df_processed['volume'].fillna(0)

    logger.debug(f"RSI calculation: Processing {len(df_processed)} points")
    logger.debug(f"RSI calculation: Processing timestamp range: {df_processed.index.min()} to {df_processed.index.max()}")

    if len(df_processed) < period + 14:  # Need enough data for RSI + SMA
        logger.warning(f"🔍 RSI GAP CAUSE: Insufficient data for RSI calculation. Need at least {period + 14} points, got {len(df_processed)}. This creates a {((original_time_index.min() + pd.Timedelta(seconds=(period + 14) * get_timeframe_seconds('1h'))) - original_time_index.min()).total_seconds() / 86400:.1f} day gap.")
        # Return placeholder data aligned with ALL original timestamps with expected keys
        rsi_col = f"RSI_{period}"
        sma_col = f"RSI_{period}_sma14"
        # Add empty columns to df_processed to ensure _extract_results includes the expected keys
        df_processed[rsi_col] = np.nan
        df_processed[sma_col] = np.nan
        return _extract_results(df_processed, [rsi_col, sma_col], original_time_index)

    try:
        # 1️⃣ Compute the raw RSI while preserving the DataFrame index
        df_processed.ta.rsi(length=period, append=True)

        # Verify the DataFrame still has the correct index after pandas-ta operation
        if not df_processed.index.equals(df.index):
            logger.warning("RSI calculation: DataFrame index changed during pandas-ta operation, realigning...")
            df_processed = df_processed.reindex(df.index)

    except Exception as e:
        logger.error(f"pandas-ta RSI calculation failed: {e}")
        # Return placeholder data aligned with ALL original timestamps with expected keys
        rsi_col = f"RSI_{period}"
        sma_col = f"RSI_{period}_sma14"
        # Add empty columns to df_processed to ensure _extract_results includes the expected keys
        df_processed[rsi_col] = np.nan
        df_processed[sma_col] = np.nan
        return _extract_results(df_processed, [rsi_col, sma_col], original_time_index)

    rsi_col = f"RSI_{period}"
    if rsi_col not in df_processed.columns:
        logger.warning(f"RSI column '{rsi_col}' not found – maybe not enough data")
        # Return placeholder data aligned with ALL original timestamps with expected keys
        sma_col = f"RSI_{period}_sma14"
        # Add empty columns to df_processed to ensure _extract_results includes the expected keys
        df_processed[rsi_col] = np.nan
        df_processed[sma_col] = np.nan
        return _extract_results(df_processed, [rsi_col, sma_col], original_time_index)

    # 2️⃣ Compute SMA‑14 of that RSI
    sma_col = f"RSI_{period}_sma14"
    df_processed[sma_col] = df_processed[rsi_col].rolling(window=14).mean()

    # DEBUG: Check DataFrame columns and lengths before extraction
    logger.debug(f"RSI DEBUG: DataFrame columns: {df_processed.columns.tolist()}")
    logger.debug(f"RSI DEBUG: RSI column '{rsi_col}' exists: {rsi_col in df_processed.columns}")
    logger.debug(f"RSI DEBUG: SMA column '{sma_col}' exists: {sma_col in df_processed.columns}")

    if rsi_col in df_processed.columns:
        rsi_non_null = df_processed[rsi_col].notna().sum()
        logger.debug(f"RSI DEBUG: RSI column length: {len(df_processed[rsi_col])}, non-null: {rsi_non_null}")

    if sma_col in df_processed.columns:
        sma_non_null = df_processed[sma_col].notna().sum()
        logger.debug(f"RSI DEBUG: SMA column length: {len(df_processed[sma_col])}, non-null: {sma_non_null}")

    # Check for length mismatch before extraction
    if rsi_col in df_processed.columns and sma_col in df_processed.columns:
        rsi_length = len(df_processed[rsi_col])
        sma_length = len(df_processed[sma_col])
        if rsi_length != sma_length:
            logger.warning(f"RSI LENGTH MISMATCH: RSI has {rsi_length} values, SMA has {sma_length} values")

            # Find the minimum length and truncate both to match
            min_length = min(rsi_length, sma_length)
            logger.warning(f"RSI FIX: Truncating both RSI and SMA to {min_length} values")

            # Truncate the longer series to match the shorter one
            if rsi_length > min_length:
                df_processed[rsi_col] = df_processed[rsi_col].iloc[:min_length]
            if sma_length > min_length:
                df_processed[sma_col] = df_processed[sma_col].iloc[:min_length]

            # Also truncate the DataFrame index to match
            df_processed = df_processed.iloc[:min_length]

    # 3️⃣ Return both columns via _extract_results
    result = _extract_results(df_processed, [rsi_col, sma_col], original_time_index)

    # INFO LOGGING FOR TROUBLESHOOTING - Show actual values being returned
    # logger.info(f"🔍 RSI CALCULATION RESULT: period={period}, total_points={len(result.get('t', []))}")
    # logger.info(f"🔍 RSI VALUES SAMPLE (last 5): {[f'{v:.2f}' if v is not None else 'None' for v in result.get('rsi', [])[-5:]]}")
    # logger.info(f"🔍 RSI SMA14 VALUES SAMPLE (last 5): {[f'{v:.2f}' if v is not None else 'None' for v in result.get('rsi_sma14', [])[-5:]]}")
    # logger.info(f"🔍 RSI TIMESTAMP SAMPLE (last 5): {[datetime.fromtimestamp(ts, timezone.utc).strftime('%Y-%m-%d %H:%M:%S') for ts in result.get('t', [])[-5:]]}")

    # Count non-null values
    rsi_values = result.get('rsi', [])
    rsi_sma14_values = result.get('rsi_sma14', [])
    rsi_non_null = sum(1 for v in rsi_values if v is not None)
    rsi_sma14_non_null = sum(1 for v in rsi_sma14_values if v is not None)
    #logger.info(f"🔍 RSI NON-NULL VALUES: rsi={rsi_non_null}/{len(rsi_values)}, rsi_sma14={rsi_sma14_non_null}/{len(rsi_sma14_values)}")

    # logger.debug(f"RSI calculation completed: {len(result.get('t', []))} points")
    return result

def calculate_stoch_rsi(df_input: pd.DataFrame, rsi_period: int, stoch_period: int, k_period: int, d_period: int) -> Dict[str, Any]:
    df = df_input.copy()
    original_time_index = df.index.to_series()

    # Count NaN values before calculation
    nan_counts_before = df.isnull().sum()
    logger.debug(f"StochRSI calculation: Input DataFrame shape: {df.shape}, NaN counts: {nan_counts_before.to_dict()}")
    logger.debug(f"StochRSI calculation: Input timestamp range: {df.index.min()} to {df.index.max()}")

    # CRITICAL: Ensure DataFrame maintains the exact same index throughout processing
    df_processed = df.copy()

    # Check if we have sufficient data before attempting calculation
    min_required_points = rsi_period + stoch_period + k_period + d_period
    logger.debug(f"StochRSI validation: need {min_required_points} points, have {len(df_processed)}")

    if len(df_processed) < min_required_points:
        logger.warning(f"🔍 STOCHRSI INSUFFICIENT DATA: Need at least {min_required_points} points, got {len(df_processed)}. "
                      f"This will cause {min_required_points - len(df_processed)} NaN values at the start.")
        # Return placeholder data aligned with ALL original timestamps with expected keys
        k_col = f'STOCHRSIk_{stoch_period}_{rsi_period}_{k_period}_{d_period}'
        d_col = f'STOCHRSId_{stoch_period}_{rsi_period}_{k_period}_{d_period}'
        # Add empty columns to df_processed to ensure _extract_results includes the expected keys
        df_processed[k_col] = np.nan
        df_processed[d_col] = np.nan
        return _extract_results(df_processed, [k_col, d_col], original_time_index)

    try:
        # Calculate StochRSI while preserving the DataFrame index
        df_processed.ta.stochrsi(rsi_length=rsi_period, length=stoch_period, k=k_period, d=d_period, append=True)

        # Verify the DataFrame still has the correct index after pandas-ta operation
        if not df_processed.index.equals(df.index):
            logger.warning("StochRSI calculation: DataFrame index changed during pandas-ta operation, realigning...")
            df_processed = df_processed.reindex(df.index)

    except Exception as e:
        logger.error(f"pandas-ta StochRSI calculation failed: {e}")
        # Return placeholder data aligned with ALL original timestamps with expected keys
        k_col = f'STOCHRSIk_{stoch_period}_{rsi_period}_{k_period}_{d_period}'
        d_col = f'STOCHRSId_{stoch_period}_{rsi_period}_{k_period}_{d_period}'
        # Add empty columns to df_processed to ensure _extract_results includes the expected keys
        df_processed[k_col] = np.nan
        df_processed[d_col] = np.nan
        return _extract_results(df_processed, [k_col, d_col], original_time_index)

    # Count NaN values after calculation
    nan_counts_after = df_processed.isnull().sum()
    logger.debug(f"StochRSI calculation: After calculation NaN counts: {nan_counts_after.to_dict()}")
    logger.debug(f"StochRSI calculation: Final timestamp range: {df_processed.index.min()} to {df_processed.index.max()}")

    # pandas-ta creates columns in the format: STOCHRSIk_{stoch_period}_{rsi_period}_{k_period}_{d_period}
    # For parameters rsi_period=60, stoch_period=10, k_period=10, d_period=10
    # pandas-ta creates: STOCHRSIk_10_60_10_10
    k_col = f'STOCHRSIk_{stoch_period}_{rsi_period}_{k_period}_{d_period}'
    d_col = f'STOCHRSId_{stoch_period}_{rsi_period}_{k_period}_{d_period}'

    # Check if columns were actually created by pandas_ta
    if k_col not in df_processed.columns or d_col not in df_processed.columns:
        logger.warning(f"Stochastic RSI columns '{k_col}' or '{d_col}' not found in DataFrame after calculation. "
                      f"This might be due to insufficient data for the indicator periods. "
                      f"Available columns: {df_processed.columns.tolist()}")
        # Return placeholder data aligned with ALL original timestamps with expected keys
        # Add empty columns to df_processed to ensure _extract_results includes the expected keys
        df_processed[k_col] = np.nan
        df_processed[d_col] = np.nan
        return _extract_results(df_processed, [k_col, d_col], original_time_index)

    # DEBUG: Check DataFrame columns and lengths before extraction
    logger.debug(f"STOCHRSI DEBUG: DataFrame columns: {df_processed.columns.tolist()}")
    logger.debug(f"STOCHRSI DEBUG: K column '{k_col}' exists: {k_col in df_processed.columns}")
    logger.debug(f"STOCHRSI DEBUG: D column '{d_col}' exists: {d_col in df_processed.columns}")

    if k_col in df_processed.columns:
        k_non_null = df_processed[k_col].notna().sum()
        logger.debug(f"STOCHRSI DEBUG: K column length: {len(df_processed[k_col])}, non-null: {k_non_null}")

    if d_col in df_processed.columns:
        d_non_null = df_processed[d_col].notna().sum()
        logger.debug(f"STOCHRSI DEBUG: D column length: {len(df_processed[d_col])}, non-null: {d_non_null}")

    # Check for length mismatch before extraction
    if k_col in df_processed.columns and d_col in df_processed.columns:
        k_length = len(df_processed[k_col])
        d_length = len(df_processed[d_col])
        if k_length != d_length:
            logger.warning(f"STOCHRSI LENGTH MISMATCH: K has {k_length} values, D has {d_length} values")

            # Find the minimum length and truncate both to match
            min_length = min(k_length, d_length)
            logger.warning(f"STOCHRSI FIX: Truncating both K and D to {min_length} values")

            # Truncate the longer series to match the shorter one
            if k_length > min_length:
                df_processed[k_col] = df_processed[k_col].iloc[:min_length]
            if d_length > min_length:
                df_processed[d_col] = df_processed[d_col].iloc[:min_length]

            # Also truncate the DataFrame index to match
            df_processed = df_processed.iloc[:min_length]

    result = _extract_results(df_processed, [k_col, d_col], original_time_index)

    # Count non-null values
    stoch_k_values = result.get('stoch_k', [])
    stoch_d_values = result.get('stoch_d', [])
    stoch_k_non_null = sum(1 for v in stoch_k_values if v is not None)
    stoch_d_non_null = sum(1 for v in stoch_d_values if v is not None)

    # 🚨 CRITICAL: Check for data lag
    current_time = datetime.now(timezone.utc)
    if result.get('t'):
        latest_data_timestamp = datetime.fromtimestamp(result['t'][-1], timezone.utc)
        time_diff_minutes = (current_time - latest_data_timestamp).total_seconds() / 60
        logger.warning(f"🚨 DATA LAG ALERT: Latest data is {time_diff_minutes:.1f} minutes old! Current time: {current_time.strftime('%H:%M:%S')}, Latest data: {latest_data_timestamp.strftime('%H:%M:%S')}")
        if time_diff_minutes > 60:  # More than 1 hour lag
            logger.error(f"🚨 CRITICAL DATA LAG: {time_diff_minutes:.1f} minutes! Check data source and WebSocket connection!")

    return result

def calculate_jma_indicator(df_input: pd.DataFrame, length: int = 7, phase: int = 50, power: int = 2) -> Dict[str, Any]:
    """Calculates the Jurik Moving Average (JMA) using the jurikIndicator.py module."""
    df = df_input.copy()
    original_time_index = df.index.to_series()

    # Count NaN values before calculation
    nan_counts_before = df.isnull().sum()
    logger.debug(f"JMA calculation: Input DataFrame shape: {df.shape}, NaN counts: {nan_counts_before.to_dict()}")
    logger.debug(f"JMA calculation: Input timestamp range: {df.index.min()} to {df.index.max()}")

    # CRITICAL: Ensure DataFrame maintains the exact same index throughout processing
    df_processed = df.copy()

    try:
        import jurikIndicator  # Import here to avoid circular dependency issues
        if not hasattr(jurikIndicator, 'calculate_jma'):
            logger.error("jurikIndicator.py does not have the 'calculate_jma' function. Returning empty subplots.")
            # Return placeholder data aligned with ALL original timestamps with expected keys
            # Add empty columns to df_processed to ensure _extract_results includes the expected keys
            df_processed['jma'] = np.nan
            df_processed['jma_up'] = np.nan
            df_processed['jma_down'] = np.nan
            return _extract_results(df_processed, ['jma', 'jma_up', 'jma_down'], original_time_index)

        jma_series = jurikIndicator.calculate_jma(df_processed['close'], length, phase, power)
        df_processed['jma'] = jma_series

        if 'jma' not in df_processed.columns:
            logger.warning("JMA column not found in DataFrame after calculation. Returning empty subplots.")
            # Return placeholder data aligned with ALL original timestamps with expected keys
            # Add empty columns to df_processed to ensure _extract_results includes the expected keys
            df_processed['jma'] = np.nan
            df_processed['jma_up'] = np.nan
            df_processed['jma_down'] = np.nan
            return _extract_results(df_processed, ['jma', 'jma_up', 'jma_down'], original_time_index)

        df_processed['jma_up'] = np.where(df_processed['jma'] > df_processed['jma'].shift(1), df_processed['jma'], np.nan)
        df_processed['jma_down'] = np.where(df_processed['jma'] < df_processed['jma'].shift(1), df_processed['jma'], np.nan)

        # Count NaN values after calculation
        nan_counts_after = df_processed.isnull().sum()
        logger.debug(f"JMA calculation: After calculation NaN counts: {nan_counts_after.to_dict()}")
        logger.debug(f"JMA calculation: Final timestamp range: {df_processed.index.min()} to {df_processed.index.max()}")

        return _extract_results(df_processed, ['jma', 'jma_up', 'jma_down'], original_time_index)

    except ImportError:
        logger.error("Could not import jurikIndicator.py module. Returning empty subplots.")
        # Return placeholder data aligned with ALL original timestamps with expected keys
        # Add empty columns to df_processed to ensure _extract_results includes the expected keys
        df_processed['jma'] = np.nan
        df_processed['jma_up'] = np.nan
        df_processed['jma_down'] = np.nan
        return _extract_results(df_processed, ['jma', 'jma_up', 'jma_down'], original_time_index)

def calculate_rsi_sma(df_input: pd.DataFrame, sma_period: int, rsi_values: List[float]) -> Dict[str, Any]:
    df = df_input.copy()
    original_time_index = df.index.to_series()  # Store original index

    if len(rsi_values) < len(df.index):  # RSI values are shorter, pad with NaNs
        padding = [np.nan] * (len(df.index) - len(rsi_values))
        aligned_rsi_values = rsi_values + padding
        logger.warning(f"calculate_rsi_sma: Padding rsi_values (from {len(rsi_values)} to {len(aligned_rsi_values)}) with NaNs to match DataFrame index.")
    elif len(rsi_values) > len(df.index):  # RSI values are longer, truncate
        aligned_rsi_values = rsi_values[:len(df.index)]
        logger.warning(f"calculate_rsi_sma: Truncating rsi_values (from {len(rsi_values)} to {len(aligned_rsi_values)}) to match DataFrame index.")
    else:
        aligned_rsi_values = rsi_values  # Lengths already match

    # Now, assign to the DataFrame
    df['rsi'] = aligned_rsi_values
    logger.debug(f"calculate_rsi_sma: Assigned RSI column of length {len(df['rsi'])} (index length: {len(df.index)}) to DataFrame.")

    df[f'RSI_SMA_{sma_period}'] = df['rsi'].rolling(window=sma_period).mean()
    return _extract_results(df, [f'RSI_SMA_{sma_period}'], original_time_index)

def find_buy_signals(df: pd.DataFrame) -> list:
    """
    Finds buy/sell signals based on RSI deviation followed by RSI SMA turnaround.

    New logic:
    1. Detect statistically significant RSI deviations from RSI SMA
    2. Track these as "pending signal candidates"
    3. Monitor RSI SMA slope changes:
       - If RSI SMA slope turns positive after deviation → BUY signal
       - If RSI SMA slope turns negative after deviation → SELL signal
    4. Include timeout mechanism (pending signals expire after 10 bars)
    """
    logger.debug("Starting find_buy_signals with RSI deviation + RSI SMA turnaround analysis")
    signals = []
    return signals
    pending_signals = []  # Track pending signal candidates: (index, deviation_z_score, deviation_points, rsi_value, sma_value)

    # Reset index for easy access
    df = df.reset_index()

    # Check if we have RSI data - use the specific column names from WebSocket mapping
    rsi_col = None
    sma_col = None

    # First, log what columns we actually have for debugging
    logger.debug(f"find_buy_signals: DataFrame columns: {list(df.columns)}")

    # The WebSocket maps RSI indicators to specific column names
    # RSI: 'RSI_14' (raw RSI) and 'RSI_14_sma14' (SMA14 of RSI)
    if 'RSI_14' in df.columns and 'RSI_14_sma14' in df.columns:
        rsi_col = 'RSI_14'
        sma_col = 'RSI_14_sma14'
        logger.debug(f"find_buy_signals: Using mapped RSI columns: {rsi_col}, {sma_col}")
    else:
        # Log available columns for debugging
        rsi_related_cols = [col for col in df.columns if 'rsi' in col.lower()]
        logger.warning(f"find_buy_signals: RSI columns not found. Expected 'RSI_14' and 'RSI_14_sma14', found RSI-related: {rsi_related_cols}, all columns: {list(df.columns)}")
        return signals

    logger.debug(f"Using RSI columns: rsi='{rsi_col}', sma='{sma_col}'")

    # Parameters for signal generation
    PENDING_SIGNAL_TIMEOUT = 10  # Bars after which pending signals expire
    SLOPE_CHANGE_CONFIRMATION = 2  # Bars to confirm slope change direction

    for i in range(len(df)):
        current_time = df['time'].iloc[i]

        # Get RSI and RSI_SMA14 values
        rsi_value = df[rsi_col].iloc[i]
        rsi_sma14_value = df[sma_col].iloc[i]

        # Skip if any value is NaN
        if pd.isna(rsi_value) or pd.isna(rsi_sma14_value):
            continue

        # Calculate RSI SMA slope (rate of change) - use at least 3 points for slope
        rsi_sma_slope = None
        if i >= 2:
            # Calculate slope using last 3 points
            try:
                x = np.array([-2, -1, 0])  # Last 3 bars relative to current
                y_vals = []
                for offset in [-2, -1, 0]:
                    if i + offset >= 0:
                        val = df[sma_col].iloc[i + offset]
                        if not pd.isna(val):
                            y_vals.append(val)
                        else:
                            break
                    else:
                        break

                if len(y_vals) >= 3:
                    y = np.array(y_vals[-3:])  # Take last 3 valid values
                    # Slope = mean of (y[i+1] - y[i]) for trend confirmation
                    slope_values = np.diff(y)
                    rsi_sma_slope = np.mean(slope_values)
                elif len(y_vals) >= 2:
                    # Fallback to simple slope with 2 points
                    rsi_sma_slope = y_vals[-1] - y_vals[-2]
            except Exception as e:
                logger.debug(f"Could not calculate slope at bar {i}: {e}")

        # DETECT STATISTICALLY SIGNIFICANT RSI DEVIATIONS
        deviation_points = rsi_sma14_value - rsi_value  # SMA - RSI (positive when RSI is below SMA)

        # Calculate statistical significance over lookback window
        lookback_window = min(50, i) if i > 20 else 20

        significant_deviation = False
        deviation_z_score = None

        if i >= lookback_window:
            rsi_deviations = []
            for lookback_idx in range(max(0, i - lookback_window), i + 1):
                rsi_val = df[rsi_col].iloc[lookback_idx]
                sma_val = df[sma_col].iloc[lookback_idx]
                if not (pd.isna(rsi_val) or pd.isna(sma_val)):
                    dev = sma_val - rsi_val
                    rsi_deviations.append(dev)

            if len(rsi_deviations) >= 20:
                try:
                    avg_deviation = np.mean(rsi_deviations[:-1])
                    std_deviation = np.std(rsi_deviations[:-1], ddof=1)

                    if std_deviation > 0:
                        deviation_z_score = (deviation_points - avg_deviation) / std_deviation
                        # Use 85% confidence level for deviation detection
                        significant_deviation = abs(deviation_z_score) > 1.44

                        if significant_deviation:
                            logger.info(f"STATISTICAL DEVIATION DETECTED at {current_time}: RSI deviation z-score={deviation_z_score:.2f}, "
                                      f"deviation={deviation_points:.2f}, historical_avg={avg_deviation:.2f}, historical_std={std_deviation:.2f}")
                except Exception as e:
                    logger.debug(f"Statistical calculation failed at bar {i}: {e}")

        # ADD SIGNIFICANT DEVIATIONS TO PENDING LIST
        if significant_deviation:
            pending_signals.append({
                'index': i,
                'z_score': deviation_z_score,
                'deviation_points': deviation_points,
                'rsi_value': rsi_value,
                'sma_value': rsi_sma14_value,
                'slope_at_detection': rsi_sma_slope
            })
            logger.debug(f"Added pending signal candidate at bar {i}: z-score={deviation_z_score:.2f}")

        # CHECK FOR SIGNAL CONFIRMATION THROUGH RSI TURNAROUND
        signals_to_remove = []

        for pending_signal in pending_signals:
            signal_index = pending_signal['index']
            bars_since_detection = i - signal_index

            # Check for timeout
            if bars_since_detection > PENDING_SIGNAL_TIMEOUT:
                logger.debug(f"Pending signal at bar {signal_index} expired (timeout)")
                signals_to_remove.append(pending_signal)
                continue

            # Need enough bars after detection to confirm slope change
            if bars_since_detection < SLOPE_CHANGE_CONFIRMATION:
                continue

            # Check if RSI SMA has turned around (slope direction change)
            slope_at_detection = pending_signal['slope_at_detection']

            if rsi_sma_slope is not None and slope_at_detection is not None:
                # Check for sign change in slope (direction change)
                slope_changed_positive = (slope_at_detection <= 0) and (rsi_sma_slope > 0)  # Negative to positive
                slope_changed_negative = (slope_at_detection >= 0) and (rsi_sma_slope < 0)  # Positive to negative

                if slope_changed_positive:
                    # RSI SMA turned from down/flat to up after significant deviation = BUY SIGNAL
                    timestamp = int(current_time.timestamp())
                    detection_timestamp = int(df['time'].iloc[signal_index].timestamp())
                    signals.append({
                        'timestamp': timestamp,
                        'price': df['close'].iloc[i],
                        'rsi': rsi_value,
                        'rsi_sma14': rsi_sma14_value,
                        'deviation_z_score': pending_signal['z_score'],
                        'deviation_points': pending_signal['deviation_points'],
                        'bars_since_deviation': bars_since_detection,
                        'slope_at_detection': slope_at_detection,
                        'slope_at_confirmation': rsi_sma_slope,
                        'detection_time': detection_timestamp,
                        'type': 'buy'
                    })
                    logger.info(f"BUY SIGNAL CONFIRMED at {current_time}: RSI SMA turnaround after {bars_since_detection} bars. "
                              f"Deviation z-score={pending_signal['z_score']:.2f}, slope: {slope_at_detection:.4f} → {rsi_sma_slope:.4f}")
                    signals_to_remove.append(pending_signal)

                elif slope_changed_negative:
                    # RSI SMA turned from up/flat to down after significant deviation = SELL SIGNAL
                    timestamp = int(current_time.timestamp())
                    detection_timestamp = int(df['time'].iloc[signal_index].timestamp())
                    signals.append({
                        'timestamp': timestamp,
                        'price': df['close'].iloc[i],
                        'rsi': rsi_value,
                        'rsi_sma14': rsi_sma14_value,
                        'deviation_z_score': pending_signal['z_score'],
                        'deviation_points': pending_signal['deviation_points'],
                        'bars_since_deviation': bars_since_detection,
                        'slope_at_detection': slope_at_detection,
                        'slope_at_confirmation': rsi_sma_slope,
                        'detection_time': detection_timestamp,
                        'type': 'sell'
                    })
                    logger.info(f"SELL SIGNAL CONFIRMED at {current_time}: RSI SMA turnaround after {bars_since_detection} bars. "
                              f"Deviation z-score={pending_signal['z_score']:.2f}, slope: {slope_at_detection:.4f} → {rsi_sma_slope:.4f}")
                    signals_to_remove.append(pending_signal)

        # Remove confirmed or expired pending signals
        for signal_to_remove in signals_to_remove:
            if signal_to_remove in pending_signals:
                pending_signals.remove(signal_to_remove)

        slope_str = f"{rsi_sma_slope:.4f}" if rsi_sma_slope is not None else "N/A"
        logger.debug(f"Bar {i} at {current_time}: RSI={rsi_value:.2f}, RSI_SMA={rsi_sma14_value:.2f}, "
                    f"Deviation={deviation_points:.2f}, SMA_slope={slope_str}, "
                    f"Pending_signals={len(pending_signals)}")

    logger.info(f"find_buy_signals completed: analyzed {len(df)} bars, found {len(signals)} signals "
               f"({len([s for s in signals if s['type'] == 'buy'])} buy, {len([s for s in signals if s['type'] == 'sell'])} sell)")
    return signals

def format_indicator_data_for_llm_as_dict(indicator_id: str, indicator_config_details: Dict[str, Any], data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Formats indicator data into a dictionary structure suitable for JSON embedding.
    data is like {'t': [...], 'value1': [...], 'value2': [...], 's': 'ok'}
    indicator_config_details is from AVAILABLE_INDICATORS
    """

    if not data or not data.get('t') or data.get('s') != 'ok':
        return {
            "indicator_name": indicator_config_details['name'],
            "params": indicator_config_details['params'],
            "status": "no_data",
            "error_message": "No valid data available for the selected range.",
            "values": []
        }

    # Determine column names from data keys, excluding 't', 's', 'errmsg'
    value_keys = [k for k in data.keys() if k not in ['t', 's', 'errmsg']]

    # Map internal keys to more readable names
    column_names_map = {
        "macd": "MACD", "signal": "Signal", "histogram": "Histogram",
        "rsi": "RSI",
        "rsi_sma14": "RSI SMA14",
        "open_interest": "OpenInterest",
        "jma": "JMA",  # Add JMA
        "stoch_k": "StochK", "stoch_d": "StochD",
        "cto_upper": "CTO Upper", "cto_lower": "CTO Lower", "cto_trend": "CTO Trend"
    }

    timestamps = data['t']
    values_by_key = {key: data[key] for key in value_keys}

    formatted_values = []
    for i, ts in enumerate(timestamps):
        dt_object = datetime.fromtimestamp(ts, timezone.utc)
        record: Dict[str, Any] = {"timestamp": dt_object.strftime('%Y-%m-%d %H:%M:%S')}
        for current_key in value_keys:
            value = values_by_key[current_key][i] if i < len(values_by_key[current_key]) else None  # Use None for N/A
            record[column_names_map.get(current_key, current_key.capitalize())] = value
        formatted_values.append(record)

    return {
        "indicator_name": indicator_config_details['name'],
        "params": indicator_config_details['params'],
        "status": "ok",
        "values": formatted_values
    }


def fetch_open_interest_from_bybit(symbol: str, interval: str, start_ts: int, end_ts: int) -> list[Dict[str, Any]]:
    """Fetches Open Interest data from Bybit."""

    #logger.info(f"Fetching Open Interest for {symbol} {interval} from {datetime.fromtimestamp(start_ts, timezone.utc)} to {datetime.fromtimestamp(end_ts, timezone.utc)}")
    all_oi_data: list[Dict[str, Any]] = []
    current_start = start_ts
    batch_count = 0
    total_entries_received = 0

    # Bybit's get_open_interest intervalTime parameter has specific values.
    # We assume TRADING_TIMEFRAME (e.g., "5m") maps to a valid intervalTime like "5min".
    # This mapping needs to be consistent with what Bybit's API expects for `intervalTime`.
    oi_interval_map = {"1m": "5min", "5m": "5min", "1h": "1h", "1d": "1d", "1w": "1d"} # "1w" might need "1d" OI
    bybit_oi_interval = oi_interval_map.get(interval, "5min") # Default to 5min if not found

    # Convert interval string to seconds for calculation (e.g., "5min" -> 300 seconds)
    oi_interval_seconds_map = {"5min": 300, "15min": 900, "30min": 1800, "1h": 3600, "4h": 14400, "1d": 86400}
    interval_seconds = oi_interval_seconds_map.get(bybit_oi_interval)
    if not interval_seconds:
        logger.error(f"Unsupported Open Interest interval for calculation: {bybit_oi_interval}")
        return []

    while current_start < end_ts:
        batch_count += 1
        # Bybit get_open_interest limit is 200 per request
        batch_end = min(current_start + (200 * interval_seconds) - 1, end_ts)

        try:
            response = session.get_open_interest(
                category="linear", # Assuming linear perpetuals
                symbol=symbol,
                intervalTime=bybit_oi_interval, # Use the string interval like "5min"
                start=current_start * 1000, # Bybit expects milliseconds
                end=batch_end * 1000,       # Bybit expects milliseconds
                limit=200
            )
        except Exception as e:
            logger.error(f"Bybit API request for Open Interest failed for {symbol} {interval} batch #{batch_count}: {e}")
            break

        if response.get("retCode") != 0:
            logger.error(f"Bybit API error for Open Interest {symbol} {interval} batch #{batch_count}: {response.get('retMsg', 'Unknown error')} (retCode: {response.get('retCode')})")
            break

        list_data = response.get("result", {}).get("list", [])
        if not list_data:
            logger.info(f"No more Open Interest data available from Bybit for {symbol} {interval} at batch #{batch_count}")
            break

        total_entries_received += len(list_data)

        # Data is usually newest first, reverse to get chronological order
        batch_oi = []
        for item in reversed(list_data):
            batch_oi.append({
                "time": int(item["timestamp"]) // 1000, # Convert ms to seconds
                "open_interest": float(item["openInterest"])
            })
        all_oi_data.extend(batch_oi)

        if not batch_oi or len(list_data) < 200: # No more data or last page
            logger.debug(f"Stopping Open Interest batch fetch for {symbol} {interval}: received {len(list_data)} entries (less than 200) or no data processed")
            break

        # Next query starts after the last fetched item's timestamp
        last_fetched_ts_in_batch = batch_oi[-1]["time"]
        current_start = last_fetched_ts_in_batch + interval_seconds

    all_oi_data.sort(key=lambda x: x["time"]) # Ensure chronological order
    #logger.info(f"Completed Bybit Open Interest fetch for {symbol} {interval}: {batch_count} batches, {total_entries_received} total entries received, {len(all_oi_data)} entries processed")
    return all_oi_data

def smma(series: pd.Series, length: int) -> pd.Series:
    """
    Calculate Smoothed Moving Average (SMMA) as implemented in Pine Script.
    SMMA provides exponential-like smoothing with a recursive formula.
    """
    smma_series = series.copy()
    smma_series.iloc[:length-1] = np.nan  # Not enough data for initial calculations

    for i in range(length-1, len(series)):
        if i == length - 1:
            # Initialize with Simple Moving Average
            smma_series.iloc[i] = series.iloc[i-length+1:i+1].mean()
        else:
            # Recursive calculation: SMMA = (SMMA[1] * (length - 1) + src) / length
            smma_series.iloc[i] = (smma_series.iloc[i-1] * (length - 1) + series.iloc[i]) / length

    return smma_series

def calculate_cto_line(df_input: pd.DataFrame, v1_period: int = 7, m1_period: int = 9,
                       m2_period: int = 11, v2_period: int = 13) -> Dict[str, Any]:
    """
    Calculate CTO Line (Larsson Line) indicator based on SMMA periods on HL2.
    Returns upper line (v1), lower line (v2), and trend signal.
    """
    df = df_input.copy()
    original_time_index = df.index.to_series()

    # Count NaN values in input data
    nan_counts_input = df.isnull().sum()
    logger.debug(f"CTO Line calculation: Input DataFrame shape: {df.shape}, NaN counts: {nan_counts_input.to_dict()}")
    logger.debug(f"CTO Line calculation: Input timestamp range: {df.index.min()} to {df.index.max()}")

    # CRITICAL: Ensure DataFrame maintains the exact same index throughout processing
    df_processed = df.copy()

    # Ensure volume is not None (set to 0 if needed) - but keep NaN in OHLC for proper handling
    if 'volume' in df_processed.columns:
        df_processed['volume'] = df_processed['volume'].fillna(0)

    logger.debug(f"CTO Line calculation: Processing {len(df_processed)} points")
    logger.debug(f"CTO Line calculation: Processing timestamp range: {df_processed.index.min()} to {df_processed.index.max()}")

    # Check if sufficient data
    max_period = max(v1_period, m1_period, m2_period, v2_period)
    if len(df_processed) < max_period:
        logger.warning(f"🔍 CTO LINE INSUFFICIENT DATA: Need at least {max_period} points, got {len(df_processed)}")
        df_processed['cto_upper'] = np.nan
        df_processed['cto_lower'] = np.nan
        df_processed['cto_trend'] = np.nan
        return _extract_results(df_processed, ['cto_upper', 'cto_lower', 'cto_trend'], original_time_index)

    # Calculate HL2
    hl2 = (df_processed['high'] + df_processed['low']) / 2

    # Calculate SMMAs
    v1 = smma(hl2, v1_period)  # Line 1 (Upper)
    v2 = smma(hl2, v2_period)  # Line 2 (Lower)
    m1 = smma(hl2, m1_period)  # Intermediate (not returned but used for logic)
    m2 = smma(hl2, m2_period)  # Intermediate (not returned but used for logic)

    # Add lines to dataframe
    df_processed['cto_upper'] = v1
    df_processed['cto_lower'] = v2

    # Trend logic as per Pine Script (conditions for coloring)
    # p2 = v1<m1 != v1<v2 or m2<v2 != v1<v2
    # p3 = not p2 and v1<v2
    # p1 = not p2 and not p3 (bullish: orange, neutral: silver, bearish: navy)

    # Calculate boolean conditions
    v1_lt_m1 = v1 < m1
    v1_lt_v2 = v1 < v2
    m2_lt_v2 = m2 < v2

    p2_bool = (v1_lt_m1 != v1_lt_v2) | (m2_lt_v2 != v1_lt_v2)
    p3_bool = (~p2_bool) & v1_lt_v2
    p1_bool = (~p2_bool) & (~p3_bool)

    # Trend: 0=bullish (p1), 1=neutral (p2), 2=bearish (p3)
    trend_series = pd.Series(index=df_processed.index, dtype=float)
    trend_series[p1_bool] = 0  # Bullish
    trend_series[p2_bool] = 1  # Neutral
    trend_series[p3_bool] = 2  # Bearish
    trend_series[~p1_bool & ~p2_bool & ~p3_bool] = np.nan  # Fallback

    df_processed['cto_trend'] = trend_series

    # Count NaN values after calculation
    nan_counts_after = df_processed.isnull().sum()
    logger.debug(f"CTO Line calculation: After calculation NaN counts: {nan_counts_after.to_dict()}")
    logger.debug(f"CTO Line calculation: Final timestamp range: {df_processed.index.min()} to {df_processed.index.max()}")

    result = _extract_results(df_processed, ['cto_upper', 'cto_lower', 'cto_trend'], original_time_index)

    # INFO LOGGING
    logger.info(f"🔍 CTO LINE CALCULATION RESULT: v1={v1_period}, m1={m1_period}, m2={m2_period}, v2={v2_period}, total_points={len(result.get('t', []))}")
    logger.info(f"🔍 CTO UPPER VALUES SAMPLE (last 5): {[f'{v:.2f}' if v is not None else 'None' for v in result.get('cto_upper', [])[-5:]]}")
    logger.info(f"🔍 CTO LOWER VALUES SAMPLE (last 5): {[f'{v:.2f}' if v is not None else 'None' for v in result.get('cto_lower', [])[-5:]]}")
    logger.info(f"🔍 CTO TREND VALUES SAMPLE (last 5): {[f'{v:.0f}' if v is not None else 'None' for v in result.get('cto_trend', [])[-5:]]}")
    logger.info(f"🔍 CTO TIMESTAMP SAMPLE (last 5): {[datetime.fromtimestamp(ts, timezone.utc).strftime('%Y-%m-%d %H:%M:%S') for ts in result.get('t', [])[-5:]]}")

    return result

def validate_indicator_data_alignment(test_result: Dict[str, Any], original_ohlc_length: int, indicator_name: str) -> bool:
    """
    Validates that indicator data has NO null values.

    Args:
        test_result: The indicator calculation result dictionary
        original_ohlc_length: The length of the original OHLC data (unused - no alignment check)
        indicator_name: Name of the indicator for logging

    Returns:
        bool: True if validation passes (no null values), False otherwise
    """
    if not test_result or not isinstance(test_result, dict):
        logger.error(f"❌ VALIDATION FAILED: {indicator_name} - Invalid result format")
        return False

    # Get all data series (excluding timestamps and status fields)
    data_keys = [k for k in test_result.keys() if k not in ['t', 's', 'errmsg']]

    if not data_keys:
        logger.error(f"❌ VALIDATION FAILED: {indicator_name} - No data series found")
        return False

    total_nulls = 0
    total_points = 0

    for key in data_keys:
        data_series = test_result.get(key, [])

        # Count null values
        null_count = sum(1 for v in data_series if v is None)
        total_nulls += null_count
        total_points += len(data_series)

        if null_count > 0:
            logger.warning(f"⚠️ VALIDATION WARNING: {indicator_name} - Data series '{key}' has {null_count} null values out of {len(data_series)} total")

    # Strict validation: NO null values allowed
    if total_nulls > 0:
        logger.error(f"❌ VALIDATION FAILED: {indicator_name} - Found {total_nulls} null values out of {total_points} total data points. All indicators must have 0 null values.")
        return False

    logger.info(f"✅ VALIDATION PASSED: {indicator_name} - All {total_points} data points are non-null across {len(data_keys)} series")
    return True
