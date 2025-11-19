import logging
from typing import Callable, Optional, List, Tuple
import requests


def fetch_gap_from_binance(
    symbol: str,
    interval: str,
    gap_start: int,
    gap_end: int,
    interval_ms: int,
    upsert_klines_to_zset: Callable[[str, str, List[list]], Tuple[int, Optional[int], Optional[int]]],
    update_klines_meta_after_insert: Callable[[str, str, Optional[int], Optional[int], int], None],
    gap_logger: logging.Logger,
    app_logger: logging.Logger,
    now_ms: Optional[int] = None,
) -> None:
    """
    Fetch historical klines for a single [gap_start, gap_end] interval from Binance
    and persist them into Redis via the provided callbacks.

    This function is stateless w.r.t. Redis and logging so it can be executed
    safely in parallel threads for different gaps or (symbol, interval) pairs.
    """
    if now_ms is not None and gap_end > now_ms:
        gap_end = now_ms

    original_gap_start = gap_start
    original_gap_end = gap_end

    # Skip trivial or inverted gaps
    if gap_start >= gap_end:
        app_logger.debug(
            "Skipping trivial/future gap for symbol=%s interval=%s "
            "original_start=%s original_end=%s clamped_start=%s clamped_end=%s",
            symbol,
            interval,
            original_gap_start,
            original_gap_end,
            gap_start,
            gap_end,
        )
        return

    gap_logger.info(
        "Filling gap for symbol=%s interval=%s start=%s end=%s",
        symbol,
        interval,
        gap_start,
        gap_end,
    )

    start_time = gap_start

    while start_time < gap_end:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": start_time,
            "limit": 1000,
        }
        try:
            resp = requests.get(
                "https://api.binance.com/api/v3/klines",
                params=params,
                timeout=10,
            )
            resp.raise_for_status()
            new_klines = resp.json()
            for kline in new_klines:
                kline[0] = (kline[0] // interval_ms) * interval_ms
        except Exception:
            app_logger.exception(
                "Error fetching klines from Binance for gap fill "
                "symbol=%s interval=%s start_time=%s",
                symbol,
                interval,
                start_time,
            )
            break

        if not new_klines:
            # No data for this portion of the gap; stop trying this gap.
            app_logger.debug(
                "No klines returned for gap segment: symbol=%s interval=%s "
                "gap_start=%s gap_end=%s segment_start=%s",
                symbol,
                interval,
                gap_start,
                gap_end,
                start_time,
            )
            break

        try:
            added_count, batch_earliest, batch_latest = upsert_klines_to_zset(
                symbol, interval, new_klines
            )
            update_klines_meta_after_insert(
                symbol,
                interval,
                batch_earliest,
                batch_latest,
                added_count,
            )
            if added_count:
                gap_logger.info(
                    "Gap fill batch: symbol=%s interval=%s added_count=%d "
                    "batch_earliest=%s batch_latest=%s",
                    symbol,
                    interval,
                    added_count,
                    batch_earliest,
                    batch_latest,
                )
            else:
                app_logger.debug(
                    "Gap fill batch produced no new candles: symbol=%s interval=%s "
                    "batch_earliest=%s batch_latest=%s",
                    symbol,
                    interval,
                    batch_earliest,
                    batch_latest,
                )
        except Exception:
            app_logger.exception(
                "Failed to insert klines into zset during gap fill symbol=%s interval=%s",
                symbol,
                interval,
            )
            break

        try:
            start_time = ((new_klines[-1][0] // interval_ms) + 1) * interval_ms
        except Exception:
            # If response format is unexpected, stop processing this gap.
            break

        # Extra safety against tight loops on pathological responses
        if start_time <= gap_start:
            break