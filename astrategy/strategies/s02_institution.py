"""
Institution Association Strategy (机构关联策略)
================================================
Stocks held by the same institutions tend to move together.  When some
stocks in a cluster have rallied but others have not, the laggards may
catch up.

Signal logic
------------
1. Build a bipartite holding network:  Institution -> [stocks].
2. For a given stock, find its *peer group* — other stocks co-held by
   the same institutions, weighted by holding overlap ratio.
3. If the peer group's average N-day return exceeds the stock's own
   return by more than a threshold, flag a catch-up opportunity.
4. Cross-reference with quarter-over-quarter institutional position
   changes (new entries, exits, increases, decreases) to confirm the
   directional view.
"""

from __future__ import annotations

import logging
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from astrategy.data_collector.fundamental import FundamentalCollector
from astrategy.data_collector.market_data import MarketDataCollector
from astrategy.strategies.base import BaseStrategy, StrategySignal

logger = logging.getLogger(__name__)

_CST = timezone(timedelta(hours=8))

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _quarter_label(dt: datetime | None = None) -> str:
    """Return a quarter label like ``'20251231'`` for the most recent
    *completed* quarter relative to *dt* (defaults to now CST).

    Reporting periods end on 03-31, 06-30, 09-30, 12-31.  Because
    disclosures lag by ~1-2 months we step back one quarter from the
    calendar quarter that contains *dt*.
    """
    if dt is None:
        dt = datetime.now(tz=_CST)
    year, month = dt.year, dt.month
    # Calendar quarter end months: 3, 6, 9, 12
    q_end = [3, 6, 9, 12]
    # Find the latest completed quarter end that is at least 1 month ago
    candidates = []
    for qm in q_end:
        qdate = datetime(year, qm, 1, tzinfo=_CST) + timedelta(days=31)
        # last day of quarter month
        if qm in (3, 9, 12):
            qdate = datetime(year, qm, {3: 31, 9: 30, 12: 31}[qm], tzinfo=_CST)
        else:
            qdate = datetime(year, qm, 30, tzinfo=_CST)
        if qdate < dt - timedelta(days=30):
            candidates.append(qdate)
    # Also check previous year
    for qm in q_end:
        if qm in (3, 9, 12):
            qdate = datetime(year - 1, qm, {3: 31, 9: 30, 12: 31}[qm], tzinfo=_CST)
        else:
            qdate = datetime(year - 1, qm, 30, tzinfo=_CST)
        if qdate < dt - timedelta(days=30):
            candidates.append(qdate)

    if not candidates:
        # Fallback: last year Q4
        return f"{year - 1}1231"

    latest = max(candidates)
    return latest.strftime("%Y%m%d")


def _prev_quarter(quarter: str) -> str:
    """Given a quarter-end date string like ``'20251231'``, return the
    previous quarter-end date string."""
    dt = datetime.strptime(quarter, "%Y%m%d")
    year, month = dt.year, dt.month
    q_map = {3: (12, -1), 6: (3, 0), 9: (6, 0), 12: (9, 0)}
    if month in q_map:
        prev_month, year_delta = q_map[month]
        prev_year = year + year_delta
    else:
        # Shouldn't happen with valid quarter labels
        prev_month, prev_year = 12, year - 1

    day = {3: 31, 6: 30, 9: 30, 12: 31}[prev_month]
    return f"{prev_year}{prev_month:02d}{day:02d}"


def _extract_fund_house(fund_name: str) -> str:
    """Extract the fund management company name (基金公司) from a fund
    name.  E.g. ``'华夏上证50ETF'`` -> ``'华夏'``.

    Heuristic: take the first 2-4 Chinese characters before common
    suffixes like 基金, 资管, 资产.
    """
    # Try to match common patterns
    m = re.match(r"^(.{2,4}?)(基金|资管|资产|证券)", fund_name)
    if m:
        return m.group(1)
    # Fallback: first 2 characters
    return fund_name[:2] if len(fund_name) >= 2 else fund_name


# ---------------------------------------------------------------------------
# Strategy
# ---------------------------------------------------------------------------

class InstitutionStrategy(BaseStrategy):
    """Detect catch-up opportunities among institutionally co-held stocks."""

    # Tunable parameters
    CATCH_UP_THRESHOLD = 0.05       # 5 % gap triggers signal
    LOOKBACK_DAYS = 20              # N-day return window
    HOLDING_PERIOD = 15             # expected holding period for signal
    MIN_PEER_OVERLAP = 2            # minimum shared institutions for a peer
    MAX_PEERS = 20                  # cap on peer group size
    CONFIDENCE_BASE = 0.50          # baseline confidence
    CONFIDENCE_GAP_SCALE = 2.0      # confidence bonus per 1 % gap above threshold
    CONFIDENCE_INST_BONUS = 0.10    # bonus when position change confirms direction

    def __init__(self, signal_dir: Path | str | None = None) -> None:
        super().__init__(signal_dir)
        self._fundamental = FundamentalCollector()
        self._market = MarketDataCollector()
        # Holding network: institution_name -> set of stock codes
        self._inst_to_stocks: dict[str, set[str]] = {}
        # Reverse map: stock_code -> set of institution names
        self._stock_to_insts: dict[str, set[str]] = {}
        # Institution cluster: fund_house -> set of institution names
        self._fund_house_clusters: dict[str, set[str]] = {}
        # Stock name lookup
        self._stock_names: dict[str, str] = {}

    # ── identity ──────────────────────────────────────────────────────

    @property
    def name(self) -> str:
        return "institution_association"

    # ── 1. Build holding network ─────────────────────────────────────

    def build_holding_network(
        self,
        stock_codes: list[str],
        quarter: str | None = None,
    ) -> dict:
        """Build a bipartite institution-stock holding graph.

        Parameters
        ----------
        stock_codes:
            Universe of stock codes to scan.
        quarter:
            Quarter-end date label (``YYYYMMDD``).  Defaults to the most
            recently completed quarter.

        Returns
        -------
        dict
            ``{"institutions": int, "stocks": int, "edges": int}``
            summarising the graph size.
        """
        if quarter is None:
            quarter = _quarter_label()

        self._inst_to_stocks.clear()
        self._stock_to_insts.clear()
        self._fund_house_clusters.clear()

        for code in stock_codes:
            self._ingest_holders(code)
            self._ingest_fund_holdings(code)

        # Build fund-house clusters
        for inst_name in self._inst_to_stocks:
            house = _extract_fund_house(inst_name)
            self._fund_house_clusters.setdefault(house, set()).add(inst_name)

        total_edges = sum(len(v) for v in self._inst_to_stocks.values())
        summary = {
            "institutions": len(self._inst_to_stocks),
            "stocks": len(self._stock_to_insts),
            "edges": total_edges,
            "fund_houses": len(self._fund_house_clusters),
        }
        logger.info("Holding network built: %s", summary)
        return summary

    def _ingest_holders(self, code: str) -> None:
        """Add top-10 shareholders of *code* to the network."""
        df = self._fundamental.get_top10_holders(code)
        if df.empty:
            return

        # Identify the holder name column
        name_col = None
        for candidate in ("股东名称", "名称"):
            if candidate in df.columns:
                name_col = candidate
                break
        if name_col is None and len(df.columns) >= 1:
            name_col = df.columns[0]
        if name_col is None:
            return

        for _, row in df.iterrows():
            inst_name = str(row[name_col]).strip()
            if not inst_name or inst_name == "nan":
                continue
            # Skip natural persons (individual shareholders)
            if _is_individual(inst_name):
                continue
            self._inst_to_stocks.setdefault(inst_name, set()).add(code)
            self._stock_to_insts.setdefault(code, set()).add(inst_name)

    def _ingest_fund_holdings(self, code: str) -> None:
        """Add mutual-fund holders of *code* to the network.

        Handles two formats:
        - New aggregate format (stock_report_fund_hold): per-stock stats with
          持股变化 and 持有基金家数 columns.  Creates synthetic institution nodes
          so that stocks with similar fund activity are linked as peers.
        - Old per-fund format (stock_fund_stock_holder): one row per fund.
        """
        df = self._fundamental.get_fund_holdings(code)
        if df.empty:
            return

        # ── New aggregate format ──────────────────────────────────────
        if "持股变化" in df.columns and "持有基金家数" in df.columns:
            row = df.iloc[0]
            change = str(row.get("持股变化", "")).strip()
            raw_count = row.get("持有基金家数", 0)
            try:
                fund_count = int(raw_count)
            except (ValueError, TypeError):
                fund_count = 0

            # Bucket by fund count to create peer groups with similar coverage
            if fund_count >= 500:
                bucket = "500p"
            elif fund_count >= 100:
                bucket = "100p"
            elif fund_count >= 20:
                bucket = "20p"
            elif fund_count >= 5:
                bucket = "5p"
            else:
                bucket = "1p"

            # Synthetic institution represents: change direction + fund breadth
            if change in ("增仓", "新进"):
                inst_name = f"fund_acc_{bucket}"
            elif change in ("减仓", "退出"):
                inst_name = f"fund_red_{bucket}"
            else:
                inst_name = f"fund_stb_{bucket}"

            self._inst_to_stocks.setdefault(inst_name, set()).add(code)
            self._stock_to_insts.setdefault(code, set()).add(inst_name)
            return

        # ── Old per-fund format ───────────────────────────────────────
        name_col = None
        for candidate in ("基金名称", "基金简称"):
            if candidate in df.columns:
                name_col = candidate
                break
        if name_col is None and len(df.columns) >= 2:
            name_col = df.columns[1]
        if name_col is None:
            return

        for _, row in df.iterrows():
            fund_name = str(row[name_col]).strip()
            if not fund_name or fund_name == "nan":
                continue
            self._inst_to_stocks.setdefault(fund_name, set()).add(code)
            self._stock_to_insts.setdefault(code, set()).add(fund_name)

    # ── 2. Find peer groups ──────────────────────────────────────────

    def find_peer_groups(self, stock_code: str) -> list[dict]:
        """Find stocks co-held by the same institutions as *stock_code*.

        Returns
        -------
        list[dict]
            Each dict has ``{"stock_code": str, "overlap": int,
            "overlap_ratio": float}`` sorted by overlap descending.
            ``overlap`` is the number of shared institutions;
            ``overlap_ratio`` is overlap / total institutions holding
            *stock_code*.
        """
        my_insts = self._stock_to_insts.get(stock_code, set())
        if not my_insts:
            return []

        peer_counter: Counter[str] = Counter()
        for inst in my_insts:
            for peer_code in self._inst_to_stocks.get(inst, set()):
                if peer_code != stock_code:
                    peer_counter[peer_code] += 1

        # Filter by minimum overlap and cap the list
        my_total = len(my_insts)
        peers = []
        for peer_code, overlap in peer_counter.most_common():
            if overlap < self.MIN_PEER_OVERLAP:
                break
            peers.append({
                "stock_code": peer_code,
                "overlap": overlap,
                "overlap_ratio": round(overlap / my_total, 4),
            })
            if len(peers) >= self.MAX_PEERS:
                break

        return peers

    # ── 3. Compute catch-up signal ───────────────────────────────────

    def compute_catch_up_signal(
        self,
        stock_code: str,
        peers: list[str],
        days: int | None = None,
    ) -> dict:
        """Calculate the return gap between *stock_code* and its peers.

        Parameters
        ----------
        stock_code:
            Target stock.
        peers:
            List of peer stock codes.
        days:
            Lookback window in calendar days (converted to trading days
            internally).  Defaults to ``LOOKBACK_DAYS``.

        Returns
        -------
        dict
            ``{"self_return": float, "peer_avg_return": float,
            "catch_up_gap": float, "is_opportunity": bool}``
        """
        if days is None:
            days = self.LOOKBACK_DAYS

        now = datetime.now(tz=_CST)
        start = (now - timedelta(days=days + 15)).strftime("%Y%m%d")
        end = now.strftime("%Y%m%d")

        self_ret = self._calc_return(stock_code, start, end, days)

        peer_returns: list[float] = []
        for pc in peers:
            r = self._calc_return(pc, start, end, days)
            if r is not None:
                peer_returns.append(r)

        if not peer_returns or self_ret is None:
            return {
                "self_return": self_ret or 0.0,
                "peer_avg_return": 0.0,
                "catch_up_gap": 0.0,
                "is_opportunity": False,
            }

        peer_avg = sum(peer_returns) / len(peer_returns)
        gap = peer_avg - self_ret

        return {
            "self_return": round(self_ret, 6),
            "peer_avg_return": round(peer_avg, 6),
            "catch_up_gap": round(gap, 6),
            "is_opportunity": abs(gap) > self.CATCH_UP_THRESHOLD,
        }

    def _calc_return(
        self, code: str, start: str, end: str, days: int
    ) -> float | None:
        """Calculate the trailing return for *code* over the last *days*
        trading days within the [start, end] window."""
        df = self._market.get_daily_quotes(code, start, end)
        if df.empty or len(df) < 2:
            return None
        # Use the last `days` rows (trading days)
        df = df.tail(days + 1).reset_index(drop=True)
        if len(df) < 2:
            return None

        close_col = "收盘" if "收盘" in df.columns else df.columns[2]
        try:
            first_close = float(df[close_col].iloc[0])
            last_close = float(df[close_col].iloc[-1])
            if first_close == 0:
                return None
            return (last_close - first_close) / first_close
        except (ValueError, TypeError):
            return None

    # ── 4. Detect position changes ───────────────────────────────────

    def detect_position_changes(self, stock_code: str) -> dict:
        """Compare current vs previous quarter top-10 holders to detect
        institutional position changes.

        Returns
        -------
        dict
            ``{"new_entries": list, "exits": list, "increases": list,
            "decreases": list, "net_sentiment": str, "detail_count":
            {"new": int, "exit": int, "increase": int, "decrease": int}}``
        """
        current_q = _quarter_label()
        prev_q = _prev_quarter(current_q)

        curr_df = self._fundamental.get_top10_holders(stock_code)
        # We use the same API for both quarters — the df may contain
        # multiple reporting periods.  Filter if a date column exists.
        curr_holders = self._extract_holder_set(curr_df, current_q)
        prev_holders = self._extract_holder_set(curr_df, prev_q)

        # If we cannot distinguish periods, treat all rows as current
        # and return a simplified result.
        if not curr_holders and not prev_holders:
            curr_holders = self._extract_holder_set(curr_df)

        new_entries = list(curr_holders - prev_holders)
        exits = list(prev_holders - curr_holders)

        # Holding quantity changes
        curr_qty = self._extract_holder_qty(curr_df, current_q)
        prev_qty = self._extract_holder_qty(curr_df, prev_q)

        increases: list[str] = []
        decreases: list[str] = []
        for name in curr_holders & prev_holders:
            cq = curr_qty.get(name, 0)
            pq = prev_qty.get(name, 0)
            if cq > pq:
                increases.append(name)
            elif cq < pq:
                decreases.append(name)

        net_positive = len(new_entries) + len(increases)
        net_negative = len(exits) + len(decreases)

        if net_positive > net_negative:
            sentiment = "increase"
        elif net_negative > net_positive:
            sentiment = "decrease"
        else:
            sentiment = "stable"

        return {
            "new_entries": new_entries,
            "exits": exits,
            "increases": increases,
            "decreases": decreases,
            "net_sentiment": sentiment,
            "detail_count": {
                "new": len(new_entries),
                "exit": len(exits),
                "increase": len(increases),
                "decrease": len(decreases),
            },
        }

    @staticmethod
    def _extract_holder_set(
        df: pd.DataFrame, quarter: str | None = None
    ) -> set[str]:
        """Extract unique holder names from *df*, optionally filtered to
        a reporting-period *quarter*."""
        if df.empty:
            return set()

        # Try to filter by quarter
        date_col = None
        for c in ("报告期", "截止日期", "公告日期"):
            if c in df.columns:
                date_col = c
                break

        if quarter and date_col:
            # Handle both string dates and datetime.date objects
            date_strs = df[date_col].astype(str).str.replace("-", "")
            mask = date_strs.str.startswith(quarter[:6])
            filtered = df[mask]
            if not filtered.empty:
                df = filtered

        name_col = None
        for c in ("股东名称", "名称"):
            if c in df.columns:
                name_col = c
                break
        if name_col is None and len(df.columns) >= 1:
            name_col = df.columns[0]
        if name_col is None:
            return set()

        return {
            str(n).strip()
            for n in df[name_col].dropna().unique()
            if str(n).strip() and str(n).strip() != "nan"
        }

    @staticmethod
    def _extract_holder_qty(
        df: pd.DataFrame, quarter: str | None = None
    ) -> dict[str, float]:
        """Extract holder -> quantity mapping from *df*."""
        if df.empty:
            return {}

        date_col = None
        for c in ("报告期", "截止日期", "公告日期"):
            if c in df.columns:
                date_col = c
                break
        if quarter and date_col:
            date_strs = df[date_col].astype(str).str.replace("-", "")
            mask = date_strs.str.startswith(quarter[:6])
            filtered = df[mask]
            if not filtered.empty:
                df = filtered

        name_col = None
        for c in ("股东名称", "名称"):
            if c in df.columns:
                name_col = c
                break
        qty_col = None
        for c in ("持股数量", "持股数", "数量"):
            if c in df.columns:
                qty_col = c
                break

        if name_col is None or qty_col is None:
            return {}

        result: dict[str, float] = {}
        for _, row in df.iterrows():
            n = str(row[name_col]).strip()
            if not n or n == "nan":
                continue
            try:
                result[n] = float(row[qty_col])
            except (ValueError, TypeError):
                pass
        return result

    # ── 5. Main run ──────────────────────────────────────────────────

    def run(self, stock_codes: list[str] | None = None) -> list[StrategySignal]:
        """Run the institution association strategy across the given
        universe.

        Parameters
        ----------
        stock_codes:
            List of 6-digit stock codes.  If ``None``, uses a small
            default set (CSI-300 constituent sampling not implemented
            here).

        Returns
        -------
        list[StrategySignal]
        """
        if stock_codes is None:
            logger.warning(
                "InstitutionStrategy.run() called without stock_codes; "
                "pass an explicit universe."
            )
            return []

        # Populate stock names
        self._load_stock_names(stock_codes)

        # Step 1: build network
        logger.info("Building holding network for %d stocks ...", len(stock_codes))
        self.build_holding_network(stock_codes)

        # Step 2-5: per-stock analysis
        signals: list[StrategySignal] = []
        for code in stock_codes:
            try:
                sigs = self.run_single(code)
                signals.extend(sigs)
            except Exception as exc:
                logger.error("run_single(%s) failed: %s", code, exc, exc_info=True)

        logger.info(
            "InstitutionStrategy produced %d signals for %d stocks",
            len(signals),
            len(stock_codes),
        )
        return signals

    # ── 6. Single-stock run ──────────────────────────────────────────

    def run_single(self, stock_code: str) -> list[StrategySignal]:
        """Run institution strategy for one stock.

        If the holding network has not been built yet, the peer group
        will be empty and no signal is generated.
        """
        stock_name = self._stock_names.get(stock_code, stock_code)

        # Find peers
        peer_info = self.find_peer_groups(stock_code)
        if not peer_info:
            logger.debug("No peers found for %s; skipping.", stock_code)
            return []

        peer_codes = [p["stock_code"] for p in peer_info]

        # Catch-up signal
        catch_up = self.compute_catch_up_signal(stock_code, peer_codes)

        # Position changes
        pos_change = self.detect_position_changes(stock_code)

        # Decide direction
        gap = catch_up["catch_up_gap"]
        sentiment = pos_change["net_sentiment"]
        inst_count = len(self._stock_to_insts.get(stock_code, set()))

        if not catch_up["is_opportunity"]:
            # No meaningful gap — skip
            return []

        # Determine direction
        if gap > 0 and sentiment in ("increase", "stable"):
            direction = "long"
        elif gap < 0 and sentiment in ("decrease", "stable"):
            direction = "short"
        elif gap > self.CATCH_UP_THRESHOLD and sentiment == "decrease":
            # Gap is strongly positive but institutions are leaving —
            # conflicting signal → neutral
            direction = "neutral"
        elif gap < -self.CATCH_UP_THRESHOLD and sentiment == "increase":
            # Gap is negative but institutions are accumulating — could
            # be a value play; cautious long.
            direction = "long"
        else:
            direction = "neutral"

        # Confidence
        confidence = self.CONFIDENCE_BASE
        # Scale by how large the gap is beyond threshold
        excess = abs(gap) - self.CATCH_UP_THRESHOLD
        confidence += min(excess * self.CONFIDENCE_GAP_SCALE, 0.30)
        # Bonus when position change confirms direction
        if (direction == "long" and sentiment == "increase") or (
            direction == "short" and sentiment == "decrease"
        ):
            confidence += self.CONFIDENCE_INST_BONUS
        confidence = max(0.0, min(1.0, confidence))

        # Expected return: assume partial gap closure
        expected_return = abs(gap) * 0.5 if direction != "neutral" else 0.0

        # Build metadata
        top_peers = peer_codes[:5]
        metadata: Dict[str, Any] = {
            "peer_stocks": top_peers,
            "peer_avg_return": catch_up["peer_avg_return"],
            "self_return": catch_up["self_return"],
            "catch_up_gap": catch_up["catch_up_gap"],
            "institution_count": inst_count,
            "net_position_change": sentiment,
            "position_detail": pos_change["detail_count"],
            "peer_overlap_top5": peer_info[:5],
        }

        reasoning_parts = [
            f"{stock_name}({stock_code}): ",
            f"peer group avg {catch_up['peer_avg_return']:.2%} vs self {catch_up['self_return']:.2%} "
            f"(gap {gap:+.2%}). ",
            f"Institutions: {inst_count} holders, sentiment={sentiment}. ",
        ]
        if direction == "long":
            reasoning_parts.append(
                f"Catch-up opportunity: lagging peers by {gap:.2%} with "
                f"institutional support."
            )
        elif direction == "short":
            reasoning_parts.append(
                f"Relative weakness: outperformed peers by {abs(gap):.2%} while "
                f"institutions reducing."
            )
        else:
            reasoning_parts.append("Conflicting signals — neutral stance.")

        signal = StrategySignal(
            strategy_name=self.name,
            stock_code=stock_code,
            stock_name=stock_name,
            direction=direction,
            confidence=round(confidence, 4),
            expected_return=round(expected_return, 4),
            holding_period_days=self.HOLDING_PERIOD,
            reasoning="".join(reasoning_parts),
            metadata=metadata,
        )
        return [signal]

    # ── helpers ───────────────────────────────────────────────────────

    def _load_stock_names(self, codes: list[str]) -> None:
        """Populate ``_stock_names`` cache for display purposes."""
        for code in codes:
            if code in self._stock_names:
                continue
            info = self._fundamental.get_stock_info(code)
            name = info.get("股票简称", info.get("股票代码", code))
            self._stock_names[code] = str(name)


def _is_individual(name: str) -> bool:
    """Heuristic: return True if *name* looks like a natural person
    rather than an institutional holder."""
    # Chinese personal names are typically 2-4 characters with no
    # company-like suffixes
    if len(name) > 6:
        return False
    institutional_keywords = (
        "公司", "基金", "银行", "保险", "证券", "集团", "资管",
        "资产", "信托", "投资", "管理", "有限", "股份", "中心",
        "社保", "汇金", "财务",
    )
    for kw in institutional_keywords:
        if kw in name:
            return False
    # If it's short and has no institutional keywords, likely a person
    if len(name) <= 4:
        return True
    return False
