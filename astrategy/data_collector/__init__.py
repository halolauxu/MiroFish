"""
A-Share Strategy Data Collector Layer
=====================================
Provides unified data collection interfaces using akshare for:
- Market data (K-lines, realtime quotes, north flow, money flow)
- Fundamental data (financials, holders, PE ratios)
- Announcements (company disclosures with importance filtering)
- News (company news, industry news, hot topics)
- Macro data (PMI, CPI, GDP, M2, LPR)
- Research data (analyst ratings, consensus forecasts)
"""

from astrategy.data_collector.market_data import MarketDataCollector
from astrategy.data_collector.fundamental import FundamentalCollector
from astrategy.data_collector.announcement import AnnouncementCollector
from astrategy.data_collector.news import NewsCollector
from astrategy.data_collector.macro import MacroCollector
from astrategy.data_collector.research import ResearchCollector

__all__ = [
    "MarketDataCollector",
    "FundamentalCollector",
    "AnnouncementCollector",
    "NewsCollector",
    "MacroCollector",
    "ResearchCollector",
]
