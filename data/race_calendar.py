"""レースカレンダー取得"""

import re
import time
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

from config import BOATRACE_BASE_URL, REQUEST_HEADERS, SCRAPE_DELAY


def get_today_and_tomorrow_dates() -> list[str]:
    """今日と明日の日付をYYYYMMDD形式で返す"""
    today = datetime.now()
    dates = [today.strftime("%Y%m%d")]
    tomorrow = today + timedelta(days=1)
    dates.append(tomorrow.strftime("%Y%m%d"))
    return dates


def get_kaisai_venues_for_date(date: str) -> list[str]:
    """指定日の開催場コードを取得

    boatrace.jpのトップページから開催場を抽出。
    戻り値: ["01", "05", "12", ...] 場コードのリスト
    """
    url = f"{BOATRACE_BASE_URL}/owpc/pc/race/index?hd={date}"
    time.sleep(SCRAPE_DELAY)
    resp = requests.get(url, headers=REQUEST_HEADERS, timeout=30)
    resp.encoding = resp.apparent_encoding or "utf-8"
    soup = BeautifulSoup(resp.text, "lxml")

    venue_codes = []
    for link in soup.select("a[href*='raceindex']"):
        href = link.get("href", "")
        m = re.search(r"jcd=(\d{2})", href)
        if m and m.group(1) not in venue_codes:
            venue_codes.append(m.group(1))

    return sorted(venue_codes)


def get_race_ids_for_date(date: str) -> list[str]:
    """指定日のレースID一覧を取得"""
    from data.scraper import BoatraceScraper
    scraper = BoatraceScraper()
    return scraper.scrape_race_list(date)


def get_upcoming_race_ids() -> dict[str, list[str]]:
    """今日・明日のレースIDを日付ごとに取得"""
    dates = get_today_and_tomorrow_dates()
    result = {}
    for date in dates:
        race_ids = get_race_ids_for_date(date)
        if race_ids:
            result[date] = race_ids
    return result


def get_month_dates(year: int, month: int) -> list[str]:
    """指定月の全日付をYYYYMMDD形式で返す"""
    from calendar import monthrange
    _, days = monthrange(year, month)
    return [f"{year}{month:02d}{d:02d}" for d in range(1, days + 1)]
