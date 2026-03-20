"""DB初期化 + 過去データ一括取得（asyncio版）

改善点:
- asyncio + aiohttp で非同期I/O → スレッド版の数倍高速
- 複数日を同時にパイプライン処理（日ごとの逐次待ちを廃止）
- グローバルセマフォ + トークンバケットでレート制限
- キャッシュヒット時はネットワーク不要 → ディレイなし
- DB書き込みはバッチで実行
"""

import sys
import asyncio
import json
import logging
import re
import time
import warnings
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
warnings.filterwarnings("ignore", category=UserWarning)

from datetime import datetime

import aiohttp
from bs4 import BeautifulSoup
from rich.console import Console
from rich.progress import (
    Progress, SpinnerColumn, BarColumn, TextColumn,
    TimeElapsedColumn, TimeRemainingColumn, MofNCompleteColumn,
)

from config import (
    BOATRACE_BASE_URL, CACHE_DIR, REQUEST_HEADERS,
    VENUE_CODES,
)
from db.schema import init_db, get_connection, insert_race, insert_result, insert_racer
from data.race_calendar import get_month_dates
from data.scraper import BoatraceScraper  # パーサーを再利用

console = Console()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# ── 設定 ──
MAX_CONCURRENT = 20       # 同時接続数
REQUESTS_PER_SEC = 10.0   # 秒間リクエスト上限
BATCH_COMMIT_SIZE = 200   # N レースごとにcommit
CONNECTOR_LIMIT = 30      # aiohttp コネクションプール上限


class TokenBucket:
    """トークンバケットによるレート制限"""

    def __init__(self, rate: float, capacity: int = 1):
        self.rate = rate
        self.capacity = capacity
        self._tokens = float(capacity)
        self._last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last
            self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
            self._last = now

            if self._tokens < 1.0:
                wait = (1.0 - self._tokens) / self.rate
                await asyncio.sleep(wait)
                self._tokens = 0.0
                self._last = time.monotonic()
            else:
                self._tokens -= 1.0


class AsyncBoatraceFetcher:
    """非同期HTMLフェッチャー（キャッシュ + レート制限付き）"""

    def __init__(self):
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        self._semaphore = asyncio.Semaphore(MAX_CONCURRENT)
        self._rate_limiter = TokenBucket(REQUESTS_PER_SEC, capacity=5)
        self._session: aiohttp.ClientSession | None = None
        self._parser = BoatraceScraper()  # パーサーメソッド再利用用

        # 統計
        self.cache_hits = 0
        self.network_requests = 0

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(
                limit=CONNECTOR_LIMIT,
                ttl_dns_cache=300,
                enable_cleanup_closed=True,
            )
            timeout = aiohttp.ClientTimeout(total=30)
            self._session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
                headers=REQUEST_HEADERS,
            )
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    def _cache_path(self, url: str, ext: str = "html") -> Path:
        cache_key = re.sub(r"[^a-zA-Z0-9_\-]", "_", url.replace("https://", ""))
        return CACHE_DIR / f"{cache_key}.{ext}"

    def _json_cache_path(self, key: str) -> Path:
        return CACHE_DIR / f"{key}.json"

    async def fetch_html(self, url: str) -> str:
        """HTMLをフェッチ（キャッシュ優先）"""
        cache_file = self._cache_path(url)
        if cache_file.exists():
            self.cache_hits += 1
            return cache_file.read_text(encoding="utf-8")

        async with self._semaphore:
            await self._rate_limiter.acquire()
            session = await self._get_session()
            try:
                async with session.get(url) as resp:
                    html = await resp.text(encoding=None)
                    self.network_requests += 1
                    # キャッシュ保存
                    cache_file.write_text(html, encoding="utf-8")
                    return html
            except Exception as e:
                logger.warning("Fetch error %s: %s", url, e)
                raise

    def get_json_cache(self, key: str):
        path = self._json_cache_path(key)
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
        return None

    def set_json_cache(self, key: str, data):
        path = self._json_cache_path(key)
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    # ── スクレイピング（パーサーは同期のBoatraceScraperを再利用） ──

    async def scrape_race_list(self, date: str) -> list[str]:
        cache_key = f"race_list_{date}"
        cached = self.get_json_cache(cache_key)
        if cached:
            return cached

        url = f"{BOATRACE_BASE_URL}/owpc/pc/race/index?hd={date}"
        html = await self.fetch_html(url)
        soup = BeautifulSoup(html, "lxml")

        venue_codes = []
        for link in soup.select("a[href*='raceindex?jcd=']"):
            href = link.get("href", "")
            m = re.search(r"jcd=(\d{2})", href)
            if m and m.group(1) not in venue_codes:
                venue_codes.append(m.group(1))

        race_ids = []
        for vc in sorted(venue_codes):
            for rno in range(1, 13):
                race_ids.append(f"{date}_{vc}_{rno:02d}")

        self.set_json_cache(cache_key, race_ids)
        return race_ids

    async def scrape_race_result(self, race_id: str) -> dict | None:
        cache_key = f"race_result_{race_id}"
        cached = self.get_json_cache(cache_key)
        if cached:
            return cached

        parts = race_id.split("_")
        date, jcd, rno = parts[0], parts[1], int(parts[2])

        url = f"{BOATRACE_BASE_URL}/owpc/pc/race/raceresult?rno={rno}&jcd={jcd}&hd={date}"
        try:
            html = await self.fetch_html(url)
        except Exception:
            return None

        soup = BeautifulSoup(html, "lxml")

        race_info = self._parser._parse_race_info(soup, race_id, date, jcd, rno)
        results = self._parser._parse_result_table(soup, race_id)
        weather = self._parser._parse_weather(soup)
        race_info.update(weather)
        race_info["results"] = results
        race_info["racer_count"] = len(results)

        self.set_json_cache(cache_key, race_info)
        return race_info


async def process_date(
    fetcher: AsyncBoatraceFetcher,
    date: str,
    db_buffer: list,
):
    """1日分のレースを非同期で全取得 → db_bufferに追加"""
    race_ids = await fetcher.scrape_race_list(date)
    if not race_ids:
        return 0

    # 全レースを同時にスクレイピング（セマフォ+レートリミッターで制御）
    tasks = [fetcher.scrape_race_result(rid) for rid in race_ids]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    count = 0
    for race_id, data in zip(race_ids, results):
        if isinstance(data, Exception) or not data or not data.get("results"):
            continue
        db_buffer.append((race_id, date, data))
        count += 1

    return count


def flush_to_db(conn, buffer: list):
    """バッファ内のデータをDBに一括書き込み"""
    for race_id, date_str, data in buffer:
        try:
            race_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            race_info = {
                "race_id": race_id,
                "date": race_date,
                "venue_code": data.get("venue_code", ""),
                "venue_name": data.get("venue_name", ""),
                "race_number": data.get("race_number", 0),
                "race_name": data.get("race_name"),
                "grade": data.get("grade"),
                "distance": data.get("distance", 1800),
                "weather": data.get("weather"),
                "wind_direction": data.get("wind_direction"),
                "wind_speed": data.get("wind_speed"),
                "wave_height": data.get("wave_height"),
                "is_night": data.get("is_night", 0),
                "racer_count": data.get("racer_count", 6),
            }
            insert_race(conn, race_info)

            for r in data.get("results", []):
                if r.get("racer_id"):
                    insert_racer(conn, {
                        "racer_id": r["racer_id"],
                        "name": r.get("racer_name", ""),
                    })
                    insert_result(conn, r)
        except Exception as e:
            logger.warning("DB error %s: %s", race_id, e)

    conn.commit()
    buffer.clear()


async def collect_past_data(
    start_year: int = 2023,
    end_year: int = 2024,
    resume_from: str | None = None,
):
    init_db()
    conn = get_connection()

    # 自動再開: DB内の最終日から
    if not resume_from:
        row = conn.execute("SELECT MAX(date) d FROM races").fetchone()
        if row and row["d"]:
            resume_from = row["d"].replace("-", "")
            console.print(f"[yellow]DB最終日 {row['d']} から再開[/yellow]")

    # 対象日リスト作成
    all_dates = []
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            now = datetime.now()
            if year > now.year or (year == now.year and month > now.month):
                break
            all_dates.extend(get_month_dates(year, month))

    if resume_from:
        all_dates = [d for d in all_dates if d >= resume_from]

    if not all_dates:
        console.print("[green]全データ取得済みです[/green]")
        conn.close()
        return

    console.print(f"[bold]対象: {len(all_dates)}日間 ({all_dates[0]} ~ {all_dates[-1]})[/bold]")
    console.print(f"[bold]同時接続: {MAX_CONCURRENT}, レート: {REQUESTS_PER_SEC} req/s[/bold]\n")

    fetcher = AsyncBoatraceFetcher()
    total_races = 0
    db_buffer = []

    # 日単位をバッチにして並行処理
    # 1バッチ = 5日分を同時処理
    DAY_BATCH_SIZE = 5

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            day_task = progress.add_task("Days", total=len(all_dates))

            for batch_start in range(0, len(all_dates), DAY_BATCH_SIZE):
                batch_dates = all_dates[batch_start:batch_start + DAY_BATCH_SIZE]
                desc = f"{batch_dates[0]}~{batch_dates[-1]}"
                progress.update(day_task, description=desc)

                # 複数日を同時処理
                tasks = [
                    process_date(fetcher, date, db_buffer)
                    for date in batch_dates
                ]
                counts = await asyncio.gather(*tasks, return_exceptions=True)

                for c in counts:
                    if isinstance(c, int):
                        total_races += c

                # バッチごとにDB書き込み
                if db_buffer:
                    flush_to_db(conn, db_buffer)

                progress.update(day_task, advance=len(batch_dates))

        # 残りフラッシュ
        if db_buffer:
            flush_to_db(conn, db_buffer)

    finally:
        await fetcher.close()
        conn.close()

    console.print(f"\n[bold green]完了: {total_races}レース[/bold green]")
    console.print(
        f"[dim]Cache hits: {fetcher.cache_hits}, "
        f"Network requests: {fetcher.network_requests}[/dim]"
    )


def main():
    import argparse
    parser = argparse.ArgumentParser(description="過去データ一括取得（asyncio高速版）")
    parser.add_argument("--start", type=int, default=2023, help="開始年")
    parser.add_argument("--end", type=int, default=2024, help="終了年")
    parser.add_argument("--resume-from", type=str, default=None, help="再開日 (YYYYMMDD)")
    parser.add_argument("--concurrent", type=int, default=20, help="同時接続数 (default: 20)")
    parser.add_argument("--rate", type=float, default=10.0, help="秒間リクエスト上限 (default: 10)")
    args = parser.parse_args()

    global MAX_CONCURRENT, REQUESTS_PER_SEC
    MAX_CONCURRENT = args.concurrent
    REQUESTS_PER_SEC = args.rate

    asyncio.run(collect_past_data(args.start, args.end, args.resume_from))


if __name__ == "__main__":
    main()
