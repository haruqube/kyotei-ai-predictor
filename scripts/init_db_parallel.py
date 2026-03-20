"""DB初期化 + 過去データ一括取得（並列版）

会場単位で並列スクレイピング → メインスレッドでDB書き込み。
キャッシュ済みデータはスキップするので、中断・再開に対応。
"""

import sys
import warnings
import threading
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

warnings.filterwarnings("ignore", category=UserWarning)

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn

from config import VENUE_CODES
from db.schema import init_db, get_connection, insert_race, insert_result, insert_racer
from data.scraper import BoatraceScraper
from data.race_calendar import get_month_dates

console = Console()

# 各スレッド用のスクレイパー (thread-local storage)
_thread_local = threading.local()

MAX_WORKERS = 4  # 並列数 (boatrace.jpへの配慮: 4並列 × 1.5s delay ≒ 2.7 req/s)


def _get_scraper() -> BoatraceScraper:
    """スレッドごとにスクレイパーインスタンスを生成"""
    if not hasattr(_thread_local, "scraper"):
        _thread_local.scraper = BoatraceScraper()
    return _thread_local.scraper


def _scrape_race(race_id: str) -> dict | None:
    """1レースの結果をスクレイピング（ワーカースレッド用）"""
    try:
        scraper = _get_scraper()
        data = scraper.scrape_race_result(race_id)
        if data and data.get("results"):
            return data
    except Exception as e:
        console.print(f"  [red]Scrape error {race_id}: {e}[/red]")
    return None


def _scrape_race_list(date: str) -> list[str]:
    """日付のレースリスト取得（メインスレッド）"""
    scraper = BoatraceScraper()
    return scraper.scrape_race_list(date)


def collect_past_data(start_year: int = 2023, end_year: int = 2024, resume_from: str | None = None):
    """過去データを並列取得してDBに保存

    Args:
        start_year: 開始年
        end_year: 終了年
        resume_from: 再開日 (YYYYMMDD形式)。この日以降のみ処理。
    """
    init_db()
    conn = get_connection()

    # 既に取得済みの最終日を確認
    if not resume_from:
        row = conn.execute("SELECT MAX(date) d FROM races").fetchone()
        if row and row["d"]:
            # 最終日は不完全の可能性があるため、その日から再開
            resume_from = row["d"].replace("-", "")
            console.print(f"[yellow]DB内の最終日 {row['d']} から再開[/yellow]")

    total_races = 0
    total_results = 0

    all_dates = []
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            now = datetime.now()
            if year > now.year or (year == now.year and month > now.month):
                break
            all_dates.extend(get_month_dates(year, month))

    # resume_from 以降のみ処理
    if resume_from:
        all_dates = [d for d in all_dates if d >= resume_from]

    console.print(f"[bold]対象: {len(all_dates)}日間 ({all_dates[0]}〜{all_dates[-1]})[/bold]")
    console.print(f"[bold]並列数: {MAX_WORKERS}ワーカー[/bold]\n")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TextColumn("{task.completed}/{task.total}"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        day_task = progress.add_task("Days", total=len(all_dates))

        for date in all_dates:
            # レースリスト取得
            race_ids = _scrape_race_list(date)
            if not race_ids:
                progress.update(day_task, advance=1, description=f"{date} (skip)")
                continue

            progress.update(day_task, description=f"{date} ({len(race_ids)}R)")

            # 並列スクレイピング
            results_map = {}
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_id = {
                    executor.submit(_scrape_race, rid): rid
                    for rid in race_ids
                }
                for future in as_completed(future_to_id):
                    rid = future_to_id[future]
                    data = future.result()
                    if data:
                        results_map[rid] = data

            # DB書き込み（メインスレッド、逐次）
            for race_id in race_ids:
                data = results_map.get(race_id)
                if not data:
                    continue

                try:
                    race_date = f"{date[:4]}-{date[4:6]}-{date[6:8]}"
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
                            total_results += 1

                    total_races += 1

                except Exception as e:
                    console.print(f"  [red]DB error {race_id}: {e}[/red]")

            conn.commit()
            progress.update(day_task, advance=1)

    conn.close()
    console.print(f"\n[bold green]完了: {total_races}レース, {total_results}結果[/bold green]")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="過去データ一括取得（並列版）")
    parser.add_argument("--start", type=int, default=2023, help="開始年")
    parser.add_argument("--end", type=int, default=2024, help="終了年")
    parser.add_argument("--resume-from", type=str, default=None, help="再開日 (YYYYMMDD)")
    parser.add_argument("--workers", type=int, default=4, help="並列数 (default: 4)")
    args = parser.parse_args()

    if args.workers:
        MAX_WORKERS = args.workers

    collect_past_data(args.start, args.end, args.resume_from)
