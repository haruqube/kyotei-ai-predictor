"""予測データをMarkdown形式で出力 — Claude Codeで記事化する素材"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import datetime
from rich.console import Console

from config import RESULTS_DIR, VENUE_CODES
from db.schema import get_connection

console = Console()


def generate_prediction_report(target_date: str | None = None) -> str:
    """予測データを整形してMarkdownレポートとして出力"""
    conn = get_connection()

    if target_date:
        races = conn.execute("""
            SELECT DISTINCT r.* FROM races r
            JOIN predictions p ON r.race_id = p.race_id
            WHERE r.date = ?
            ORDER BY r.venue_code, r.race_number
        """, (target_date,)).fetchall()
    else:
        races = conn.execute("""
            SELECT DISTINCT r.* FROM races r
            JOIN predictions p ON r.race_id = p.race_id
            ORDER BY r.date DESC, r.venue_code, r.race_number
            LIMIT 72
        """).fetchall()

    if not races:
        console.print("[red]予測データがありません。predict_races.pyを先に実行してください。[/red]")
        conn.close()
        return ""

    dates = sorted(set(r["date"] for r in races))
    venues = sorted(set(r["venue_name"] for r in races if r["venue_name"]))

    lines = []
    lines.append(f"# 競艇AI予測データ {', '.join(dates)}")
    lines.append(f"開催場: {' / '.join(venues)}")
    lines.append(f"生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append("")

    for race in races:
        race_id = race["race_id"]

        lines.append("---")
        lines.append(f"## {race['venue_name']} {race['race_number']}R {race['race_name'] or ''}")
        lines.append(f"- グレード: {race['grade'] or '一般'}")
        lines.append(f"- 天候: {race['weather'] or '不明'} / 風: {race['wind_direction'] or '?'} {race['wind_speed'] or '?'}m")
        lines.append(f"- 波高: {race['wave_height'] or '?'}cm")
        lines.append(f"- {'ナイター' if race['is_night'] else 'デイ'}レース")
        lines.append("")

        predictions = conn.execute("""
            SELECT p.*, e.racer_name, e.course, e.class, e.branch,
                   e.national_win_rate, e.local_win_rate,
                   e.motor_2nd_rate, e.boat_2nd_rate,
                   e.exhibition_time, e.start_timing,
                   e.odds, e.popularity
            FROM predictions p
            LEFT JOIN entries e ON p.race_id = e.race_id AND p.racer_id = e.racer_id
            WHERE p.race_id = ?
            ORDER BY p.predicted_rank
        """, (race_id,)).fetchall()

        if not predictions:
            lines.append("(予測データなし)")
            lines.append("")
            continue

        marks = ["◎", "○", "▲", "△", "△", ""]

        lines.append("| 印 | コース | 選手名 | 級別 | 全国勝率 | 当地勝率 | モーター | 展示T | スコア |")
        lines.append("|---|---|---|---|---|---|---|---|---|")

        for i, p in enumerate(predictions):
            mark = marks[i] if i < len(marks) else ""

            lines.append(
                f"| {mark} "
                f"| {p['course'] or '?'} "
                f"| {p['racer_name'] or p['racer_id']} "
                f"| {p['class'] or '?'} "
                f"| {p['national_win_rate'] or '?'} "
                f"| {p['local_win_rate'] or '?'} "
                f"| {p['motor_2nd_rate'] or '?'}% "
                f"| {p['exhibition_time'] or '?'} "
                f"| {p['predicted_score']:.3f} |"
            )

        lines.append("")

        # 上位選手の同コース成績
        lines.append("**上位選手のコース別成績:**")
        for i, p in enumerate(predictions[:3]):
            racer_id = p["racer_id"]
            name = p["racer_name"] or racer_id
            course = p["course"]

            course_results = conn.execute("""
                SELECT rr.finish_position
                FROM race_results rr
                JOIN races r ON rr.race_id = r.race_id
                WHERE rr.racer_id = ? AND rr.course = ?
                ORDER BY r.date DESC LIMIT 10
            """, (racer_id, course)).fetchall()

            course_str = " ".join(
                str(r["finish_position"]) for r in course_results if r["finish_position"]
            ) or "データなし"

            lines.append(f"- {marks[i] if i < len(marks) else ''} {name} ({course}コース): [{course_str}]")

        lines.append("")

    conn.close()

    report = "\n".join(lines)

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    date_file = dates[0].replace("-", "") if dates else datetime.now().strftime("%Y%m%d")
    report_path = RESULTS_DIR / f"prediction_report_{date_file}.md"
    report_path.write_text(report, encoding="utf-8")

    console.print(f"[green]予測レポート保存: {report_path}[/green]")
    console.print(f'[dim]このファイルをClaude Codeに読ませて記事を生成してください。[/dim]')

    return str(report_path)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default=None, help="対象日 (YYYY-MM-DD)")
    args = parser.parse_args()
    generate_prediction_report(args.date)
