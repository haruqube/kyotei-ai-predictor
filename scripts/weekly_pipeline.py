"""全体オーケストレーション — パイプライン

競艇は毎日開催のため、競馬と違って曜日固定ではなく日次で回す。
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from rich.console import Console
from rich.panel import Panel

console = Console()


def run_result_pipeline():
    """結果更新パイプライン: 結果取得 → 精度検証"""
    console.print(Panel("Result Pipeline", style="bold blue"))

    console.print("\n[bold]Step 1: 直近結果の取得[/bold]")
    from scripts.update_data import update_recent_results
    update_recent_results(days_back=3)

    console.print("\n[bold]Step 2: 予測精度検証[/bold]")
    from backtest.evaluator import evaluate_recent
    evaluate_recent(days_back=7)

    console.print("\n[green]結果更新パイプライン完了[/green]")


def run_predict_pipeline(target_date: str | None = None):
    """予測パイプライン: データ取得 → 予測 → レポート出力"""
    console.print(Panel("Predict Pipeline", style="bold blue"))

    console.print("\n[bold]Step 1: レース予測生成[/bold]")
    from scripts.predict_races import predict_races
    predictions = predict_races(target_date)

    if not predictions:
        console.print("[red]予測が生成されませんでした。[/red]")
        return

    console.print("\n[bold]Step 2: 予測レポート出力[/bold]")
    from scripts.generate_article import generate_prediction_report
    report_path = generate_prediction_report()

    console.print("\n[green]予測パイプライン完了[/green]")
    console.print(f"\n[bold]次のステップ:[/bold]")
    console.print(f'  Claude Codeで: 「{report_path} を読んでnote.com用の競艇予想記事を書いて」')


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Pipeline")
    parser.add_argument("--predict", action="store_true", help="Run predict pipeline")
    parser.add_argument("--result", action="store_true", help="Run result pipeline")
    parser.add_argument("--date", type=str, default=None, help="Target date (YYYYMMDD)")
    args = parser.parse_args()

    if args.predict:
        run_predict_pipeline(args.date)
    elif args.result:
        run_result_pipeline()
    else:
        console.print("[yellow]--predict または --result を指定してください[/yellow]")
