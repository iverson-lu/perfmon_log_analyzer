"""Flask application that summarizes PerfMon CSV logs."""

from __future__ import annotations

from pathlib import Path

from flask import Flask, jsonify, render_template, request

from perfmon_analyzer import PerfmonAnalyzer

BASE_DIR = Path(__file__).parent
CSV_PATH = BASE_DIR / "Performance Counter.csv"

analyzer = PerfmonAnalyzer(CSV_PATH)
analyzer.load()

app = Flask(__name__)


def _paginate(items, page: int, per_page: int):
    start = (page - 1) * per_page
    end = start + per_page
    return items[start:end]


@app.route("/")
def index():
    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 50))
    metric_stats = analyzer.metric_stats()
    paginated_metrics = _paginate(metric_stats, page, per_page)
    total_pages = (len(metric_stats) + per_page - 1) // per_page
    category_stats = analyzer.category_stats()
    return render_template(
        "index.html",
        csv_name=CSV_PATH.name,
        metric_stats=paginated_metrics,
        page=page,
        per_page=per_page,
        total_pages=total_pages,
        metric_count=len(metric_stats),
        category_stats=category_stats,
    )


@app.route("/api/metrics")
def metrics_api():
    metrics = [stat.to_dict() for stat in analyzer.metric_stats()]
    return jsonify(metrics)


@app.route("/api/categories")
def categories_api():
    return jsonify(analyzer.category_stats())


if __name__ == "__main__":
    app.run(debug=True)
