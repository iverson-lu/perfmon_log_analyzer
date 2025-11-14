# PerfMon Log Analyzer

This project provides a simple Flask application that summarizes Windows PerfMon CSV exports. It loads the bundled `Performance Counter.csv` file at startup and exposes both an HTML dashboard and JSON APIs with the same information.

## Features

* Per-counter minimum, average, and maximum statistics.
* Category aggregation (CPU, GPU, Memory, Disk, Network, and Other) using keyword matching.
* HTML dashboard with pagination plus JSON endpoints at `/api/metrics` and `/api/categories`.

## Getting started

1. Install the dependencies:

   ```bash
   pip install -e .
   ```

2. Run the Flask development server:

   ```bash
   flask --app app run --debug
   ```

3. Open <http://127.0.0.1:5000> to inspect the dashboard.

The analyzer loads the CSV once during startup. To analyze a different log, replace `Performance Counter.csv` and restart the app.
