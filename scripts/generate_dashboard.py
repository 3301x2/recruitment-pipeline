"""Generate a static HTML dashboard from the gold layer.

Usage (with containers running):
    python3 scripts/generate_dashboard.py

Opens dashboard.html automatically in your default browser.
"""
import json
import os
import webbrowser
from pathlib import Path

import psycopg2

HOST = os.getenv("POSTGRES_HOST", "localhost")
CONN = dict(host=HOST, port=5432, dbname="recruitment", user="pipeline", password="pipeline123")
OUT = Path(__file__).resolve().parent.parent / "dashboard.html"


def query(sql):
    with psycopg2.connect(**CONN) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


def main():
    summary = query("""
        SELECT COUNT(*) AS total_positions,
               COUNT(*) FILTER (WHERE is_open = true) AS open_positions,
               COUNT(*) FILTER (WHERE is_open = false) AS filled_positions,
               ROUND(AVG(days_to_fill), 1) AS avg_days_to_fill
        FROM public_gold.fact_jobs
    """)[0]

    by_dept = query("""
        SELECT d.department_name, COUNT(*) AS total_jobs
        FROM public_gold.fact_jobs f
        JOIN public_gold.dim_department d ON d.department_key = f.department_key
        GROUP BY d.department_name ORDER BY total_jobs DESC
    """)

    time_to_fill = query("""
        SELECT d.department_name,
               ROUND(AVG(f.days_to_fill), 1) AS avg_days
        FROM public_gold.fact_jobs f
        JOIN public_gold.dim_department d ON d.department_key = f.department_key
        WHERE f.days_to_fill IS NOT NULL
        GROUP BY d.department_name ORDER BY avg_days DESC
    """)

    quarterly = query("""
        SELECT dt.year, dt.quarter, COUNT(*) AS jobs_opened
        FROM public_gold.fact_jobs f
        JOIN public_gold.dim_date dt ON dt.date_key = f.open_date_key
        GROUP BY dt.year, dt.quarter ORDER BY dt.year, dt.quarter
    """)

    velocity = query("""
        SELECT d.department_name,
               COUNT(*) FILTER (WHERE f.is_open = true) AS still_open,
               COUNT(*) FILTER (WHERE f.is_open = false) AS filled
        FROM public_gold.fact_jobs f
        JOIN public_gold.dim_department d ON d.department_key = f.department_key
        GROUP BY d.department_name ORDER BY d.department_name
    """)

    # Build quarterly labels
    q_labels = [f"Q{r['quarter']} {r['year']}" for r in quarterly]
    q_values = [r['jobs_opened'] for r in quarterly]

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Recruitment Pipeline Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
         background: #0f172a; color: #e2e8f0; padding: 24px; }}
  h1 {{ text-align: center; font-size: 1.8rem; margin-bottom: 8px; color: #f8fafc; }}
  .subtitle {{ text-align: center; color: #94a3b8; margin-bottom: 32px; font-size: 0.95rem; }}
  .kpi-row {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 32px; }}
  .kpi {{ background: #1e293b; border-radius: 12px; padding: 24px; text-align: center;
          border: 1px solid #334155; }}
  .kpi .value {{ font-size: 2.4rem; font-weight: 700; color: #38bdf8; }}
  .kpi .label {{ font-size: 0.85rem; color: #94a3b8; margin-top: 4px; text-transform: uppercase;
                 letter-spacing: 0.05em; }}
  .charts {{ display: grid; grid-template-columns: 1fr 1fr; gap: 24px; margin-bottom: 32px; }}
  .card {{ background: #1e293b; border-radius: 12px; padding: 24px;
           border: 1px solid #334155; }}
  .card h2 {{ font-size: 1rem; color: #cbd5e1; margin-bottom: 16px; }}
  .full-width {{ grid-column: 1 / -1; }}
  canvas {{ max-height: 320px; }}
  footer {{ text-align: center; color: #475569; font-size: 0.8rem; margin-top: 24px; }}
</style>
</head>
<body>

<h1>Recruitment Pipeline Dashboard</h1>
<p class="subtitle">Live data from the Gold layer &mdash; star schema powered by dbt</p>

<div class="kpi-row">
  <div class="kpi">
    <div class="value">{summary['total_positions']}</div>
    <div class="label">Total Positions</div>
  </div>
  <div class="kpi">
    <div class="value">{summary['open_positions']}</div>
    <div class="label">Open Positions</div>
  </div>
  <div class="kpi">
    <div class="value">{summary['filled_positions']}</div>
    <div class="label">Filled Positions</div>
  </div>
  <div class="kpi">
    <div class="value">{summary['avg_days_to_fill'] or 'N/A'}</div>
    <div class="label">Avg Days to Fill</div>
  </div>
</div>

<div class="charts">
  <div class="card">
    <h2>Jobs by Department</h2>
    <canvas id="deptChart"></canvas>
  </div>
  <div class="card">
    <h2>Avg Time-to-Fill by Department (days)</h2>
    <canvas id="ttfChart"></canvas>
  </div>
  <div class="card full-width">
    <h2>Quarterly Hiring Trends</h2>
    <canvas id="quarterChart"></canvas>
  </div>
  <div class="card full-width">
    <h2>Department Hiring Velocity (Open vs Filled)</h2>
    <canvas id="velocityChart"></canvas>
  </div>
</div>

<footer>Generated from public_gold schema &bull; PostgreSQL &rarr; dbt &rarr; Chart.js</footer>

<script>
const COLORS = ['#38bdf8','#818cf8','#34d399','#fbbf24','#f87171','#a78bfa','#fb923c','#2dd4bf','#e879f9','#94a3b8'];

// --- Jobs by Department (doughnut) ---
new Chart(document.getElementById('deptChart'), {{
  type: 'doughnut',
  data: {{
    labels: {json.dumps([r['department_name'] for r in by_dept])},
    datasets: [{{ data: {json.dumps([r['total_jobs'] for r in by_dept])},
                  backgroundColor: COLORS.slice(0, {len(by_dept)}) }}]
  }},
  options: {{ responsive: true, plugins: {{ legend: {{ position: 'right', labels: {{ color: '#cbd5e1' }} }} }} }}
}});

// --- Time-to-Fill (horizontal bar) ---
new Chart(document.getElementById('ttfChart'), {{
  type: 'bar',
  data: {{
    labels: {json.dumps([r['department_name'] for r in time_to_fill])},
    datasets: [{{ label: 'Avg days', data: {json.dumps([float(r['avg_days']) for r in time_to_fill])},
                  backgroundColor: '#818cf8', borderRadius: 4 }}]
  }},
  options: {{ indexAxis: 'y', responsive: true,
              scales: {{ x: {{ ticks: {{ color: '#94a3b8' }} }}, y: {{ ticks: {{ color: '#cbd5e1' }} }} }},
              plugins: {{ legend: {{ display: false }} }} }}
}});

// --- Quarterly Trends (line) ---
new Chart(document.getElementById('quarterChart'), {{
  type: 'line',
  data: {{
    labels: {json.dumps(q_labels)},
    datasets: [{{ label: 'Jobs opened', data: {json.dumps(q_values)},
                  borderColor: '#38bdf8', backgroundColor: 'rgba(56,189,248,0.1)',
                  fill: true, tension: 0.3, pointRadius: 3 }}]
  }},
  options: {{ responsive: true,
              scales: {{ x: {{ ticks: {{ color: '#94a3b8', maxRotation: 45 }} }},
                         y: {{ ticks: {{ color: '#94a3b8' }}, beginAtZero: true }} }},
              plugins: {{ legend: {{ labels: {{ color: '#cbd5e1' }} }} }} }}
}});

// --- Velocity (stacked bar) ---
new Chart(document.getElementById('velocityChart'), {{
  type: 'bar',
  data: {{
    labels: {json.dumps([r['department_name'] for r in velocity])},
    datasets: [
      {{ label: 'Filled', data: {json.dumps([r['filled'] for r in velocity])},
         backgroundColor: '#34d399', borderRadius: 4 }},
      {{ label: 'Still Open', data: {json.dumps([r['still_open'] for r in velocity])},
         backgroundColor: '#fbbf24', borderRadius: 4 }}
    ]
  }},
  options: {{ responsive: true, scales: {{
               x: {{ stacked: true, ticks: {{ color: '#cbd5e1' }} }},
               y: {{ stacked: true, ticks: {{ color: '#94a3b8' }}, beginAtZero: true }} }},
              plugins: {{ legend: {{ labels: {{ color: '#cbd5e1' }} }} }} }}
}});
</script>
</body>
</html>"""

    OUT.write_text(html)
    print(f"Dashboard written to {OUT}")
    webbrowser.open(f"file://{OUT}")


if __name__ == "__main__":
    main()
