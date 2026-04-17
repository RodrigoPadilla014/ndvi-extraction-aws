"""
lot_history.py — Interactive HTML: full cycle history of a lot.

Shows NDVI Referencia over time (x=fecha) for a random sample of lots.
Each cycle (cierre) is one colored line. No renovation logic.

Output: eda/lot_history.html
"""

import json
import os
import socket
import subprocess
import time
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

PROJECT_ROOT = Path(__file__).parent.parent.parent
load_dotenv(PROJECT_ROOT / ".env")

SSH_HOST = os.environ["SSH_HOST"]
SSH_USER = os.environ["SSH_USER"]
SSH_KEY  = PROJECT_ROOT / os.environ["SSH_KEY"]
DB_HOST  = os.environ["DB_HOST"]
DB_PORT  = int(os.environ["DB_PORT"])
DB_NAME  = os.environ["DB_NAME"]
DB_USER  = os.environ["DB_USER"]
DB_PASS  = os.environ["DB_PASSWORD"]

OUTPUT_PATH = Path(__file__).parent / "lot_history.html"

FIXED_LOTS = [
    '03-10187301', '14-18160027', '07-0151201', '03-10678102', '14-13880001',
    '14-12280003', '14-14220028', '07-1770104', '04-JI8640301', '13-215.09',
    '07-1750105', '19-A840902', '03-10011205', '03-102071002', '14-13920001',
    '14-13760012', '14-11040102', '14-14150009', '07-1740210', '03-10015801',
    '19-6650101', '03-10101101', '03-100513705', '14-18030416', '03-10004701',
    '07-0850102', '19-0030202', '19-A700101', '14-14310504', '19-3010102',
    '14-13610001', '19-1660105', '19-0200201', '14-12010122', '19-0070103',
    '14-11210027', '14-13190002', '13-6868.09', '03-10369401', '19-0050601',
    '14-11110213A', '07-0101302', '07-0102601', '03-10909801', '04-PA0010701',
    '14-11210015B', '07-0950507', '04-TQ0272801', '03-10640104', '03-100082501',
]


def open_tunnel():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    proc = subprocess.Popen([
        "ssh", "-N", "-L", f"{port}:{DB_HOST}:{DB_PORT}",
        "-i", str(SSH_KEY), "-o", "StrictHostKeyChecking=no",
        "-o", "BatchMode=yes", "-p", "22", f"{SSH_USER}@{SSH_HOST}",
    ])
    time.sleep(3)
    return proc, port


def load_data(engine):
    df = pd.read_sql(text("""
        SELECT cod_cg,
               fecha::date        AS fecha,
               ndvi_ref,
               edad_de_cultivo,
               cierre_ciclo::text AS cierre_ciclo
        FROM maestra
        WHERE cod_cg = ANY(:cods)
          AND ndvi_ref IS NOT NULL
        ORDER BY cod_cg, fecha
    """), engine, params={"cods": FIXED_LOTS})

    renovations = pd.read_sql(text("""
        SELECT * FROM (
            -- Pre-harvest drops within productividad cycles
            SELECT DISTINCT ON (cod_cg, cierre_ciclo)
                   cod_cg, cierre_ciclo::text, fecha::date AS fecha,
                   ndvi_ref, edad_de_cultivo
            FROM maestra
            WHERE cod_cg = ANY(:cods)
              AND gap_in_data = FALSE
              AND cierre_ciclo IS NOT NULL
              AND cierre IS NOT NULL
              AND edad_de_cultivo > 200
              AND ndvi_ref < 0.2
              AND fecha::date < cierre_ciclo
            ORDER BY cod_cg, cierre_ciclo, fecha
        ) pre_harvest

        UNION ALL

        SELECT * FROM (
            -- Renovation-detected cycle boundaries (cierre_ciclo set by migration 010)
            SELECT DISTINCT ON (cod_cg, cierre_ciclo)
                   cod_cg, cierre_ciclo::text, fecha::date AS fecha,
                   ndvi_ref, edad_de_cultivo
            FROM maestra
            WHERE cod_cg = ANY(:cods)
              AND gap_in_data = FALSE
              AND cierre_ciclo IS NOT NULL
              AND cierre IS NULL
              AND edad_de_cultivo > 200
              AND ndvi_ref < 0.2
            ORDER BY cod_cg, cierre_ciclo, fecha
        ) renovations_detected
    """), engine, params={"cods": FIXED_LOTS})

    print(f"Rows loaded: {len(df):,}  |  Renovation points: {len(renovations)}")
    return df, renovations


def build_html(df, renovations):
    COLORS = [
        "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
        "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
        "#aec7e8", "#ffbb78", "#98df8a", "#ff9896", "#c5b0d5",
    ]

    lots = []
    for cod_cg, lot_df in df.groupby("cod_cg"):
        lot_df = lot_df.sort_values("fecha")
        all_ciclos = sorted(c for c in lot_df["cierre_ciclo"].unique() if c is not None)
        has_null = lot_df["cierre_ciclo"].isna().any()

        lot_renovs = renovations[renovations["cod_cg"] == cod_cg]

        traces = []
        color_idx = 0
        for ciclo in all_ciclos:
            cyc = lot_df[lot_df["cierre_ciclo"] == ciclo].sort_values("fecha")
            traces.append({
                "label": ciclo,
                "color": COLORS[color_idx % len(COLORS)],
                "fecha": [str(v) for v in cyc["fecha"]],
                "ndvi":  [round(float(v), 4) for v in cyc["ndvi_ref"]],
                "edad":  [int(v) if pd.notna(v) else None for v in cyc["edad_de_cultivo"]],
            })
            color_idx += 1

        if has_null:
            cyc = lot_df[lot_df["cierre_ciclo"].isna()].sort_values("fecha")
            traces.append({
                "label": "sin cierre",
                "color": "#aaaaaa",
                "fecha": [str(v) for v in cyc["fecha"]],
                "ndvi":  [round(float(v), 4) for v in cyc["ndvi_ref"]],
                "edad":  [int(v) if pd.notna(v) else None for v in cyc["edad_de_cultivo"]],
            })

        renov_list = [
            {
                "fecha":        str(r.fecha),
                "ndvi":         round(float(r.ndvi_ref), 4),
                "edad":         int(r.edad_de_cultivo) if pd.notna(r.edad_de_cultivo) else None,
                "cierre_ciclo": str(r.cierre_ciclo),
            }
            for r in lot_renovs.itertuples()
        ]

        lots.append({"label": cod_cg, "traces": traces, "renovations": renov_list})

    lots_json   = json.dumps(lots, default=str)
    first_label = lots[0]["label"] if lots else ""

    return f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<title>Historial de Lote</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
  body   {{ font-family: Arial, sans-serif; margin: 20px; background: #f4f6f9; }}
  h2     {{ margin: 0 0 8px 0; font-size: 1.1rem; color: #444; }}
  #lot-title {{
    font-size: 1.4rem; font-weight: bold; color: #111;
    background: #fff; border: 1px solid #ddd; border-radius: 4px;
    padding: 6px 14px; display: inline-block; margin-bottom: 12px;
    user-select: text; cursor: text;
  }}
  .controls {{ display: flex; align-items: center; gap: 16px; margin-bottom: 12px; }}
  label  {{ font-weight: bold; font-size: 0.95rem; }}
  select {{ font-size: 0.95rem; padding: 5px 10px; border: 1px solid #ccc; border-radius: 4px; min-width: 210px; }}
  #plot  {{ background: #fff; border-radius: 6px; border: 1px solid #ddd; }}
</style>
</head>
<body>

<h2>Historial de Lote — NDVI Referencia por Ciclo</h2>
<div id="lot-title">{first_label}</div><br>

<div class="controls">
  <label for="lot-select">Lote: </label>
  <select id="lot-select"></select>
</div>

<div id="plot"></div>

<script>
const LOTS = {lots_json};
let currentIdx = 0;

const sel = document.getElementById('lot-select');
LOTS.forEach((l, i) => {{
  const opt = document.createElement('option');
  opt.value = i;
  opt.textContent = l.label;
  sel.appendChild(opt);
}});
sel.value = 0;
sel.addEventListener('change', function () {{
  currentIdx = parseInt(this.value);
  document.getElementById('lot-title').textContent = LOTS[currentIdx].label;
  renderPlot();
}});

function renderPlot() {{
  const lot = LOTS[currentIdx];
  const traces = lot.traces.map(t => ({{
    x: t.fecha,
    y: t.ndvi,
    customdata: t.edad,
    mode: 'lines+markers',
    name: 'cierre ' + t.label,
    line:   {{ color: t.color, width: 2 }},
    marker: {{ size: 4 }},
    hovertemplate:
      'fecha: <b>%{{x}}</b><br>' +
      'ndvi_ref: <b>%{{y:.4f}}</b><br>' +
      'edad: <b>%{{customdata}}</b> días' +
      '<extra>cierre ' + t.label + '</extra>',
  }}));

  // Renovation markers
  lot.renovations.forEach(r => {{
    traces.push({{
      x: [r.fecha],
      y: [r.ndvi],
      mode: 'markers',
      name: 'Renovación',
      showlegend: false,
      marker: {{ color: 'crimson', size: 14, symbol: 'x', line: {{ width: 3 }} }},
      hovertemplate:
        'fecha: <b>' + r.fecha + '</b><br>' +
        'ndvi_ref: <b>' + r.ndvi + '</b><br>' +
        'edad: ' + r.edad + ' días<br>' +
        'cierre: ' + r.cierre_ciclo +
        '<extra>Renovación</extra>',
    }});
  }});

  Plotly.react('plot', traces, {{
    xaxis: {{ title: 'Fecha', type: 'date' }},
    yaxis: {{ title: 'NDVI Referencia', range: [-0.1, 0.9] }},
    legend: {{ title: {{ text: 'Ciclo (zafra)' }}, x: 1.01, y: 1, xanchor: 'left' }},
    height: 560,
    margin: {{ t: 30, r: 200, l: 65, b: 60 }},
    hovermode: 'x unified',
    template: 'plotly_white',
  }});
}}

renderPlot();
</script>
</body>
</html>"""


def main():
    tunnel, port = open_tunnel()
    try:
        engine = create_engine(
            f"postgresql://{DB_USER}:{DB_PASS}@127.0.0.1:{port}/{DB_NAME}"
        )
        df, renovations = load_data(engine)
    finally:
        tunnel.terminate()

    html = build_html(df, renovations)
    OUTPUT_PATH.write_text(html, encoding="utf-8")
    print(f"Saved: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
