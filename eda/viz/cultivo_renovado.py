"""
cultivo_renovado.py — Interactive HTML: NDVI vs edad_de_cultivo for detected
cultivo renovado cycles. Uses ndvi_ref. Renovation point marked in red.
Output: eda/cultivo_renovado.html
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

OUTPUT_PATH = Path(__file__).parent / "cultivo_renovado.html"
RANDOM_SEED = 42
N_CYCLES    = 50


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
    # Sample flagged cycles
    flagged = pd.read_sql(text("""
        SELECT cod_cg, cierre,
               MIN(fecha::date)     AS renovation_date,
               MIN(edad_de_cultivo) AS edad_at_renovation
        FROM maestra
        WHERE gap_in_data     = FALSE
          AND edad_de_cultivo > 300
          AND ndvi_ref        < 0.2
          AND ndvi_ref        IS NOT NULL
        GROUP BY cod_cg, cierre
    """), engine)
    print(f"Total flagged cycles: {len(flagged)}")

    sampled = flagged.sample(n=N_CYCLES, random_state=RANDOM_SEED)

    # Pull full cycle data for sampled lots
    pairs = [(r.cod_cg, r.cierre) for r in sampled.itertuples()]
    cod_cgs  = [p[0] for p in pairs]
    cierres  = [p[1] for p in pairs]

    # Load full lot history (all cierres) for the sampled lots
    df = pd.read_sql(text("""
        SELECT m.cod_cg, m.cierre, m.fecha::date AS fecha,
               m.edad_de_cultivo, m.ndvi_ref
        FROM maestra m
        WHERE m.cod_cg = ANY(:cods)
          AND m.gap_in_data = FALSE
          AND m.ndvi_ref IS NOT NULL
        ORDER BY m.cod_cg, m.fecha
    """), engine, params={"cods": cod_cgs})

    print(f"Rows loaded: {len(df):,}")
    return df, sampled


def build_html(df, sampled):
    COLORS = [
        "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
        "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
    ]

    renovation_map = {
        (r.cod_cg, r.cierre): str(r.renovation_date)
        for r in sampled.itertuples()
    }

    lots = []
    for cod_cg, lot_df in df.groupby("cod_cg"):
        lot_df = lot_df.sort_values("fecha")
        cierres = sorted([c for c in lot_df["cierre"].unique() if c is not None])

        # Build one trace per cierre (for coloring)
        traces = []
        for i, cierre in enumerate(cierres):
            cyc = lot_df[lot_df["cierre"] == cierre].sort_values("fecha")
            traces.append({
                "cierre": str(cierre),
                "color": COLORS[i % len(COLORS)],
                "fecha": [str(v) for v in cyc["fecha"]],
                "edad": cyc["edad_de_cultivo"].tolist(),
                "ndvi": [round(float(v), 4) for v in cyc["ndvi_ref"]],
            })

        # Renovation events for this lot
        renovations = []
        for cierre in cierres:
            rdate = renovation_map.get((cod_cg, cierre))
            if rdate:
                # Find ndvi_ref at that date
                row = lot_df[lot_df["fecha"].astype(str) == rdate]
                rndvi = round(float(row["ndvi_ref"].iloc[0]), 4) if not row.empty else None
                edad_r = int(row["edad_de_cultivo"].iloc[0]) if not row.empty else None
                renovations.append({
                    "fecha": rdate,
                    "ndvi": rndvi,
                    "edad": edad_r,
                    "cierre": str(cierre),
                })

        lots.append({
            "label": cod_cg,
            "traces": traces,
            "renovations": renovations,
        })

    lots_json = json.dumps(lots)
    first_label = lots[0]["label"]

    return f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<title>Cultivo Renovado</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
  body {{ font-family: Arial, sans-serif; margin: 20px; background: #f9f9f9; }}
  h2 {{ margin: 0 0 4px 0; font-size: 1.1rem; color: #333; }}
  #lot-title {{
    font-size: 1.3rem; font-weight: bold; color: #111;
    background: #fff; border: 1px solid #ddd; border-radius: 4px;
    padding: 6px 12px; display: inline-block; margin-bottom: 12px;
    user-select: text; cursor: text;
  }}
  .controls {{ display: flex; align-items: center; gap: 16px; margin-bottom: 12px; }}
  label {{ font-weight: bold; font-size: 0.95rem; }}
  select {{ font-size: 0.95rem; padding: 5px 8px; border: 1px solid #ccc; border-radius: 4px; min-width: 200px; }}
  #renovation-info {{
    background: #fff3cd; border: 1px solid #ffc107; border-radius: 4px;
    padding: 6px 14px; font-size: 0.9rem; color: #555;
    display: inline-block; margin-bottom: 12px;
  }}
  #plot {{ background: #fff; border-radius: 6px; border: 1px solid #ddd; }}
</style>
</head>
<body>

<h2>Cultivo Renovado — NDVI Referencia vs Fecha</h2>
<div id="lot-title">{first_label}</div><br>
<div id="renovation-info">Cargando...</div>

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
sel.addEventListener('change', function() {{
  currentIdx = parseInt(this.value);
  document.getElementById('lot-title').textContent = LOTS[currentIdx].label;
  renderPlot();
}});

function renderPlot() {{
  const lot = LOTS[currentIdx];
  const traces = [];

  // One line per cierre
  lot.traces.forEach(t => {{
    traces.push({{
      x: t.fecha,
      y: t.ndvi,
      customdata: t.edad,
      mode: 'lines+markers',
      name: 'cierre ' + t.cierre,
      line: {{ color: t.color, width: 2 }},
      marker: {{ size: 4 }},
      hovertemplate: 'fecha: <b>%{{x}}</b><br>ndvi_ref: <b>%{{y:.4f}}</b><br>edad: %{{customdata}} días<extra>cierre ' + t.cierre + '</extra>',
    }});
  }});

  // Red X markers for each renovation event
  lot.renovations.forEach(r => {{
    if (r.fecha && r.ndvi !== null) {{
      traces.push({{
        x: [r.fecha],
        y: [r.ndvi],
        mode: 'markers',
        name: 'Renovación (cierre ' + r.cierre + ')',
        marker: {{ color: 'red', size: 14, symbol: 'x', line: {{ width: 3 }} }},
        hovertemplate: 'fecha: <b>' + r.fecha + '</b><br>ndvi_ref: <b>' + r.ndvi + '</b><br>edad: ' + r.edad + ' días<extra>Renovación</extra>',
      }});
    }}
  }});

  // Threshold line at 0.2
  const allFechas = lot.traces.flatMap(t => t.fecha).sort();
  if (allFechas.length) {{
    traces.push({{
      x: [allFechas[0], allFechas[allFechas.length - 1]],
      y: [0.2, 0.2],
      mode: 'lines',
      name: 'Umbral 0.2',
      line: {{ color: '#bbb', width: 1, dash: 'dash' }},
      hoverinfo: 'skip',
      showlegend: true,
    }});
  }}

  // Update info banner
  const info = document.getElementById('renovation-info');
  if (lot.renovations.length > 0) {{
    const parts = lot.renovations.map(r =>
      'cierre ' + r.cierre + ' → fecha ' + r.fecha + ' (edad ' + r.edad + ' días, ndvi ' + r.ndvi + ')'
    );
    info.textContent = 'Renovaciones detectadas: ' + parts.join(' | ');
  }} else {{
    info.textContent = 'Sin renovaciones detectadas';
  }}

  Plotly.react('plot', traces, {{
    xaxis: {{ title: 'Fecha', type: 'date' }},
    yaxis: {{ title: 'NDVI Referencia', range: [-0.05, 0.85] }},
    legend: {{ title: {{ text: 'Ciclo (cierre)' }}, x: 1.01, y: 1, xanchor: 'left' }},
    height: 560,
    margin: {{ t: 30, r: 200, l: 60, b: 60 }},
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
        engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@127.0.0.1:{port}/{DB_NAME}")
        df, sampled = load_data(engine)
    finally:
        tunnel.terminate()

    html = build_html(df, sampled)
    OUTPUT_PATH.write_text(html, encoding="utf-8")
    print(f"Saved: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
