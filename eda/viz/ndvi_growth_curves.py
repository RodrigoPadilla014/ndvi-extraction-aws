"""
ndvi_growth_curves.py — Interactive Plotly HTML: NDVI vs edad_de_cultivo per lot.

Selects 75 random lots with >=2 clean cycles (gap_in_data=FALSE, edad IS NOT NULL).
Each cycle is one line, identified by its cierre date.
Output: eda/ndvi_growth_curves.html
"""

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

OUTPUT_PATH = Path(__file__).parent / "ndvi_growth_curves.html"
RANDOM_SEED = 42
N_LOTS = 75
MIN_CYCLES = 2


def open_tunnel() -> tuple[subprocess.Popen, int]:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    proc = subprocess.Popen([
        "ssh", "-N",
        "-L", f"{port}:{DB_HOST}:{DB_PORT}",
        "-i", str(SSH_KEY),
        "-o", "StrictHostKeyChecking=no",
        "-o", "BatchMode=yes",
        "-p", "22",
        f"{SSH_USER}@{SSH_HOST}",
    ])
    time.sleep(3)
    return proc, port


def load_data(engine) -> pd.DataFrame:
    lots_sql = text("""
        SELECT cod_cg
        FROM maestra
        WHERE edad_de_cultivo IS NOT NULL
          AND cierre IS NOT NULL
          AND ndvi_corrected IS NOT NULL
        GROUP BY cod_cg
        HAVING COUNT(DISTINCT cierre) >= :min_cycles
    """)
    lots = pd.read_sql(lots_sql, engine, params={"min_cycles": MIN_CYCLES})
    print(f"Lots with >= {MIN_CYCLES} clean cycles: {len(lots)}")

    sampled = lots["cod_cg"].sample(n=N_LOTS, random_state=RANDOM_SEED).tolist()

    data_sql = text("""
        SELECT cod_cg, edad_de_cultivo, ndvi_corrected, ndvi_ref, cierre,
               fecha::date AS fecha, id_img
        FROM maestra
        WHERE cod_cg = ANY(:lots)
          AND edad_de_cultivo IS NOT NULL
          AND ndvi_corrected IS NOT NULL
        ORDER BY cod_cg, cierre, edad_de_cultivo
    """)
    df = pd.read_sql(data_sql, engine, params={"lots": sampled})
    print(f"Rows loaded: {len(df):,}")
    return df


def build_html(df: pd.DataFrame) -> str:
    import json

    COLORS = [
        "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
        "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
    ]

    lots = sorted(df["cod_cg"].unique())
    lot_data = {}
    for lot in lots:
        lot_df = df[df["cod_cg"] == lot]
        cycles = sorted(c for c in lot_df["cierre"].unique() if c is not None)
        lot_data[lot] = []
        for i, cierre in enumerate(cycles):
            cyc = lot_df[lot_df["cierre"] == cierre].sort_values("edad_de_cultivo")
            ref_vals = [None if pd.isna(v) else round(float(v), 4) for v in cyc["ndvi_ref"]]
            lot_data[lot].append({
                "cierre": str(cierre),
                "color": COLORS[i % len(COLORS)],
                "edad": cyc["edad_de_cultivo"].tolist(),
                "stac": [round(float(v), 4) for v in cyc["ndvi_corrected"]],
                "ref": ref_vals,
                "fecha": [str(v) for v in cyc["fecha"]],
                "id_img": [str(v) for v in cyc["id_img"]],
            })

    lot_data_json = json.dumps(lot_data)
    lots_json = json.dumps(lots)
    first_lot = lots[0]

    return f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<title>NDVI vs Edad de Cultivo</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
  body {{ font-family: Arial, sans-serif; margin: 20px; background: #f9f9f9; }}
  h2 {{ margin: 0 0 4px 0; font-size: 1.1rem; color: #333; }}
  #lot-title {{
    font-size: 1.4rem; font-weight: bold; color: #111;
    background: #fff; border: 1px solid #ddd; border-radius: 4px;
    padding: 6px 12px; display: inline-block; margin-bottom: 12px;
    user-select: text; cursor: text;
  }}
  .controls {{ display: flex; align-items: center; gap: 16px; margin-bottom: 12px; flex-wrap: wrap; }}
  label {{ font-weight: bold; font-size: 0.95rem; }}
  select {{
    font-size: 0.95rem; padding: 5px 8px; border: 1px solid #ccc;
    border-radius: 4px; min-width: 200px;
  }}
  .toggle-group {{ display: flex; border: 1px solid #ccc; border-radius: 4px; overflow: hidden; }}
  .toggle-group button {{
    padding: 5px 18px; border: none; background: #fff;
    cursor: pointer; font-size: 0.9rem; transition: background 0.15s;
  }}
  .toggle-group button.active {{ background: #1f77b4; color: #fff; font-weight: bold; }}
  .toggle-group button:hover:not(.active) {{ background: #e8f0fe; }}
  #plot {{ background: #fff; border-radius: 6px; border: 1px solid #ddd; }}
</style>
</head>
<body>

<h2>NDVI vs Edad de Cultivo</h2>
<div id="lot-title">{first_lot}</div>

<div class="controls">
  <div>
    <label for="lot-select">Lote: </label>
    <select id="lot-select"></select>
  </div>
  <div>
    <label>Fuente: </label>
    <div class="toggle-group">
      <button id="btn-stac" class="active" onclick="setSource('stac')">STAC corregido</button>
      <button id="btn-ref" onclick="setSource('ref')">Referencia</button>
    </div>
  </div>
</div>

<div id="plot"></div>

<script>
const LOT_DATA = {lot_data_json};
const LOTS = {lots_json};
let currentLot = LOTS[0];
let currentSource = 'stac';

// Populate dropdown
const sel = document.getElementById('lot-select');
LOTS.forEach((lot, i) => {{
  const opt = document.createElement('option');
  opt.value = lot;
  opt.textContent = lot;
  sel.appendChild(opt);
}});
sel.value = currentLot;
sel.addEventListener('change', function() {{
  currentLot = this.value;
  document.getElementById('lot-title').textContent = currentLot;
  renderPlot();
}});

function setSource(src) {{
  currentSource = src;
  document.getElementById('btn-stac').classList.toggle('active', src === 'stac');
  document.getElementById('btn-ref').classList.toggle('active', src === 'ref');
  renderPlot();
}}

function renderPlot() {{
  const cycles = LOT_DATA[currentLot];
  const traces = cycles.map(c => {{
    const y = currentSource === 'stac' ? c.stac : c.ref;
    const customdata = c.fecha.map((f, i) => [f, c.id_img[i]]);
    return {{
      x: c.edad,
      y: y,
      customdata: customdata,
      mode: 'lines+markers',
      name: 'cierre ' + c.cierre,
      line: {{ color: c.color, width: 2 }},
      marker: {{ size: 4 }},
      hovertemplate:
        'edad: <b>%{{x}}</b><br>' +
        'ndvi: <b>%{{y:.4f}}</b><br>' +
        'fecha: %{{customdata[0]}}<br>' +
        'imagen: %{{customdata[1]}}' +
        '<extra>cierre ' + c.cierre + '</extra>',
    }};
  }});

  const yLabel = currentSource === 'stac' ? 'NDVI corregido (STAC)' : 'NDVI Referencia';

  Plotly.react('plot', traces, {{
    xaxis: {{ title: 'Edad de cultivo (días desde último cierre)', range: [0, 530] }},
    yaxis: {{ title: yLabel, range: [-0.1, 0.85] }},
    legend: {{ title: {{ text: 'Ciclo (cierre)' }}, x: 1.01, y: 1, xanchor: 'left' }},
    height: 560,
    margin: {{ t: 30, r: 180, l: 60, b: 60 }},
    hovermode: 'x unified',
    template: 'plotly_white',
  }});
}}

renderPlot();
</script>
</body>
</html>"""


def main() -> None:
    tunnel, port = open_tunnel()
    try:
        engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@127.0.0.1:{port}/{DB_NAME}")
        df = load_data(engine)
    finally:
        tunnel.terminate()

    html = build_html(df)
    OUTPUT_PATH.write_text(html, encoding="utf-8")
    print(f"Saved: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
