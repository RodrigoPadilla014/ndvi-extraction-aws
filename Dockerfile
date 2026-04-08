# ── Base image ────────────────────────────────────────────────────────────────
# python:3.11-bookworm (non-slim) includes libexpat1 and other base libraries
# required by rasterio's bundled GDAL — avoids apt-get network issues
FROM python:3.11-bookworm

# ── Python dependencies ───────────────────────────────────────────────────────
# rasterio and fiona wheels from PyPI include GDAL bundled inside them
COPY scripts/requirements.txt /app/scripts_requirements.txt
COPY reference/requirements.txt /app/reference_requirements.txt
RUN pip install --no-cache-dir -r /app/scripts_requirements.txt && \
    pip install --no-cache-dir -r /app/reference_requirements.txt

# ── Application ───────────────────────────────────────────────────────────────
COPY scripts/process_shapefile.py /app/process_shapefile.py
COPY reference/process_reference_tiff.py /app/process_reference_tiff.py
COPY entrypoint.sh /app/entrypoint.sh

RUN chmod +x /app/entrypoint.sh

WORKDIR /app

# ── Entry point ───────────────────────────────────────────────────────────────
ENTRYPOINT ["/app/entrypoint.sh"]
