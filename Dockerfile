# ── Base image ────────────────────────────────────────────────────────────────
# python:3.11-bookworm (non-slim) includes libexpat1 and other base libraries
# required by rasterio's bundled GDAL — avoids apt-get network issues
FROM python:3.11-bookworm

# ── Python dependencies ───────────────────────────────────────────────────────
# rasterio and fiona wheels from PyPI include GDAL bundled inside them
COPY pipelines/requirements_stac.txt /app/requirements_stac.txt
COPY pipelines/requirements_reference.txt /app/requirements_reference.txt
RUN pip install --no-cache-dir -r /app/requirements_stac.txt && \
    pip install --no-cache-dir -r /app/requirements_reference.txt

# ── Application ───────────────────────────────────────────────────────────────
COPY pipelines/process_shapefile.py /app/process_shapefile.py
COPY pipelines/process_reference_tiff.py /app/process_reference_tiff.py
COPY entrypoint.sh /app/entrypoint.sh

RUN chmod +x /app/entrypoint.sh

WORKDIR /app

# ── Entry point ───────────────────────────────────────────────────────────────
ENTRYPOINT ["/app/entrypoint.sh"]
