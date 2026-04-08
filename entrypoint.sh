#!/bin/bash
# -*- coding: utf-8 -*-
#
# entrypoint.sh — Routes Docker container to the appropriate pipeline script
#
# Checks SCRIPT_TYPE environment variable and runs:
#   - process_shapefile.py      if SCRIPT_TYPE=shapefile  (STAC pipeline, one job per .shp)
#   - process_reference_tiff.py if SCRIPT_TYPE=reference  (reference pipeline, one job per year)

set -e

SCRIPT_TYPE="${SCRIPT_TYPE:-shapefile}"

case "$SCRIPT_TYPE" in
    shapefile)
        echo "[ENTRYPOINT] Running STAC shapefile pipeline..."
        python /app/process_shapefile.py
        ;;
    reference)
        echo "[ENTRYPOINT] Running reference TIFF pipeline..."
        python /app/process_reference_tiff.py
        ;;
    *)
        echo "[ENTRYPOINT] ERROR: Invalid SCRIPT_TYPE='$SCRIPT_TYPE'. Must be 'shapefile' or 'reference'."
        exit 1
        ;;
esac
