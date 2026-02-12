# Iceland ski atlas pipeline: analyze + convert_to_geoparquet (compose overrides CMD)
FROM python:3.11-slim

WORKDIR /app

# Install system deps for geopandas
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgeos-dev \
    libproj-dev \
    gdal-bin \
    libgdal-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py .
COPY scripts/enrich_geojson_properties.py scripts/
RUN mkdir -p boundaries

VOLUME ["/app/output"]

CMD ["python", "analyze_ski_areas.py", "--help"]
