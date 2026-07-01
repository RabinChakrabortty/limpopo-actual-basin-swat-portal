# Limpopo Hybrid Digital Twin Portal

## Works now, without keys
- Open-Meteo 16-day forecast: rainfall, temperature, ET0, wind, humidity
- Open-Meteo historical climate: control-period analysis
- Open-Meteo Flood: discharge screening up to 30 days
- Open-Meteo Air Quality
- NASA POWER point climate comparison
- Control-period charts and up-to-10-year dry/baseline/wet/high-demand scenario projections
- Upload interface and live CSV download

## Actual GIS geometry — two choices
### 1. Upload to `data/vector/`
- `limpopo_basin_boundary.geojson`
- `limpopo_subbasins_level4.geojson`
- `limpopo_subbasins_level6.geojson`
- `limpopo_river_network.geojson`
- `limpopo_monitoring_stations.geojson`
- `swat_subbasins.geojson`

### 2. Configure direct official GeoJSON URLs in Render
- `BASIN_BOUNDARY_GEOJSON_URL`
- `SUBBASINS_L4_GEOJSON_URL`
- `SUBBASINS_L6_GEOJSON_URL`
- `RIVER_NETWORK_GEOJSON_URL`
- `STATIONS_GEOJSON_URL`
- `SWAT_SUBBASINS_GEOJSON_URL`

## Optional account-based modules
- Google Earth Engine: `GEE_PROJECT_ID`
- Copernicus CDS: `CDS_API_KEY`
- Copernicus Data Space: `COPERNICUS_CLIENT_ID`, `COPERNICUS_CLIENT_SECRET`

Keep credentials in Render Environment Variables, never GitHub.

## Render commands
Build: `pip install -r requirements.txt`

Start: `python -m uvicorn main:app --host 0.0.0.0 --port $PORT`

## Scientific rule
The portal does not invent basin/sub-basin geometry. Use valid HydroBASINS, HydroATLAS, LIMCOM/national authority, or SWAT delineated data.

For production, place large rasters and SWAT outputs in cloud/object storage. Render local uploads can be erased after redeployment.
