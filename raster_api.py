from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import HTMLResponse, Response

from drive_storage import (
    CACHE_DIR,
    drive_raster_catalogue,
    ensure_raster_available,
    resolve_folder_id,
)
from raster_utils import (
    calculate_histogram,
    calculate_statistics,
    create_transparent_png,
    read_basin_masked_raster,
)

router = APIRouter()

BASE_DIR = Path(__file__).resolve().parent
BASIN_FILE = (
    BASE_DIR / "data" / "vector" / "limpopo_basin_boundary.geojson"
)

RASTER_STYLE = {
    "dem.tif": ("dem", "Elevation", "m", "terrain", "continuous"),
    "slope.tif": ("slope", "Slope", "degree", "YlOrBr", "continuous"),
    "aspect.tif": ("aspect", "Aspect", "degree", "hsv", "continuous"),
    "hillshade.tif": (
        "hillshade", "Hillshade", "index", "gray", "continuous"
    ),
    "rainfall_recent_2021_2025.tif": (
        "rainfall_recent",
        "Recent rainfall 2021–2025",
        "mm/year",
        "Blues",
        "continuous",
    ),
    "rainfall_climatology_1991_2020.tif": (
        "rainfall_climatology",
        "Rainfall climatology 1991–2020",
        "mm/year",
        "Blues",
        "continuous",
    ),
    "rainfall_anomaly_2021_2025_vs_1991_2020.tif": (
        "rainfall_anomaly",
        "Rainfall anomaly",
        "mm/year",
        "RdBu",
        "continuous",
    ),
    "rainfall_anomaly_percent_2021_2025_vs_1991_2020.tif": (
        "rainfall_anomaly_percent",
        "Rainfall anomaly percentage",
        "%",
        "RdBu",
        "continuous",
    ),
    "temperature_mean_2021_2025.tif": (
        "temperature_mean",
        "Mean air temperature 2021–2025",
        "°C",
        "RdYlBu_r",
        "continuous",
    ),
    "temperature_max_mean_2021_2025.tif": (
        "temperature_max",
        "Mean maximum temperature",
        "°C",
        "inferno",
        "continuous",
    ),
    "soil_moisture_mean_2021_2025.tif": (
        "soil_moisture",
        "Mean soil moisture",
        "m³/m³",
        "YlGnBu",
        "continuous",
    ),
    "runoff_annual_mean_2021_2025.tif": (
        "runoff",
        "Mean annual runoff",
        "mm/year",
        "Blues",
        "continuous",
    ),
    "lst_day_mean_2021_2025.tif": (
        "lst_day",
        "Mean daytime land-surface temperature",
        "°C",
        "inferno",
        "continuous",
    ),
    "et_annual_mean_2021_2025.tif": (
        "et",
        "Mean annual evapotranspiration",
        "mm/year",
        "YlGnBu",
        "continuous",
    ),
    "ndvi_mean_2021_2025.tif": (
        "ndvi_mean",
        "Mean NDVI",
        "index",
        "RdYlGn",
        "continuous",
    ),
    "ndvi_p10_2021_2025.tif": (
        "ndvi_p10",
        "NDVI 10th percentile",
        "index",
        "RdYlGn",
        "continuous",
    ),
    "worldcover_2021.tif": (
        "worldcover",
        "ESA WorldCover 2021",
        "class",
        "tab20",
        "categorical",
    ),
    "surface_water_occurrence.tif": (
        "water_occurrence",
        "Surface-water occurrence",
        "%",
        "Blues",
        "continuous",
    ),
    "surface_water_seasonality.tif": (
        "water_seasonality",
        "Surface-water seasonality",
        "months",
        "Blues",
        "continuous",
    ),
    "population_2020.tif": (
        "population",
        "Population count 2020",
        "persons/cell",
        "magma",
        "continuous",
    ),
    "merit_river_mask.tif": (
        "merit_river_mask",
        "MERIT Hydro river mask",
        "binary",
        "Blues",
        "categorical",
    ),
}

def style_for_filename(filename: str) -> dict[str, Any]:
    entry = RASTER_STYLE.get(filename)
    if entry:
        layer_id, name, units, palette, kind = entry
    else:
        layer_id = Path(filename).stem
        name = Path(filename).stem.replace("_", " ").title()
        units = "unknown"
        palette = "viridis"
        kind = "continuous"

    return {
        "id": layer_id,
        "name": name,
        "units": units,
        "palette": palette,
        "type": kind,
        "filename": filename,
    }

def layer_from_id(layer_id: str) -> dict[str, Any]:
    for filename in RASTER_STYLE:
        style = style_for_filename(filename)
        if style["id"] == layer_id:
            return style

    for item in drive_raster_catalogue():
        style = style_for_filename(item["filename"])
        if style["id"] == layer_id:
            return style

    raise HTTPException(
        status_code=404,
        detail="Unknown raster layer.",
    )

@router.get("/api/raster/catalog")
def raster_catalogue():
    try:
        drive_items = drive_raster_catalogue()
        folder_id = resolve_folder_id()
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Google Drive catalogue failed: {exc}",
        )

    layers = [
        {**item, **style_for_filename(item["filename"])}
        for item in drive_items
    ]

    return {
        "storage": "google_drive",
        "folder_name": os.getenv(
            "GOOGLE_DRIVE_FOLDER_NAME",
            "Limpopo_DigitalTwin_Exports",
        ),
        "folder_id": folder_id,
        "basin_available": BASIN_FILE.exists(),
        "cache_directory": str(CACHE_DIR),
        "layers": layers,
    }

@router.get("/api/raster/{layer_id}/metadata")
def raster_metadata(layer_id: str):
    layer = layer_from_id(layer_id)
    try:
        raster_path = ensure_raster_available(layer["filename"])
        array, valid, bounds, metadata = read_basin_masked_raster(
            raster_path,
            BASIN_FILE,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=str(exc),
        )

    west, south, east, north = bounds
    return {
        "layer": layer,
        "statistics": calculate_statistics(array, valid),
        "bounds_wgs84": {
            "west": west,
            "south": south,
            "east": east,
            "north": north,
        },
        "raster": metadata,
        "cached_file": str(raster_path),
        "masked_width": int(array.shape[1]),
        "masked_height": int(array.shape[0]),
    }

@router.get("/api/raster/{layer_id}/histogram")
def raster_histogram(
    layer_id: str,
    bins: int = Query(30, ge=5, le=100),
):
    layer = layer_from_id(layer_id)
    try:
        raster_path = ensure_raster_available(layer["filename"])
        array, valid, _, _ = read_basin_masked_raster(
            raster_path,
            BASIN_FILE,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=str(exc),
        )

    return {
        "layer": layer,
        "histogram": calculate_histogram(
            array,
            valid,
            bins,
        ),
    }

@router.get("/api/raster/{layer_id}/preview.png")
def raster_preview(
    layer_id: str,
    opacity: float = Query(0.75, ge=0.05, le=1.0),
):
    layer = layer_from_id(layer_id)
    try:
        raster_path = ensure_raster_available(layer["filename"])
        result = create_transparent_png(
            raster_path=raster_path,
            basin_path=BASIN_FILE,
            palette=layer["palette"],
            opacity=opacity,
            categorical=layer["type"] == "categorical",
        )
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=str(exc),
        )

    return Response(
        content=result["png"],
        media_type="image/png",
        headers={"Cache-Control": "public, max-age=3600"},
    )

@router.get("/rasters", response_class=HTMLResponse)
def raster_page():
    return HTMLResponse(RASTER_HTML)

RASTER_HTML = r"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Limpopo GEE Raster Explorer</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
body{margin:0;font-family:Arial;background:#eef2f5;color:#172033}
header{background:#101b2d;color:white;padding:14px 18px}
header h1{margin:0;font-size:20px}header small{color:#cbd5e1}
main{display:grid;grid-template-columns:330px 1fr;height:calc(100vh - 72px)}
aside{background:white;padding:14px;overflow:auto;border-right:1px solid #d7dee7}
label{font-size:12px;font-weight:bold;display:block;margin-top:10px}
select,input,button{width:100%;padding:9px;margin-top:5px;border-radius:4px;border:1px solid #cfd7e3}
button{background:#087ca7;color:white;font-weight:bold;cursor:pointer}
#map{height:100%}.status{font-size:12px;margin-top:10px;padding:8px;background:#eef6f8}
.panel{position:absolute;z-index:900;right:15px;top:90px;width:470px;max-height:80vh;overflow:auto;background:white;padding:14px;box-shadow:0 8px 30px #0004;display:none}
.panel.show{display:block}.chart{height:310px}.cards{display:grid;grid-template-columns:repeat(3,1fr);gap:7px}
.card{background:#f2f5f7;padding:8px}.card small{display:block;color:#667085}
</style>
</head>
<body>
<header><h1>Limpopo GEE Raster Explorer</h1><small>Google Drive discovery • tile mosaic • basin masking • transparent display</small></header>
<main>
<aside>
<label>Raster layer</label><select id="layer"></select>
<label>Opacity</label><input id="opacity" type="range" min=".1" max="1" step=".05" value=".75">
<button onclick="loadRaster()">Load raster</button>
<button onclick="showAnalytics()">Statistics and charts</button>
<button onclick="clearRaster()">Clear</button>
<div id="status" class="status">Loading Google Drive catalogue…</div>
<p style="font-size:11px;color:#667085">Large first-time requests may take several minutes because the portal must download and possibly mosaic Earth Engine tiles.</p>
</aside>
<div id="map"></div>
<section id="panel" class="panel"><button onclick="closePanel()">Close</button><h3 id="title"></h3><div id="content"></div></section>
</main>
<script>
let map=L.map('map').setView([-23.7,30.0],6),overlay=null;
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',{attribution:'© OpenStreetMap'}).addTo(map);
async function start(){
  let r=await fetch('/api/raster/catalog'),d=await r.json();
  if(!r.ok){document.getElementById('status').textContent=d.detail;return}
  let s=document.getElementById('layer');
  d.layers.forEach(x=>s.add(new Option((x.tiled?'Tiles: ':'')+x.name,x.id)));
  document.getElementById('status').textContent=`${d.layers.length} raster layers found in ${d.folder_name}.`;
}
async function loadRaster(){
  let id=document.getElementById('layer').value,op=document.getElementById('opacity').value;
  document.getElementById('status').textContent='Downloading/processing raster. Please wait…';
  let r=await fetch('/api/raster/'+id+'/metadata'),d=await r.json();
  if(!r.ok){document.getElementById('status').textContent=d.detail;return}
  clearRaster();
  let b=d.bounds_wgs84,bounds=[[b.south,b.west],[b.north,b.east]];
  overlay=L.imageOverlay('/api/raster/'+id+'/preview.png?opacity='+op+'&t='+Date.now(),bounds,{opacity:1}).addTo(map);
  map.fitBounds(bounds);
  document.getElementById('status').textContent=`${d.layer.name}: mean ${d.statistics.mean.toFixed(3)} ${d.layer.units}; ${d.statistics.count} valid pixels.`;
}
function clearRaster(){if(overlay){map.removeLayer(overlay);overlay=null}}
function closePanel(){document.getElementById('panel').classList.remove('show')}
async function showAnalytics(){
  let id=document.getElementById('layer').value;
  let [mr,hr]=await Promise.all([fetch('/api/raster/'+id+'/metadata'),fetch('/api/raster/'+id+'/histogram')]);
  let m=await mr.json(),h=await hr.json();
  if(!mr.ok||!hr.ok){alert(m.detail||h.detail);return}
  let s=m.statistics;
  document.getElementById('title').textContent=m.layer.name;
  document.getElementById('content').innerHTML=`<div class=cards>
  <div class=card><small>Minimum</small><b>${s.minimum.toFixed(3)}</b></div>
  <div class=card><small>Mean</small><b>${s.mean.toFixed(3)}</b></div>
  <div class=card><small>Maximum</small><b>${s.maximum.toFixed(3)}</b></div>
  <div class=card><small>Median</small><b>${s.median.toFixed(3)}</b></div>
  <div class=card><small>Std. deviation</small><b>${s.standard_deviation.toFixed(3)}</b></div>
  <div class=card><small>Valid pixels</small><b>${s.count}</b></div></div>
  <div id=hist class=chart></div><div id=profile class=chart></div>`;
  document.getElementById('panel').classList.add('show');
  Plotly.newPlot('hist',[{x:h.histogram.centres,y:h.histogram.counts,type:'bar'}],
    {title:'Basin pixel-value histogram',xaxis:{title:m.layer.units},yaxis:{title:'Frequency'}},{responsive:true});
  Plotly.newPlot('profile',[{x:['Min','P02','P05','P10','P25','Median','Mean','P75','P90','P95','P98','Max'],
    y:[s.minimum,s.p02,s.p05,s.p10,s.p25,s.median,s.mean,s.p75,s.p90,s.p95,s.p98,s.maximum],
    type:'scatter',mode:'lines+markers'}],
    {title:'Statistical profile',yaxis:{title:m.layer.units}},{responsive:true});
}
start();
</script>
</body>
</html>"""
