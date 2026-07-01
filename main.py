from __future__ import annotations

import io
import json
import zipfile
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

# ===============================================================
# LIMPOPO ACTUAL BASIN + SUB-BASIN + STATION + SWAT DATA PORTAL
# IMPORTANT: NO FAKE RECTANGLES OR INVENTED BASIN GEOMETRY.
# The map displays only real uploaded GeoJSON files.
# ===============================================================

BASE = Path(__file__).resolve().parent
DATA = BASE / "data"
VECTOR = DATA / "vector"
RASTER = DATA / "raster"
SWAT = DATA / "swat"
META = DATA / "metadata"
for p in [VECTOR, RASTER, SWAT, META]:
    p.mkdir(parents=True, exist_ok=True)

app = FastAPI(
    title="Limpopo Basin Digital Twin and SWAT Portal",
    version="3.0.0",
    description="Actual-boundary data catalogue for Limpopo Basin, HydroBASINS, stations, rasters and SWAT outputs."
)
app.mount("/files", StaticFiles(directory=str(DATA)), name="files")

# ----------------------------------------------------------------
# DATA CATALOGUE. Paths are relative to data/.
# Add exact raster preview bounds in metadata/catalogue.json.
# ----------------------------------------------------------------
DEFAULT = {
  "portal": {
    "title": "Limpopo Basin Digital Twin and SWAT Data Portal",
    "description": "Basin-scale, sub-basin-scale and station-scale data catalogue using only actual uploaded data.",
    "note": "The portal intentionally does not draw placeholder boundaries. Upload valid HydroBASINS/SWAT GeoJSON files."
  },
  "categories": [
    "Natural Basin Characteristics",
    "Flood Monitoring & Early Warning",
    "Drought Monitoring & Risk Assessment",
    "Socio-Economic Profile",
    "Water Resources",
    "Water Use Monitoring & Analysis",
    "Ecosystems",
    "Satellite Images",
    "SWAT Model Outputs",
    "Predictions & Scenarios",
    "Data Sources & Providers"
  ],
  "datasets": [
    {"id":"basin_boundary","name":"Actual Limpopo Basin Boundary","category":"Natural Basin Characteristics","kind":"vector","path":"vector/limpopo_basin_boundary.geojson","format":"GeoJSON","source":"HydroBASINS / HydroATLAS or SWAT delineation","description":"Actual outer Limpopo Basin watershed polygon.","period":"Static reference boundary","resolution":"Vector polygon"},
    {"id":"hydrobasins_l4","name":"HydroBASINS Level 4 Sub-basins","category":"Natural Basin Characteristics","kind":"vector","path":"vector/limpopo_subbasins_level4.geojson","format":"GeoJSON","source":"HydroBASINS Africa","description":"Major nested sub-basins for basin reporting.","period":"Static reference boundary","resolution":"HydroBASINS Level 4"},
    {"id":"hydrobasins_l6","name":"HydroBASINS Level 6 Sub-basins","category":"Natural Basin Characteristics","kind":"vector","path":"vector/limpopo_subbasins_level6.geojson","format":"GeoJSON","source":"HydroBASINS Africa","description":"Detailed operational sub-basins.","period":"Static reference boundary","resolution":"HydroBASINS Level 6"},
    {"id":"swat_subbasins","name":"SWAT Delineated Sub-basins","category":"SWAT Model Outputs","kind":"vector","path":"vector/swat_subbasins.geojson","format":"GeoJSON","source":"SWAT / SWAT+ project","description":"Modelled sub-basins from SWAT watershed delineation.","period":"Static model geometry","resolution":"SWAT sub-basin"},
    {"id":"swat_hrus","name":"SWAT Hydrologic Response Units","category":"SWAT Model Outputs","kind":"vector","path":"vector/swat_hrus.geojson","format":"GeoJSON","source":"SWAT / SWAT+ project","description":"Land-use, soil and slope response units.","period":"Static model geometry","resolution":"SWAT HRU"},
    {"id":"river_network","name":"Limpopo River Network","category":"Water Resources","kind":"vector","path":"vector/limpopo_river_network.geojson","format":"GeoJSON","source":"HydroRIVERS / SWAT reach network","description":"Actual river and tributary network clipped to basin.","period":"Static reference network","resolution":"Vector line"},
    {"id":"stations","name":"Monitoring Stations","category":"Flood Monitoring & Early Warning","kind":"vector","path":"vector/limpopo_monitoring_stations.geojson","format":"GeoJSON","source":"National agencies / LIMCOM / SWAT outlets","description":"Rainfall, discharge, water quality and reservoir gauge stations.","period":"Station-specific time series","resolution":"Point"},
    {"id":"reservoirs","name":"Reservoirs and Dams","category":"Water Resources","kind":"vector","path":"vector/limpopo_reservoirs.geojson","format":"GeoJSON","source":"GRanD / national authorities / SWAT","description":"Actual reservoir and dam locations.","period":"Static + time-series","resolution":"Point/polygon"},
    {"id":"rainfall","name":"Monthly Rainfall","category":"Drought Monitoring & Risk Assessment","kind":"raster","path":"raster/rainfall_monthly_latest.tif","preview_path":"raster/rainfall_monthly_latest_preview.png","format":"GeoTIFF","source":"CHIRPS / ERA5-Land / gauge interpolation","description":"Monthly rainfall raster clipped to actual basin.","period":"Monthly; long-term control period available","resolution":"Source-dependent"},
    {"id":"rainfall_anomaly","name":"Rainfall Anomaly","category":"Drought Monitoring & Risk Assessment","kind":"raster","path":"raster/rainfall_anomaly_latest.tif","preview_path":"raster/rainfall_anomaly_latest_preview.png","format":"GeoTIFF","source":"Calculated from selected control period","description":"Rainfall anomaly relative to control period.","period":"Monthly / seasonal / annual","resolution":"Source-dependent"},
    {"id":"et0","name":"Potential Evapotranspiration (ET0)","category":"Drought Monitoring & Risk Assessment","kind":"raster","path":"raster/et0_monthly_latest.tif","preview_path":"raster/et0_monthly_latest_preview.png","format":"GeoTIFF","source":"ERA5-Land / FAO-56 workflow","description":"Potential evapotranspiration raster.","period":"Monthly","resolution":"Source-dependent"},
    {"id":"water_balance","name":"Rainfall Minus ET0 Water Balance","category":"Drought Monitoring & Risk Assessment","kind":"raster","path":"raster/water_balance_latest.tif","preview_path":"raster/water_balance_latest_preview.png","format":"GeoTIFF","source":"Calculated P - ET0","description":"Climatic water-balance raster.","period":"Monthly / seasonal / annual","resolution":"Source-dependent"},
    {"id":"soil_moisture","name":"Soil Moisture","category":"Drought Monitoring & Risk Assessment","kind":"raster","path":"raster/soil_moisture_latest.tif","preview_path":"raster/soil_moisture_latest_preview.png","format":"GeoTIFF","source":"SMAP / ERA5-Land / model output","description":"Surface or root-zone soil moisture / anomaly.","period":"Daily to monthly","resolution":"Source-dependent"},
    {"id":"ndvi","name":"Vegetation and Crop Stress (NDVI)","category":"Drought Monitoring & Risk Assessment","kind":"raster","path":"raster/ndvi_latest.tif","preview_path":"raster/ndvi_latest_preview.png","format":"GeoTIFF","source":"MODIS / Sentinel-2","description":"Vegetation condition and crop stress layer.","period":"8-day to monthly","resolution":"10-250 m"},
    {"id":"lulc","name":"Land Use / Land Cover","category":"Natural Basin Characteristics","kind":"raster","path":"raster/lulc_latest.tif","preview_path":"raster/lulc_latest_preview.png","format":"GeoTIFF","source":"ESA WorldCover / Copernicus Land Cover","description":"Land cover classes: urban, cropland, forest, grassland, water and bare land.","period":"Latest annual layer","resolution":"10-100 m"},
    {"id":"population","name":"Population Density","category":"Socio-Economic Profile","kind":"raster","path":"raster/population_density_latest.tif","preview_path":"raster/population_density_latest_preview.png","format":"GeoTIFF","source":"WorldPop / GHSL","description":"Population density for flood and drought exposure assessment.","period":"Latest available year","resolution":"Source-dependent"},
    {"id":"flood_hazard","name":"Flood Hazard","category":"Flood Monitoring & Early Warning","kind":"raster","path":"raster/flood_hazard_latest.tif","preview_path":"raster/flood_hazard_latest_preview.png","format":"GeoTIFF","source":"GloFAS / Sentinel-1 / hydraulic model / SWAT","description":"Flood hazard or inundation-risk raster.","period":"Event / seasonal / historical","resolution":"Source-dependent"},
    {"id":"drought_risk","name":"Composite Drought Risk","category":"Drought Monitoring & Risk Assessment","kind":"raster","path":"raster/drought_risk_latest.tif","preview_path":"raster/drought_risk_latest_preview.png","format":"GeoTIFF","source":"Calculated P, ET0, soil moisture, NDVI and exposure","description":"Composite drought-risk layer.","period":"Monthly / seasonal","resolution":"Source-dependent"}
  ],
  "swat_outputs": [
    {"id":"basin","name":"SWAT Basin Water Balance","path":"swat/basin_water_balance.csv","description":"Basin precipitation, ET, surface runoff, lateral flow, groundwater, water yield and storage."},
    {"id":"subbasin","name":"SWAT Sub-basin Outputs","path":"swat/subbasin_outputs.csv","description":"Sub-basin water yield, runoff, ET, sediment, nutrients and water-balance outputs."},
    {"id":"reach","name":"SWAT Reach Outputs","path":"swat/reach_outputs.csv","description":"Reach discharge, sediment, water quality and environmental-flow outputs."},
    {"id":"hru","name":"SWAT HRU Outputs","path":"swat/hru_outputs.csv","description":"HRU ET, soil water, runoff, percolation and crop-water-use outputs."},
    {"id":"reservoir","name":"SWAT Reservoir Outputs","path":"swat/reservoir_outputs.csv","description":"Reservoir inflow, outflow, storage, evaporation and spill outputs."},
    {"id":"station","name":"Observed and Modelled Station Time Series","path":"swat/station_timeseries.csv","description":"Station-level rainfall, observed discharge, simulated discharge and water-quality data."}
  ]
}

def data_path(relative: str) -> Path:
    return DATA / relative

def load_catalogue() -> dict:
    p = META / "catalogue.json"
    if not p.exists():
        return DEFAULT
    try:
        override = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return DEFAULT
    # Merge by dataset id so preview bounds or descriptions can be overridden.
    result = json.loads(json.dumps(DEFAULT))
    if isinstance(override.get("portal"), dict):
        result["portal"].update(override["portal"])
    if isinstance(override.get("categories"), list):
        result["categories"] = override["categories"]
    for key in ["datasets", "swat_outputs"]:
        if isinstance(override.get(key), list):
            byid = {x["id"]: x for x in result[key]}
            for x in override[key]:
                if x.get("id") in byid:
                    byid[x["id"]].update(x)
                else:
                    result[key].append(x)
    return result

def item_status(item: dict) -> str:
    return "Available" if data_path(item["path"]).exists() else "Missing"

def dataset_by_id(dataset_id: str) -> dict:
    for x in load_catalogue()["datasets"]:
        if x["id"] == dataset_id:
            return x
    raise HTTPException(404, "Dataset not found")

def swat_by_id(component_id: str) -> dict:
    for x in load_catalogue()["swat_outputs"]:
        if x["id"] == component_id:
            return x
    raise HTTPException(404, "SWAT component not found")

def public_dataset(x: dict) -> dict:
    y = dict(x)
    y["status"] = item_status(x)
    y["download_url"] = "/download/dataset/" + x["id"]
    y["geojson_url"] = "/api/geojson/" + x["id"] if x["kind"] == "vector" else None
    pp = x.get("preview_path")
    y["preview_url"] = "/files/" + pp if pp and data_path(pp).exists() else None
    return y

def public_swat(x: dict) -> dict:
    y = dict(x)
    y["status"] = item_status(x)
    y["download_url"] = "/download/swat/" + x["id"]
    return y

def read_geojson(dataset_id: str) -> dict:
    x = dataset_by_id(dataset_id)
    if x["kind"] != "vector":
        raise HTTPException(400, "This dataset is not vector geometry")
    p = data_path(x["path"])
    if not p.exists():
        raise HTTPException(404, f"Actual GeoJSON is missing: {x['path']}")
    try:
        value = json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        raise HTTPException(400, f"Invalid GeoJSON: {e}")
    if value.get("type") not in ["FeatureCollection", "Feature"]:
        raise HTTPException(400, "Not valid GeoJSON")
    return value

def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise HTTPException(404, f"SWAT file missing: {path.relative_to(DATA)}")
    for kwargs in [{"sep":","}, {"sep":";"}, {"sep":"\t"}, {"sep":r"\s+", "engine":"python"}]:
        try:
            df = pd.read_csv(path, **kwargs)
            if len(df.columns) >= 2:
                return df
        except Exception:
            pass
    raise HTTPException(400, "Cannot parse CSV. Use comma-separated CSV.")

def find_date_column(df: pd.DataFrame) -> str | None:
    names = {str(c).lower():str(c) for c in df.columns}
    for n in ["date","datetime","time","timestamp","day"]:
        if n in names: return names[n]
    for c in df.columns:
        lc = str(c).lower()
        if "date" in lc or "time" in lc: return str(c)
    return None

def find_id_column(df: pd.DataFrame) -> str | None:
    names = {str(c).lower():str(c) for c in df.columns}
    for n in ["subbasin_id","subbasin","sub","reach_id","reach","hru_id","hru","station_id","station","id"]:
        if n in names: return names[n]
    return None

def numeric_cols(df: pd.DataFrame) -> list[str]:
    out = []
    for c in df.columns:
        if pd.to_numeric(df[c], errors="coerce").notna().sum() > 0:
            out.append(str(c))
    return out

def prepare_df(df: pd.DataFrame) -> tuple[pd.DataFrame, str | None]:
    date_col = find_date_column(df)
    out = df.copy()
    if date_col:
        d = pd.to_datetime(out[date_col], errors="coerce")
        out = out.loc[d.notna()].copy()
        out["_date"] = d.loc[d.notna()]
    return out, date_col

def apply_dates(df: pd.DataFrame, start: str | None, end: str | None) -> pd.DataFrame:
    if "_date" not in df.columns: return df.copy()
    out = df.copy()
    if start: out = out[out["_date"] >= pd.Timestamp(start)]
    if end: out = out[out["_date"] <= pd.Timestamp(end)]
    return out

def aggregate(df: pd.DataFrame, col: str, temporal: str) -> list[dict]:
    if "_date" not in df.columns:
        raise HTTPException(400, "Date column required for temporal analysis")
    x = df[["_date", col]].copy()
    x[col] = pd.to_numeric(x[col], errors="coerce")
    x = x.dropna().sort_values("_date")
    if x.empty: return []
    if temporal == "daily":
        g = x.groupby(x["_date"].dt.date)[col].mean()
        idx = [str(z) for z in g.index]
    elif temporal == "weekly":
        g = x.groupby(x["_date"].dt.to_period("W"))[col].mean()
        idx = [str(z) for z in g.index]
    elif temporal == "monthly":
        g = x.groupby(x["_date"].dt.to_period("M"))[col].mean()
        idx = [str(z) for z in g.index]
    elif temporal == "seasonal":
        season = x["_date"].dt.month.map(lambda m:"DJF" if m in [12,1,2] else "MAM" if m in [3,4,5] else "JJA" if m in [6,7,8] else "SON")
        g = x.groupby(x["_date"].dt.year.astype(str)+"-"+season)[col].mean()
        idx = [str(z) for z in g.index]
    elif temporal == "annual":
        g = x.groupby(x["_date"].dt.year)[col].mean()
        idx = [str(z) for z in g.index]
    else:
        raise HTTPException(400, "Temporal must be daily, weekly, monthly, seasonal or annual")
    return [{"period":i, "value":round(float(v),4)} for i,v in zip(idx, g.tolist())]

def control_stats(df: pd.DataFrame, col: str) -> dict:
    x = pd.to_numeric(df[col], errors="coerce").dropna()
    if x.empty: return {"count":0,"mean":None,"min":None,"max":None,"std":None}
    return {"count":int(x.count()),"mean":round(float(x.mean()),4),"min":round(float(x.min()),4),"max":round(float(x.max()),4),"std":round(float(x.std(ddof=0)),4)}

def projection(df: pd.DataFrame, col: str, start_year: int, end_year: int, scenario: str) -> list[dict]:
    if "_date" not in df.columns:
        raise HTTPException(400, "Date column required for prediction")
    x = df[["_date", col]].copy()
    x[col] = pd.to_numeric(x[col], errors="coerce")
    x = x.dropna()
    if x.empty: raise HTTPException(400, "No values in selected control period")
    clim = x.assign(month=x["_date"].dt.month).groupby("month")[col].mean()
    factor = {"dry":0.80,"baseline":1.0,"wet":1.20,"high_demand":0.90}.get(scenario,1.0)
    rows=[]
    for yr in range(start_year,end_year+1):
        for mo in range(1,13):
            val=float(clim.get(mo,x[col].mean()))*factor
            rows.append({"period":f"{yr}-{mo:02d}","year":yr,"month":mo,"predicted_value":round(val,4)})
    return rows

@app.get("/api/catalogue")
def catalogue():
    c=load_catalogue()
    return {"portal":c["portal"],"categories":c["categories"],"datasets":[public_dataset(x) for x in c["datasets"]],"swat_outputs":[public_swat(x) for x in c["swat_outputs"]]}

@app.get("/api/basin-description")
def basin_description():
    p=META/"basin_description.json"
    fallback={"title":"Limpopo River Basin","general_description":"This portal supports actual basin, sub-basin and station datasets, raster catalogues, SWAT outputs, control-period analysis and scenarios.","specific_discussion":"Use HydroBASINS/HydroATLAS or SWAT-delineated polygons. Ten-year outputs are hydroclimate scenario projections, not exact daily weather forecasts.","sections":["Basin overview","Natural basin characteristics","Flood monitoring","Drought monitoring","LULC","Population exposure","Water resources","SWAT outputs","Predictions and scenarios","Downloads and metadata"]}
    if p.exists():
        try: return json.loads(p.read_text(encoding="utf-8"))
        except Exception: pass
    return fallback

@app.get("/api/geojson/{dataset_id}")
def geojson(dataset_id: str):
    return JSONResponse(read_geojson(dataset_id))

@app.get("/api/swat/{component_id}/columns")
def swat_columns(component_id: str):
    item=swat_by_id(component_id)
    df=read_csv(data_path(item["path"]))
    df,date_col=prepare_df(df)
    return {"component":component_id,"name":item["name"],"rows":len(df),"date_column":date_col,"id_column":find_id_column(df),"numeric_columns":numeric_cols(df),"all_columns":[str(c) for c in df.columns if str(c)!="_date"]}

@app.get("/api/swat/{component_id}/timeseries")
def swat_timeseries(component_id: str, value_column: str=Query(...), temporal: str=Query("monthly"), control_start: str|None=None, control_end: str|None=None, unit_id: str|None=None):
    item=swat_by_id(component_id)
    df=read_csv(data_path(item["path"]))
    df,date_col=prepare_df(df)
    if value_column not in df.columns: raise HTTPException(400,f"Column {value_column} not found")
    idc=find_id_column(df)
    if unit_id and idc: df=df[df[idc].astype(str)==str(unit_id)]
    control=apply_dates(df,control_start,control_end)
    return {"component":component_id,"name":item["name"],"value_column":value_column,"date_column":date_col,"id_column":idc,"control_period":{"start":control_start,"end":control_end},"temporal":temporal,"statistics":control_stats(control,value_column),"series":aggregate(control,value_column,temporal)}

@app.get("/api/swat/{component_id}/prediction")
def swat_prediction(component_id: str, value_column: str=Query(...), control_start: str|None=None, control_end: str|None=None, prediction_start_year: int=Query(...,ge=1900,le=2200), prediction_end_year: int=Query(...,ge=1900,le=2200), scenario: str=Query("baseline"), unit_id: str|None=None):
    if prediction_end_year < prediction_start_year or prediction_end_year-prediction_start_year>9:
        raise HTTPException(400,"Choose a prediction period up to 10 years.")
    item=swat_by_id(component_id)
    df=read_csv(data_path(item["path"]))
    df,_=prepare_df(df)
    if value_column not in df.columns: raise HTTPException(400,f"Column {value_column} not found")
    idc=find_id_column(df)
    if unit_id and idc: df=df[df[idc].astype(str)==str(unit_id)]
    control=apply_dates(df,control_start,control_end)
    series=projection(control,value_column,prediction_start_year,prediction_end_year,scenario)
    vals=[x["predicted_value"] for x in series]
    return {"component":component_id,"name":item["name"],"value_column":value_column,"scenario":scenario,"control_period":{"start":control_start,"end":control_end},"prediction_period":{"start_year":prediction_start_year,"end_year":prediction_end_year},"method":"Monthly climatology scenario projection based on the selected control period. It is not an exact daily weather forecast.","summary":{"total":round(sum(vals),4),"mean_monthly":round(sum(vals)/len(vals),4) if vals else None},"series":series}

@app.get("/download/dataset/{dataset_id}")
def download_dataset(dataset_id: str):
    item=dataset_by_id(dataset_id); p=data_path(item["path"])
    if not p.exists(): raise HTTPException(404,f"File missing: {item['path']}")
    return FileResponse(str(p),filename=p.name,media_type="application/octet-stream")

@app.get("/download/swat/{component_id}")
def download_swat(component_id: str):
    item=swat_by_id(component_id); p=data_path(item["path"])
    if not p.exists(): raise HTTPException(404,f"File missing: {item['path']}")
    return FileResponse(str(p),filename=p.name,media_type="text/csv")

@app.get("/download/portal-package.zip")
def portal_package():
    mem=io.BytesIO()
    with zipfile.ZipFile(mem,"w",zipfile.ZIP_DEFLATED) as z:
        for folder in [VECTOR,RASTER,SWAT,META]:
            for f in folder.rglob("*"):
                if f.is_file(): z.write(f,f.relative_to(DATA))
    mem.seek(0)
    return StreamingResponse(mem,media_type="application/zip",headers={"Content-Disposition":'attachment; filename="limpopo_portal_data.zip"'})

@app.get("/health")
def health():
    return {"status":"ok"}

HTML = r"""
<!doctype html>
<html>
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Limpopo Basin Digital Twin and SWAT Portal</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
:root{--navy:#121b2c;--cyan:#46adc6;--line:#d7dde5;--muted:#687386;--bg:#f5f6f8}
*{box-sizing:border-box}body{margin:0;font-family:Arial,sans-serif;background:var(--bg);color:#161d29}
header{height:82px;background:var(--navy);color:#fff;display:flex;align-items:center;padding:0 24px;gap:16px}header h1{margin:0;font-size:24px}.head{flex:1}.head p{margin:5px 0 0;color:#b9c4d3;font-size:13px}.headbtn{background:transparent;color:#fff;border:1px solid #dce4ee;padding:9px 13px;border-radius:4px;font-weight:bold;cursor:pointer;text-decoration:none}
.app{display:grid;grid-template-columns:385px 1fr;min-height:calc(100vh - 82px)}aside{background:#f8f9fb;border-right:1px solid var(--line);overflow:auto}.sidehead{background:#fff;padding:18px;border-bottom:1px solid var(--line)}.tabhead{display:flex;gap:18px;font-size:16px;align-items:center}.tabhead button{font-size:16px;background:transparent;border:0;cursor:pointer}.tabhead .active{padding:10px 18px;border:1px solid #cad3df;border-radius:4px;background:#fff}.search{width:100%;margin-top:15px;border:0;background:#f4f5f7;padding:14px;font-size:16px}.catalog{padding:8px}.cat{border-bottom:1px solid var(--line)}.cathead{padding:14px 6px;display:flex;justify-content:space-between;font-weight:bold;cursor:pointer}.cathead.active{background:var(--cyan);color:#fff;padding:14px 10px;margin:0 -4px;box-shadow:inset 0 -3px 0 #aa67ff}.list{display:none;padding:4px 8px 10px}.list.show{display:block}.row{display:flex;gap:10px;padding:10px 4px;cursor:pointer;border-radius:4px}.row:hover{background:#eaedf1}.ico{width:17px;height:14px;border:1.5px solid #798493;border-radius:2px;flex:0 0 auto;margin-top:2px}.name{font-size:14px;font-weight:bold;line-height:1.25}.status{display:block;margin-top:3px;font-size:11px;color:var(--muted)}.ok{color:#14805a}
main{position:relative;overflow:hidden}#map{height:calc(100vh - 82px);width:100%}.info{display:none;position:absolute;z-index:700;right:18px;top:18px;width:400px;max-height:calc(100vh - 118px);overflow:auto;background:#fff;box-shadow:0 8px 25px rgba(0,0,0,.22);border-radius:5px}.info.show,.analysis.show{display:block}.infohead{background:var(--navy);color:#fff;padding:16px 18px;display:flex;justify-content:space-between;gap:10px}.infohead h2{margin:0;font-size:20px}.close{background:transparent;border:1px solid #dce4ee;color:#fff;border-radius:4px;padding:6px 10px;font-weight:bold;cursor:pointer}.infobody{padding:18px}.grid{display:grid;grid-template-columns:1fr 1fr;gap:9px;margin:12px 0}.met{background:#f4f6f8;border-radius:4px;padding:9px}.met small{display:block;font-size:10px;color:var(--muted);margin-bottom:3px}.met b{font-size:12px}.action{width:100%;margin-top:8px;padding:10px;border:0;border-radius:4px;background:#328ba5;color:#fff;font-weight:bold;cursor:pointer}.action.alt{background:#526178}.action.warn{background:#aa6b00}.note{font-size:12px;color:#596475;line-height:1.5}.analysis{display:none;position:absolute;z-index:650;left:18px;right:18px;bottom:18px;max-height:46vh;overflow:auto;background:#fff;box-shadow:0 8px 25px rgba(0,0,0,.22);border-radius:5px}.anhead{padding:13px 16px;border-bottom:1px solid var(--line);display:flex;justify-content:space-between;align-items:center}.anbody{padding:16px}.controls{display:grid;grid-template-columns:repeat(4,1fr);gap:10px}.controls label{display:block;font-size:11px;color:var(--muted);font-weight:bold;margin-bottom:3px}.controls input,.controls select{width:100%;padding:8px;border:1px solid var(--line);border-radius:4px}.legend{padding:10px 12px;background:#fff;border-radius:4px;box-shadow:0 2px 8px rgba(0,0,0,.16);font-size:12px}
@media(max-width:900px){.app{grid-template-columns:1fr}aside{display:none}.info{left:12px;right:12px;width:auto}.controls{grid-template-columns:repeat(2,1fr)}}
</style>
</head>
<body>
<header><div class="head"><h1>Limpopo Basin Digital Twin and SWAT Data Portal</h1><p>Actual basin boundaries, real sub-basins, point stations, raster catalogue, control-period outputs and up-to-10-year scenarios.</p></div><button class="headbtn" onclick="about()">Basin description</button><a class="headbtn" href="/download/portal-package.zip">Download portal package</a></header>
<div class="app"><aside><div class="sidehead"><div class="tabhead"><button class="active">Data</button><button onclick="mydata()">My Data</button></div><input id="search" class="search" placeholder="Search the catalogue" oninput="filter()"></div><div id="catalog" class="catalog"></div></aside><main><div id="map"></div>
<div id="info" class="info"><div class="infohead"><h2 id="ititle">Dataset</h2><button class="close" onclick="closeInfo()">Done</button></div><div id="ibody" class="infobody"></div></div>
<div id="analysis" class="analysis"><div class="anhead"><b id="atitle">SWAT analysis</b><button onclick="closeAnalysis()">Close</button></div><div id="abody" class="anbody"></div></div>
</main></div>
<script>
let map, catalogData, layers={}, raster=null, state={component:null};
const esc=s=>String(s??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#039;"}[c]));
function init(){map=L.map("map").setView([-23.7,30.1],6);let osm=L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",{attribution:"© OpenStreetMap contributors"}).addTo(map),topo=L.tileLayer("https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png",{attribution:"© OpenTopoMap contributors"});L.control.layers({"Standard":osm,"Topographic":topo}).addTo(map);let lg=L.control({position:"bottomleft"});lg.onAdd=()=>{let d=L.DomUtil.create("div","legend");d.innerHTML="<b>Actual GIS only</b><br>No placeholder basin or sub-basin rectangles are generated.";return d};lg.addTo(map)}
function ds(id){return catalogData.datasets.find(x=>x.id===id)}function sw(id){return catalogData.swat_outputs.find(x=>x.id===id)}
function catitems(cat){let x=catalogData.datasets.filter(d=>d.category===cat);if(cat==="SWAT Model Outputs")x=x.concat(catalogData.swat_outputs.map(s=>({...s,kind:"swat",category:cat,format:"CSV"})));return x}
function render(){let root=document.getElementById("catalog");root.innerHTML="";catalogData.categories.forEach((cat,i)=>{let wrap=document.createElement("div");wrap.className="cat";let h=document.createElement("div");h.className="cathead"+(i===0?" active":"");h.innerHTML=`<span>${esc(cat)}</span><span>⌃</span>`;let list=document.createElement("div");list.className="list"+(i===0?" show":"");h.onclick=()=>{list.classList.toggle("show");h.classList.toggle("active");h.lastChild.textContent=list.classList.contains("show")?"⌃":"⌄"};let items=catitems(cat);if(!items.length)list.innerHTML='<div class="note">No registered data.</div>';items.forEach(x=>{let r=document.createElement("div");r.className="row";r.dataset.search=(x.name+" "+x.description+" "+(x.source||"")).toLowerCase();let st=x.status||"Missing";r.innerHTML=`<span class="ico"></span><span><span class="name">${esc(x.name)}</span><span class="status ${st==="Available"?"ok":""}">${esc(st)} · ${esc(x.format||"CSV")}</span></span>`;r.onclick=()=>x.kind==="swat"?openSwat(x.id):openDs(x.id);list.appendChild(r)});wrap.append(h,list);root.appendChild(wrap)})}
function filter(){let q=document.getElementById("search").value.trim().toLowerCase();document.querySelectorAll(".row").forEach(r=>r.style.display=!q||r.dataset.search.includes(q)?"flex":"none")}
function closeInfo(){document.getElementById("info").classList.remove("show")}function closeAnalysis(){document.getElementById("analysis").classList.remove("show")}
function openPanel(title,html){document.getElementById("ititle").textContent=title;document.getElementById("ibody").innerHTML=html;document.getElementById("info").classList.add("show")}
async function about(){let d=await fetch("/api/basin-description").then(r=>r.json());openPanel(d.title||"Limpopo River Basin",`<h3>General discussion</h3><p>${esc(d.general_description||"")}</p><h3>Specific discussion</h3><p>${esc(d.specific_discussion||"")}</p><h3>Sections</h3><ul>${(d.sections||[]).map(x=>`<li>${esc(x)}</li>`).join("")}</ul>`)}
function mydata(){openPanel("Required actual files",`<p>This portal will not map false geometry. Upload valid GIS and model files.</p><h3>Vector files</h3><p><code>data/vector/limpopo_basin_boundary.geojson</code><br><code>data/vector/limpopo_subbasins_level4.geojson</code><br><code>data/vector/limpopo_subbasins_level6.geojson</code><br><code>data/vector/swat_subbasins.geojson</code><br><code>data/vector/limpopo_river_network.geojson</code><br><code>data/vector/limpopo_monitoring_stations.geojson</code></p><h3>SWAT CSV</h3><p>Use a date column, optional ID column such as subbasin_id/reach_id/hru_id/station_id, and numeric variables such as FLOW_OUTcms, ET_mm, WYLD_mm, SURQ_mm, SED_OUTtons or SOIL_WATER.</p>`)}
function openDs(id){let x=ds(id),img=x.preview_url?`<img src="${x.preview_url}" style="width:100%;margin:10px 0;border-radius:4px">`:"";openPanel(x.name,`<p>${esc(x.description)}</p>${img}<div class="grid"><div class="met"><small>Status</small><b>${esc(x.status)}</b></div><div class="met"><small>Format</small><b>${esc(x.format)}</b></div><div class="met"><small>Source</small><b>${esc(x.source)}</b></div><div class="met"><small>Period</small><b>${esc(x.period)}</b></div><div class="met"><small>Resolution</small><b>${esc(x.resolution)}</b></div><div class="met"><small>Type</small><b>${esc(x.kind)}</b></div></div>${x.status==="Available"?`<button class="action" onclick="window.open('/download/dataset/${id}','_blank')">Download ${esc(x.format)}</button>`:""}${x.kind==="vector"&&x.status==="Available"?`<button class="action alt" onclick="toggleVector('${id}')">Toggle map layer</button>`:""}${x.kind==="raster"&&x.preview_url?`<button class="action alt" onclick="toggleRaster('${id}')">Toggle raster preview</button>`:""}<p class="note">Raster previews require exact preview_bounds in metadata/catalogue.json. The portal does not use arbitrary bounds.</p>`)}
async function toggleVector(id){if(layers[id]){map.removeLayer(layers[id]);delete layers[id];return}try{let r=await fetch("/api/geojson/"+id);let d=await r.json();if(!r.ok)throw Error(d.detail||"Cannot load GeoJSON");let styles={basin_boundary:{color:"#111827",weight:4,fill:false},hydrobasins_l4:{color:"#2563eb",weight:2,fillColor:"#60a5fa",fillOpacity:.08},hydrobasins_l6:{color:"#0d9488",weight:1,fillColor:"#5eead4",fillOpacity:.06},swat_subbasins:{color:"#7c3aed",weight:1.5,fillColor:"#c4b5fd",fillOpacity:.08},swat_hrus:{color:"#c2410c",weight:.8,fillColor:"#fdba74",fillOpacity:.04},river_network:{color:"#0284c7",weight:2},reservoirs:{color:"#0369a1",weight:2,fillColor:"#38bdf8",fillOpacity:.5},stations:{color:"#111827",weight:2,fillColor:"#fff",fillOpacity:1,radius:7}};let ly=L.geoJSON(d,{style:()=>styles[id]||{color:"#334155",weight:2},pointToLayer:(f,ll)=>L.circleMarker(ll,styles[id]||{radius:6}),onEachFeature:(f,l)=>{let p=f.properties||{};l.bindPopup(Object.entries(p).slice(0,14).map(([k,v])=>`<b>${esc(k)}:</b> ${esc(v)}<br>`).join("")||"No attributes")}}).addTo(map);layers[id]=ly;if(ly.getBounds().isValid())map.fitBounds(ly.getBounds(),{padding:[22,22]})}catch(e){alert(e.message)}}
function toggleRaster(id){let x=ds(id);if(!x.preview_url)return;if(raster&&raster.id===id){map.removeLayer(raster.layer);raster=null;return}if(raster)map.removeLayer(raster.layer);if(!x.preview_bounds||x.preview_bounds.length!==2){alert("Add exact preview_bounds to metadata/catalogue.json: [[south,west],[north,east]].");return}let ly=L.imageOverlay(x.preview_url,x.preview_bounds,{opacity:.62}).addTo(map);raster={id:id,layer:ly};map.fitBounds(x.preview_bounds)}
async function openSwat(id){let x=sw(id);document.getElementById("analysis").classList.add("show");document.getElementById("atitle").textContent=x.name;document.getElementById("abody").innerHTML="Loading SWAT outputs...";try{let r=await fetch(`/api/swat/${id}/columns`),d=await r.json();if(!r.ok)throw Error(d.detail||"Unavailable");state.component=id;let opts=d.numeric_columns.map(c=>`<option value="${esc(c)}">${esc(c)}</option>`).join("");document.getElementById("abody").innerHTML=`<p>${esc(x.description)}</p><p class="note">Date column: <b>${esc(d.date_column||"not detected")}</b>. ID column: <b>${esc(d.id_column||"not detected")}</b>. Rows: <b>${d.rows}</b>.</p><div class="controls"><div><label>Variable</label><select id="var">${opts}</select></div><div><label>Temporal output</label><select id="temp"><option value="monthly">Monthly</option><option value="daily">Daily</option><option value="weekly">Weekly</option><option value="seasonal">Seasonal</option><option value="annual">Annual</option></select></div><div><label>Control start</label><input id="cs" type="date"></div><div><label>Control end</label><input id="ce" type="date"></div><div><label>Unit ID optional</label><input id="uid" placeholder="sub-basin / reach / HRU"></div><div><label>Scenario</label><select id="sc"><option value="baseline">Baseline</option><option value="dry">Dry</option><option value="wet">Wet</option><option value="high_demand">High demand</option></select></div><div><label>Prediction start year</label><input id="ps" type="number" value="2027"></div><div><label>Prediction end year</label><input id="pe" type="number" value="2036"></div></div><button class="action" onclick="runAnalysis()">Control-period charts</button><button class="action alt" onclick="runProjection()">Up-to-10-year projection</button><button class="action warn" onclick="window.open('/download/swat/${id}','_blank')">Download SWAT CSV</button><div id="stats" class="note"></div><div id="chart" style="height:320px;margin-top:12px"></div>`}catch(e){document.getElementById("abody").innerHTML=`<p class="note">${esc(e.message)}</p>`}}
function qs(o){let p=new URLSearchParams();Object.entries(o).forEach(([k,v])=>{if(v!==undefined&&v!==null&&String(v).trim()!=="")p.append(k,v)});return p.toString()}
async function runAnalysis(){let o={value_column:var.value,temporal:temp.value,control_start:cs.value,control_end:ce.value,unit_id:uid.value},r=await fetch(`/api/swat/${state.component}/timeseries?${qs(o)}`),d=await r.json();if(!r.ok)return alert(d.detail);stats.innerHTML=`<b>Control-period statistics:</b> count ${d.statistics.count}; mean ${d.statistics.mean}; min ${d.statistics.min}; max ${d.statistics.max}; standard deviation ${d.statistics.std}.`;Plotly.newPlot("chart",[{x:d.series.map(x=>x.period),y:d.series.map(x=>x.value),type:"scatter",mode:"lines",name:d.value_column}],{title:`${d.name}: ${d.value_column} (${d.temporal})`,margin:{l:60,r:20,t:50,b:70},xaxis:{title:"Period"},yaxis:{title:d.value_column}},{responsive:true})}
async function runProjection(){let a=Number(ps.value),b=Number(pe.value);if(b<a||b-a>9)return alert("Choose a period up to 10 years.");let o={value_column:var.value,control_start:cs.value,control_end:ce.value,unit_id:uid.value,prediction_start_year:a,prediction_end_year:b,scenario:sc.value},r=await fetch(`/api/swat/${state.component}/prediction?${qs(o)}`),d=await r.json();if(!r.ok)return alert(d.detail);stats.innerHTML=`<b>Scenario projection:</b> ${esc(d.method)}<br>Total: <b>${d.summary.total}</b>; mean monthly: <b>${d.summary.mean_monthly}</b>.`;Plotly.newPlot("chart",[{x:d.series.map(x=>x.period),y:d.series.map(x=>x.predicted_value),type:"scatter",mode:"lines",fill:"tozeroy",name:"Projected"}],{title:`${d.name}: ${d.value_column} ${d.scenario} scenario`,margin:{l:60,r:20,t:50,b:70},xaxis:{title:"Month"},yaxis:{title:d.value_column}},{responsive:true})}
async function boot(){init();catalogData=await fetch("/api/catalogue").then(r=>r.json());render();let b=catalogData.datasets.find(x=>x.id==="basin_boundary"&&x.status==="Available");if(b)toggleVector("basin_boundary")}
document.addEventListener("DOMContentLoaded",boot);
</script></body></html>
"""

@app.get("/", response_class=HTMLResponse)
def home():
    return HTMLResponse(HTML)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
