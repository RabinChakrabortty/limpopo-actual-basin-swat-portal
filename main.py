from __future__ import annotations

import io
import json
import zipfile
from datetime import datetime
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

BASE = Path(__file__).resolve().parent
DATA = BASE / "data"
VECTOR = DATA / "vector"
RASTER = DATA / "raster"
SWAT = DATA / "swat"
META = DATA / "metadata"
for d in (VECTOR, RASTER, SWAT, META):
    d.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="Limpopo Basin Digital Twin and SWAT Data Portal", version="4.0.0")
app.mount("/files", StaticFiles(directory=str(DATA)), name="files")

# IMPORTANT: no fake basin/sub-basin shapes are generated anywhere in this app.
# Only actual uploaded GeoJSON files are displayed.

CATALOGUE = {
    "title": "Limpopo Basin Digital Twin and SWAT Data Portal",
    "note": "Only actual uploaded GeoJSON and raster preview files are visualized. No artificial boxes are generated.",
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
        "Data Sources & Providers",
    ],
    "datasets": [
        {"id":"basin_boundary","name":"Actual Limpopo Basin Boundary","category":"Natural Basin Characteristics","kind":"vector","path":"vector/limpopo_basin_boundary.geojson","format":"GeoJSON","source":"HydroBASINS / HydroATLAS / SWAT delineation","description":"Actual outer watershed polygon. Upload a valid WGS84 GeoJSON boundary.","period":"Static","resolution":"Vector polygon"},
        {"id":"subbasins_l4","name":"HydroBASINS Level 4 Sub-basins","category":"Natural Basin Characteristics","kind":"vector","path":"vector/limpopo_subbasins_level4.geojson","format":"GeoJSON","source":"HydroBASINS Africa","description":"Major nested sub-basins for basin reporting.","period":"Static","resolution":"HydroBASINS level 4"},
        {"id":"subbasins_l6","name":"HydroBASINS Level 6 Sub-basins","category":"Natural Basin Characteristics","kind":"vector","path":"vector/limpopo_subbasins_level6.geojson","format":"GeoJSON","source":"HydroBASINS Africa","description":"Detailed operational sub-basins for analysis.","period":"Static","resolution":"HydroBASINS level 6"},
        {"id":"swat_subbasins","name":"SWAT Delineated Sub-basins","category":"SWAT Model Outputs","kind":"vector","path":"vector/swat_subbasins.geojson","format":"GeoJSON","source":"SWAT / SWAT+ project","description":"Model-derived watershed units from SWAT delineation.","period":"Static model geometry","resolution":"SWAT sub-basin"},
        {"id":"swat_hrus","name":"SWAT Hydrologic Response Units","category":"SWAT Model Outputs","kind":"vector","path":"vector/swat_hrus.geojson","format":"GeoJSON","source":"SWAT / SWAT+ project","description":"Land-use, soil and slope response units.","period":"Static model geometry","resolution":"SWAT HRU"},
        {"id":"rivers","name":"Limpopo River Network","category":"Water Resources","kind":"vector","path":"vector/limpopo_river_network.geojson","format":"GeoJSON","source":"HydroRIVERS / SWAT reaches","description":"Actual rivers and tributaries clipped to basin.","period":"Static","resolution":"Vector line"},
        {"id":"stations","name":"Monitoring Stations","category":"Flood Monitoring & Early Warning","kind":"vector","path":"vector/limpopo_monitoring_stations.geojson","format":"GeoJSON","source":"LIMCOM / national agencies / SWAT outlets","description":"Point rainfall, discharge, reservoir and water-quality monitoring stations.","period":"Station-specific","resolution":"Point"},
        {"id":"reservoirs","name":"Reservoirs and Dams","category":"Water Resources","kind":"vector","path":"vector/limpopo_reservoirs.geojson","format":"GeoJSON","source":"GRanD / national authorities / SWAT","description":"Actual reservoir and dam locations.","period":"Static + time series","resolution":"Point or polygon"},
        {"id":"rainfall","name":"Monthly Rainfall","category":"Drought Monitoring & Risk Assessment","kind":"raster","path":"raster/rainfall_monthly_latest.tif","preview_path":"raster/rainfall_monthly_latest_preview.png","format":"GeoTIFF","source":"CHIRPS / ERA5-Land","description":"Monthly rainfall raster clipped to actual basin.","period":"Monthly","resolution":"Dataset dependent"},
        {"id":"et0","name":"Potential Evapotranspiration (ET0)","category":"Drought Monitoring & Risk Assessment","kind":"raster","path":"raster/et0_monthly_latest.tif","preview_path":"raster/et0_monthly_latest_preview.png","format":"GeoTIFF","source":"ERA5-Land / FAO-56","description":"Potential evapotranspiration raster.","period":"Monthly","resolution":"Dataset dependent"},
        {"id":"water_balance","name":"Rainfall Minus ET0 Water Balance","category":"Drought Monitoring & Risk Assessment","kind":"raster","path":"raster/water_balance_latest.tif","preview_path":"raster/water_balance_latest_preview.png","format":"GeoTIFF","source":"Calculated P - ET0","description":"Climatic water balance raster.","period":"Monthly / seasonal / annual","resolution":"Dataset dependent"},
        {"id":"soil_moisture","name":"Soil Moisture","category":"Drought Monitoring & Risk Assessment","kind":"raster","path":"raster/soil_moisture_latest.tif","preview_path":"raster/soil_moisture_latest_preview.png","format":"GeoTIFF","source":"SMAP / ERA5-Land","description":"Surface or root-zone soil moisture raster.","period":"Daily to monthly","resolution":"Dataset dependent"},
        {"id":"ndvi","name":"Vegetation & Crop Stress (NDVI)","category":"Drought Monitoring & Risk Assessment","kind":"raster","path":"raster/ndvi_latest.tif","preview_path":"raster/ndvi_latest_preview.png","format":"GeoTIFF","source":"MODIS / Sentinel-2","description":"Vegetation condition and crop stress raster.","period":"8-day to monthly","resolution":"10-250 m"},
        {"id":"lulc","name":"Land Use / Land Cover","category":"Natural Basin Characteristics","kind":"raster","path":"raster/lulc_latest.tif","preview_path":"raster/lulc_latest_preview.png","format":"GeoTIFF","source":"ESA WorldCover / Copernicus","description":"Land-cover classes clipped to the actual basin.","period":"Latest annual","resolution":"10-100 m"},
        {"id":"population","name":"Population Density","category":"Socio-Economic Profile","kind":"raster","path":"raster/population_density_latest.tif","preview_path":"raster/population_density_latest_preview.png","format":"GeoTIFF","source":"WorldPop / GHSL","description":"Population density used for exposure assessment.","period":"Latest available year","resolution":"Dataset dependent"},
        {"id":"flood_hazard","name":"Flood Hazard","category":"Flood Monitoring & Early Warning","kind":"raster","path":"raster/flood_hazard_latest.tif","preview_path":"raster/flood_hazard_latest_preview.png","format":"GeoTIFF","source":"GloFAS / Sentinel-1 / hydraulic model / SWAT","description":"Flood hazard or inundation-risk raster.","period":"Event / seasonal / historical","resolution":"Dataset dependent"},
        {"id":"drought_risk","name":"Composite Drought Risk","category":"Drought Monitoring & Risk Assessment","kind":"raster","path":"raster/drought_risk_latest.tif","preview_path":"raster/drought_risk_latest_preview.png","format":"GeoTIFF","source":"Derived multi-indicator assessment","description":"Drought risk raster based on rainfall, ET0, soil moisture, NDVI and exposure.","period":"Monthly / seasonal","resolution":"Dataset dependent"},
    ],
    "swat_outputs": [
        {"id":"basin","name":"SWAT Basin Water Balance","path":"swat/basin_water_balance.csv","description":"Basin precipitation, ET, runoff, recharge, water yield and storage."},
        {"id":"subbasin","name":"SWAT Sub-basin Outputs","path":"swat/subbasin_outputs.csv","description":"Sub-basin runoff, water yield, ET, sediment and water-balance outputs."},
        {"id":"reach","name":"SWAT Reach Outputs","path":"swat/reach_outputs.csv","description":"Reach discharge, sediment, nutrient and environmental-flow outputs."},
        {"id":"hru","name":"SWAT HRU Outputs","path":"swat/hru_outputs.csv","description":"HRU ET, soil water, runoff, percolation and crop-water-use outputs."},
        {"id":"reservoir","name":"SWAT Reservoir Outputs","path":"swat/reservoir_outputs.csv","description":"Reservoir inflow, outflow, storage, evaporation and spill outputs."},
        {"id":"station","name":"Observed and Modelled Station Series","path":"swat/station_timeseries.csv","description":"Station rainfall, observed discharge and SWAT simulated discharge."},
    ]
}

def fpath(rel: str) -> Path:
    return DATA / rel

def status(item: dict) -> str:
    return "Available" if fpath(item["path"]).exists() else "Missing"

def dataset(dataset_id: str) -> dict:
    for item in CATALOGUE["datasets"]:
        if item["id"] == dataset_id:
            return item
    raise HTTPException(404, "Dataset not found")

def swat(component_id: str) -> dict:
    for item in CATALOGUE["swat_outputs"]:
        if item["id"] == component_id:
            return item
    raise HTTPException(404, "SWAT component not found")

def public_dataset(item: dict) -> dict:
    out = dict(item)
    out["status"] = status(item)
    out["download_url"] = "/download/dataset/" + item["id"]
    out["geojson_url"] = "/api/geojson/" + item["id"] if item["kind"] == "vector" else None
    p = item.get("preview_path")
    out["preview_url"] = "/files/" + p if p and fpath(p).exists() else None
    return out

def public_swat(item: dict) -> dict:
    out = dict(item)
    out["status"] = status(item)
    out["download_url"] = "/download/swat/" + item["id"]
    return out

def get_geojson(dataset_id: str) -> dict:
    item = dataset(dataset_id)
    if item["kind"] != "vector":
        raise HTTPException(400, "Not a vector dataset")
    p = fpath(item["path"])
    if not p.exists():
        raise HTTPException(404, f"Actual GeoJSON file is missing: {item['path']}")
    try:
        value = json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        raise HTTPException(400, f"Invalid GeoJSON: {e}")
    if value.get("type") not in ["FeatureCollection", "Feature"]:
        raise HTTPException(400, "Invalid GeoJSON object")
    return value

def read_swat_csv(component_id: str) -> pd.DataFrame:
    item = swat(component_id)
    p = fpath(item["path"])
    if not p.exists():
        raise HTTPException(404, f"SWAT file missing: {item['path']}")
    for kw in [{"sep":","},{"sep":";"},{"sep":"\t"},{"sep":r"\s+","engine":"python"}]:
        try:
            df = pd.read_csv(p, **kw)
            if len(df.columns) >= 2:
                return df
        except Exception:
            pass
    raise HTTPException(400, "Cannot read CSV. Use comma-separated columns.")

def date_col(df: pd.DataFrame) -> str | None:
    for c in df.columns:
        if str(c).lower() in ["date","datetime","time","timestamp","day"] or "date" in str(c).lower():
            return str(c)
    return None

def id_col(df: pd.DataFrame) -> str | None:
    wanted = ["subbasin_id","subbasin","sub","reach_id","reach","hru_id","hru","station_id","station","id"]
    lower = {str(c).lower():str(c) for c in df.columns}
    for c in wanted:
        if c in lower:
            return lower[c]
    return None

def numeric_cols(df: pd.DataFrame) -> list[str]:
    return [str(c) for c in df.columns if pd.to_numeric(df[c], errors="coerce").notna().sum() > 0]

def prep(df: pd.DataFrame) -> tuple[pd.DataFrame, str | None]:
    dcol = date_col(df)
    out = df.copy()
    if dcol:
        d = pd.to_datetime(out[dcol], errors="coerce")
        out = out.loc[d.notna()].copy()
        out["_date"] = d.loc[d.notna()]
    return out, dcol

def filter_dates(df, start, end):
    if "_date" not in df.columns:
        return df.copy()
    out = df.copy()
    if start:
        out = out[out["_date"] >= pd.Timestamp(start)]
    if end:
        out = out[out["_date"] <= pd.Timestamp(end)]
    return out

def timeseries(df, col, temporal):
    if "_date" not in df.columns:
        raise HTTPException(400, "The CSV needs a date column for time-series charts.")
    x = df[["_date",col]].copy()
    x[col] = pd.to_numeric(x[col], errors="coerce")
    x = x.dropna().sort_values("_date")
    if x.empty:
        return []
    if temporal == "daily":
        g = x.groupby(x["_date"].dt.date)[col].mean(); labels=[str(v) for v in g.index]
    elif temporal == "weekly":
        g = x.groupby(x["_date"].dt.to_period("W"))[col].mean(); labels=[str(v) for v in g.index]
    elif temporal == "monthly":
        g = x.groupby(x["_date"].dt.to_period("M"))[col].mean(); labels=[str(v) for v in g.index]
    elif temporal == "seasonal":
        ss=x["_date"].dt.month.map(lambda m:"DJF" if m in [12,1,2] else "MAM" if m in [3,4,5] else "JJA" if m in [6,7,8] else "SON")
        g=x.groupby(x["_date"].dt.year.astype(str)+"-"+ss)[col].mean(); labels=[str(v) for v in g.index]
    elif temporal == "annual":
        g=x.groupby(x["_date"].dt.year)[col].mean(); labels=[str(v) for v in g.index]
    else:
        raise HTTPException(400, "Invalid temporal resolution")
    return [{"period":k, "value":round(float(v),4)} for k,v in zip(labels,g.tolist())]

def scenario_projection(df, col, start_year, end_year, scenario):
    if end_year < start_year or end_year-start_year > 9:
        raise HTTPException(400, "Select a prediction range up to 10 years.")
    if "_date" not in df.columns:
        raise HTTPException(400, "Prediction requires a date column.")
    x=df[["_date",col]].copy()
    x[col]=pd.to_numeric(x[col],errors="coerce")
    x=x.dropna()
    if x.empty:
        raise HTTPException(400, "No values remain in the selected control period.")
    clim=x.assign(_month=x["_date"].dt.month).groupby("_month")[col].mean()
    factor={"dry":.8,"baseline":1.0,"wet":1.2,"high_demand":.9}.get(scenario,1)
    rows=[]
    for year in range(start_year,end_year+1):
        for month in range(1,13):
            rows.append({"period":f"{year}-{month:02d}","predicted_value":round(float(clim.get(month,x[col].mean()))*factor,4)})
    return rows

@app.get("/api/catalogue")
def api_catalogue():
    return {"title":CATALOGUE["title"],"note":CATALOGUE["note"],"categories":CATALOGUE["categories"],"datasets":[public_dataset(x) for x in CATALOGUE["datasets"]],"swat_outputs":[public_swat(x) for x in CATALOGUE["swat_outputs"]]}

@app.get("/api/geojson/{dataset_id}")
def api_geojson(dataset_id: str):
    return JSONResponse(get_geojson(dataset_id))

@app.get("/api/basin-description")
def basin_description():
    p=META/"basin_description.json"
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"title":"Limpopo River Basin","general":"This portal supports actual basin and sub-basin geometries, point stations, raster data catalogues and SWAT output analysis.","specific":"The dashboard does not generate artificial basin geometry. A 1-10 year chart is a monthly hydroclimate scenario projection based on the selected control period; it is not an exact daily weather forecast."}

@app.get("/api/swat/{component_id}/columns")
def api_swat_columns(component_id: str):
    df,dcol=prep(read_swat_csv(component_id))
    return {"name":swat(component_id)["name"],"rows":int(len(df)),"date_column":dcol,"id_column":id_col(df),"numeric_columns":numeric_cols(df)}

@app.get("/api/swat/{component_id}/timeseries")
def api_swat_timeseries(component_id: str,value_column:str=Query(...),temporal:str="monthly",control_start:str|None=None,control_end:str|None=None,unit_id:str|None=None):
    df,dcol=prep(read_swat_csv(component_id))
    if value_column not in df.columns: raise HTTPException(400,"Column not found")
    ident=id_col(df)
    if unit_id and ident: df=df[df[ident].astype(str)==str(unit_id)]
    df=filter_dates(df,control_start,control_end)
    values=pd.to_numeric(df[value_column],errors="coerce").dropna()
    return {"name":swat(component_id)["name"],"value_column":value_column,"series":timeseries(df,value_column,temporal),"stats":{"count":int(values.count()),"mean":round(float(values.mean()),4) if len(values) else None,"min":round(float(values.min()),4) if len(values) else None,"max":round(float(values.max()),4) if len(values) else None}}

@app.get("/api/swat/{component_id}/prediction")
def api_swat_prediction(component_id: str,value_column:str=Query(...),control_start:str|None=None,control_end:str|None=None,prediction_start_year:int=Query(...),prediction_end_year:int=Query(...),scenario:str="baseline",unit_id:str|None=None):
    df,_=prep(read_swat_csv(component_id))
    if value_column not in df.columns: raise HTTPException(400,"Column not found")
    ident=id_col(df)
    if unit_id and ident: df=df[df[ident].astype(str)==str(unit_id)]
    df=filter_dates(df,control_start,control_end)
    rows=scenario_projection(df,value_column,prediction_start_year,prediction_end_year,scenario)
    return {"name":swat(component_id)["name"],"method":"Monthly climatology scenario projection from selected control-period data; not a deterministic daily forecast.","series":rows}

@app.get("/download/dataset/{dataset_id}")
def download_dataset(dataset_id: str):
    item=dataset(dataset_id); p=fpath(item["path"])
    if not p.exists(): raise HTTPException(404,"File missing")
    return FileResponse(str(p),filename=p.name,media_type="application/octet-stream")

@app.get("/download/swat/{component_id}")
def download_swat(component_id: str):
    item=swat(component_id); p=fpath(item["path"])
    if not p.exists(): raise HTTPException(404,"File missing")
    return FileResponse(str(p),filename=p.name,media_type="text/csv")

@app.get("/download/portal-package.zip")
def download_package():
    memory=io.BytesIO()
    with zipfile.ZipFile(memory,"w",zipfile.ZIP_DEFLATED) as z:
        for folder in (VECTOR,RASTER,SWAT,META):
            for p in folder.rglob("*"):
                if p.is_file(): z.write(p,p.relative_to(DATA))
    memory.seek(0)
    return StreamingResponse(memory,media_type="application/zip",headers={"Content-Disposition":'attachment; filename="limpopo_portal_data.zip"'})

@app.get("/health")
def health(): return {"status":"ok"}

HTML = r"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Limpopo Basin Digital Twin and SWAT Data Portal</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
*{box-sizing:border-box}body{margin:0;font-family:Arial,sans-serif;color:#18202c;background:#eef1f4}header{height:76px;background:#111a2b;color:white;display:flex;align-items:center;padding:0 20px;gap:12px}header h1{margin:0;font-size:22px}header p{margin:4px 0 0;color:#b8c5d4;font-size:12px}.head{flex:1}.btn{border:1px solid #d5dde8;background:#111a2b;color:white;padding:8px 12px;border-radius:4px;font-weight:bold;cursor:pointer;text-decoration:none}.app{display:grid;grid-template-columns:310px 1fr;height:calc(100vh - 76px)}aside{background:#f6f7f9;border-right:1px solid #dce2e8;overflow:auto}.tabs{padding:14px;background:white;border-bottom:1px solid #dce2e8}.tabs button{border:0;background:transparent;padding:8px 14px;font-size:15px;cursor:pointer}.tabs button:first-child{border:1px solid #cfd7e1;border-radius:4px}.search{margin-top:12px;width:100%;padding:11px;border:0;background:#f1f3f6;font-size:14px}.cat{border-bottom:1px solid #d8dee6}.cathead{padding:13px 12px;font-weight:bold;display:flex;justify-content:space-between;cursor:pointer}.cathead.active{background:#49aac3;color:white;box-shadow:inset 0 -3px #b765f5}.list{display:none;padding:5px 8px 10px}.list.show{display:block}.item{padding:9px 5px;display:flex;gap:9px;cursor:pointer}.item:hover{background:#e8edf1}.icon{width:16px;height:13px;border:1px solid #7a8492;border-radius:2px;margin-top:2px}.iname{font-weight:bold;font-size:13px}.istat{font-size:10px;color:#6c7583;margin-top:3px}.ok{color:#148756}main{position:relative}#map{width:100%;height:100%}.panel{position:absolute;z-index:1000;top:16px;right:16px;width:370px;max-height:calc(100% - 32px);overflow:auto;background:white;box-shadow:0 7px 24px #0004;display:none}.panel.show{display:block}.phead{background:#111a2b;color:white;padding:14px;display:flex;justify-content:space-between}.phead h2{font-size:18px;margin:0}.close{border:1px solid #d5dde8;background:none;color:white;padding:5px 9px;cursor:pointer}.pbody{padding:15px;font-size:13px;line-height:1.45}.meta{display:grid;grid-template-columns:1fr 1fr;gap:8px}.box{background:#f3f5f7;padding:9px}.box small{display:block;color:#697386}.action{width:100%;padding:10px;border:0;background:#2d90aa;color:white;font-weight:bold;margin-top:8px;cursor:pointer}.secondary{background:#526179}.bottom{position:absolute;z-index:950;left:16px;right:16px;bottom:16px;background:white;box-shadow:0 7px 24px #0004;max-height:43%;overflow:auto;display:none}.bottom.show{display:block}.bhead{padding:12px;border-bottom:1px solid #dce2e8;display:flex;justify-content:space-between}.bbody{padding:12px}.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:9px}.grid label{font-size:11px;color:#697386;font-weight:bold}.grid input,.grid select{width:100%;padding:7px;border:1px solid #dce2e8}.notice{background:white;padding:10px;border-radius:4px;box-shadow:0 2px 8px #0003;font-size:12px}@media(max-width:750px){.app{grid-template-columns:1fr}aside{display:none}.panel{left:12px;right:12px;width:auto}.grid{grid-template-columns:repeat(2,1fr)}}
</style></head>
<body>
<header><div class="head"><h1>Limpopo Basin Digital Twin and SWAT Data Portal</h1><p>Actual basin boundaries, valid sub-basins, station data, raster catalogue, control-period analysis and up-to-10-year scenarios.</p></div><button class="btn" onclick="about()">Basin description</button><a class="btn" href="/download/portal-package.zip">Download portal package</a></header>
<div class="app"><aside><div class="tabs"><button>Data</button><button onclick="myData()">My Data</button><input class="search" id="search" placeholder="Search the catalogue" oninput="filter()"></div><div id="catalogue"></div></aside><main><div id="map"></div><div id="panel" class="panel"><div class="phead"><h2 id="ptitle">Dataset</h2><button class="close" onclick="closePanel()">Done</button></div><div id="pbody" class="pbody"></div></div><div id="bottom" class="bottom"><div class="bhead"><b id="btitle">SWAT analysis</b><button onclick="closeBottom()">Close</button></div><div id="bbody" class="bbody"></div></div></main></div>
<script>
let MAP,DATA,LAYERS={},ACTIVE_RASTER=null,CURRENT_SWAT=null;
const esc=x=>String(x??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#039;"}[c]));
function init(){MAP=L.map("map").setView([-23.7,30.1],6);L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",{attribution:"© OpenStreetMap"}).addTo(MAP);let note=L.control({position:"bottomleft"});note.onAdd=()=>{let d=L.DomUtil.create("div","notice");d.innerHTML="<b>Actual-data mode</b><br>No fake basin/sub-basin boxes are created.";return d};note.addTo(MAP)}
function group(cat){let a=DATA.datasets.filter(x=>x.category===cat);if(cat==="SWAT Model Outputs")a=a.concat(DATA.swat_outputs.map(x=>({...x,kind:"swat",category:cat,format:"CSV"})));return a}
function render(){let root=document.getElementById("catalogue");root.innerHTML="";DATA.categories.forEach((cat,i)=>{let w=document.createElement("div");w.className="cat";let h=document.createElement("div");h.className="cathead"+(i===0?" active":"");h.innerHTML="<span>"+esc(cat)+"</span><span>⌃</span>";let l=document.createElement("div");l.className="list"+(i===0?" show":"");h.onclick=()=>{l.classList.toggle("show");h.classList.toggle("active");h.lastChild.textContent=l.classList.contains("show")?"⌃":"⌄"};group(cat).forEach(x=>{let r=document.createElement("div");r.className="item";r.dataset.s=(x.name+" "+x.description+" "+(x.source||"")).toLowerCase();r.innerHTML='<span class="icon"></span><span><div class="iname">'+esc(x.name)+'</div><div class="istat '+(x.status==="Available"?"ok":"")+'">'+esc(x.status)+" · "+esc(x.format||"CSV")+"</div></span>";r.onclick=()=>x.kind==="swat"?openSwat(x.id):openData(x.id);l.appendChild(r)});w.append(h,l);root.appendChild(w)})}
function filter(){let q=document.getElementById("search").value.toLowerCase();document.querySelectorAll(".item").forEach(x=>x.style.display=!q||x.dataset.s.includes(q)?"flex":"none")}
function show(){document.getElementById("panel").classList.add("show")}function closePanel(){document.getElementById("panel").classList.remove("show")}function closeBottom(){document.getElementById("bottom").classList.remove("show")}
function find(id){return DATA.datasets.find(x=>x.id===id)}function getSwat(id){return DATA.swat_outputs.find(x=>x.id===id)}
function openData(id){let x=find(id);document.getElementById("ptitle").textContent=x.name;let preview=x.preview_url?'<img src="'+x.preview_url+'" style="width:100%;margin:9px 0">':"";document.getElementById("pbody").innerHTML="<p>"+esc(x.description)+"</p>"+preview+'<div class="meta"><div class="box"><small>Status</small><b>'+esc(x.status)+'</b></div><div class="box"><small>Format</small><b>'+esc(x.format)+'</b></div><div class="box"><small>Source</small><b>'+esc(x.source)+'</b></div><div class="box"><small>Period</small><b>'+esc(x.period)+'</b></div><div class="box"><small>Resolution</small><b>'+esc(x.resolution)+'</b></div><div class="box"><small>Type</small><b>'+esc(x.kind)+'</b></div></div>' +(x.status==="Available"?'<button class="action" onclick="window.open(\\''+x.download_url+'\\')">Download '+esc(x.format)+'</button>':"")+(x.kind==="vector"&&x.status==="Available"?'<button class="action secondary" onclick="toggleVector(\\''+id+'\\')">Toggle map layer</button>':"")+'<p>Only actual GIS files are displayed. Upload valid HydroBASINS, HydroRIVERS or SWAT-exported GeoJSON.</p>';show()}
async function toggleVector(id){if(LAYERS[id]){MAP.removeLayer(LAYERS[id]);delete LAYERS[id];return}let r=await fetch("/api/geojson/"+id);let j=await r.json();if(!r.ok)return alert(j.detail||"Unable to load GeoJSON");let style={basin_boundary:{color:"#13223a",weight:4,fill:false},subbasins_l4:{color:"#2563eb",weight:2,fillOpacity:.05},subbasins_l6:{color:"#009688",weight:1,fillOpacity:.04},swat_subbasins:{color:"#7c3aed",weight:1.5,fillOpacity:.04},swat_hrus:{color:"#d97706",weight:.7,fillOpacity:.03},rivers:{color:"#0284c7",weight:2},stations:{color:"#111827",weight:2,radius:7,fillColor:"#fff",fillOpacity:1},reservoirs:{color:"#0369a1",weight:2,fillOpacity:.3}}[id]||{color:"#334155",weight:2};let layer=L.geoJSON(j,{style:()=>style,pointToLayer:(f,ll)=>L.circleMarker(ll,style),onEachFeature:(f,l)=>{let p=f.properties||{};l.bindPopup(Object.entries(p).slice(0,12).map(([k,v])=>"<b>"+esc(k)+":</b> "+esc(v)+"<br>").join("")||"No attributes")}}).addTo(MAP);LAYERS[id]=layer;if(layer.getBounds&&layer.getBounds().isValid())MAP.fitBounds(layer.getBounds(),{padding:[20,20]})}
function about(){fetch("/api/basin-description").then(r=>r.json()).then(x=>{document.getElementById("ptitle").textContent=x.title;document.getElementById("pbody").innerHTML="<h3>General discussion</h3><p>"+esc(x.general)+"</p><h3>Specific scientific discussion</h3><p>"+esc(x.specific)+"</p>";show()})}
function myData(){document.getElementById("ptitle").textContent="Upload actual data";document.getElementById("pbody").innerHTML="<p><b>Required:</b></p><code>data/vector/limpopo_basin_boundary.geojson</code><br><code>data/vector/limpopo_subbasins_level4.geojson</code><br><code>data/vector/limpopo_subbasins_level6.geojson</code><br><code>data/vector/swat_subbasins.geojson</code><br><code>data/vector/limpopo_monitoring_stations.geojson</code><br><code>data/swat/subbasin_outputs.csv</code><p>Use WGS84 GeoJSON. The portal will never draw manual rectangles.</p>";show()}
async function openSwat(id){CURRENT_SWAT=id;let x=getSwat(id);document.getElementById("bottom").classList.add("show");document.getElementById("btitle").textContent=x.name;document.getElementById("bbody").innerHTML="Loading SWAT variables...";let r=await fetch("/api/swat/"+id+"/columns"),d=await r.json();if(!r.ok){document.getElementById("bbody").innerHTML="<p>"+esc(d.detail||"SWAT file unavailable")+"</p>";return}let opts=d.numeric_columns.map(x=>'<option value="'+esc(x)+'">'+esc(x)+"</option>").join("");document.getElementById("bbody").innerHTML='<p>'+esc(x.description)+'</p><p>Date column: <b>'+esc(d.date_column||"not detected")+'</b>; Unit ID: <b>'+esc(d.id_column||"not detected")+'</b>; Rows: '+d.rows+'</p><div class="grid"><div><label>Variable</label><select id="var">'+opts+'</select></div><div><label>Temporal</label><select id="temporal"><option>monthly</option><option>daily</option><option>weekly</option><option>seasonal</option><option>annual</option></select></div><div><label>Control start</label><input id="cs" type="date"></div><div><label>Control end</label><input id="ce" type="date"></div><div><label>Unit ID optional</label><input id="unit"></div><div><label>Scenario</label><select id="scenario"><option value="baseline">Baseline</option><option value="dry">Dry</option><option value="wet">Wet</option><option value="high_demand">High demand</option></select></div><div><label>Prediction start</label><input id="ys" type="number" value="2027"></div><div><label>Prediction end</label><input id="ye" type="number" value="2036"></div></div><button class="action" onclick="runControl()">Create control-period chart</button><button class="action secondary" onclick="runProjection()">Create 10-year scenario chart</button><button class="action secondary" onclick="window.open(\\'/download/swat/'+id+'\\')">Download CSV</button><div id="stats"></div><div id="chart" style="height:310px;margin-top:10px"></div>'}
function qs(o){let q=new URLSearchParams();for(let k in o)if(o[k])q.append(k,o[k]);return q.toString()}
async function runControl(){let o={value_column:var.value,temporal:temporal.value,control_start:cs.value,control_end:ce.value,unit_id:unit.value};let r=await fetch("/api/swat/"+CURRENT_SWAT+"/timeseries?"+qs(o)),d=await r.json();if(!r.ok)return alert(d.detail);stats.innerHTML="<p><b>Control period:</b> mean "+d.stats.mean+"; min "+d.stats.min+"; max "+d.stats.max+"; records "+d.stats.count+"</p>";Plotly.newPlot("chart",[{x:d.series.map(x=>x.period),y:d.series.map(x=>x.value),type:"scatter",mode:"lines",name:d.value_column}],{title:d.name+" — "+d.value_column,margin:{l:60,r:20,t:45,b:60},xaxis:{title:"Period"},yaxis:{title:d.value_column}},{responsive:true})}
async function runProjection(){let ys=Number(document.getElementById("ys").value),ye=Number(document.getElementById("ye").value);if(ye<ys||ye-ys>9)return alert("Select up to 10 years, for example 2027–2036.");let o={value_column:var.value,control_start:cs.value,control_end:ce.value,unit_id:unit.value,prediction_start_year:ys,prediction_end_year:ye,scenario:scenario.value};let r=await fetch("/api/swat/"+CURRENT_SWAT+"/prediction?"+qs(o)),d=await r.json();if(!r.ok)return alert(d.detail);stats.innerHTML="<p><b>Method:</b> "+esc(d.method)+"</p>";Plotly.newPlot("chart",[{x:d.series.map(x=>x.period),y:d.series.map(x=>x.predicted_value),type:"scatter",mode:"lines",fill:"tozeroy",name:"Projection"}],{title:d.name+" scenario projection",margin:{l:60,r:20,t:45,b:60},xaxis:{title:"Month"},yaxis:{title:var.value}},{responsive:true})}
async function boot(){init();let r=await fetch("/api/catalogue");DATA=await r.json();render();let b=DATA.datasets.find(x=>x.id==="basin_boundary"&&x.status==="Available");if(b)toggleVector("basin_boundary")}
boot();
</script></body></html>"""

@app.get("/", response_class=HTMLResponse)
def home():
    return HTMLResponse(HTML)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
