from __future__ import annotations

import asyncio, csv, io, json, math, os, time
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

APP_TITLE = 'Limpopo Hybrid Digital Twin Portal'
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / 'data'
VECTOR_DIR = DATA_DIR / 'vector'
UPLOAD_DIR = DATA_DIR / 'uploads'
SWAT_DIR = DATA_DIR / 'swat'
for p in (VECTOR_DIR, UPLOAD_DIR, SWAT_DIR): p.mkdir(parents=True, exist_ok=True)

app = FastAPI(title=APP_TITLE, version='5.0.0')
app.mount('/files', StaticFiles(directory=str(DATA_DIR)), name='files')
CACHE: dict[str, tuple[float, Any]] = {}

NODES = {
 'upper_limpopo': {'name':'Upper Limpopo Analysis Node','lat':-25.20,'lon':26.90,'country':'South Africa / Botswana'},
 'gaborone': {'name':'Gaborone Catchment Analysis Node','lat':-24.65,'lon':25.91,'country':'Botswana'},
 'shashe': {'name':'Shashe Tributary Analysis Node','lat':-21.17,'lon':27.51,'country':'Botswana / Zimbabwe'},
 'polokwane': {'name':'Polokwane / Mogalakwena Analysis Node','lat':-23.90,'lon':29.45,'country':'South Africa'},
 'beitbridge': {'name':'Beitbridge Main-stem Analysis Node','lat':-22.22,'lon':30.00,'country':'Zimbabwe / South Africa'},
 'olifants': {'name':'Olifants Analysis Node','lat':-24.00,'lon':31.50,'country':'South Africa / Mozambique'},
 'massingir': {'name':'Massingir Analysis Node','lat':-23.88,'lon':32.16,'country':'Mozambique'},
 'xai_xai': {'name':'Xai-Xai Basin Outlet Analysis Node','lat':-25.05,'lon':33.65,'country':'Mozambique'},
}

GEOMETRIES = {
 'basin_boundary': ('Actual Limpopo Basin Boundary','limpopo_basin_boundary.geojson','BASIN_BOUNDARY_GEOJSON_URL','HydroBASINS / HydroATLAS / LIMCOM / SWAT delineation'),
 'subbasins_level4': ('HydroBASINS Level 4 Sub-basins','limpopo_subbasins_level4.geojson','SUBBASINS_L4_GEOJSON_URL','HydroBASINS Africa clipped to Limpopo'),
 'subbasins_level6': ('HydroBASINS Level 6 Sub-basins','limpopo_subbasins_level6.geojson','SUBBASINS_L6_GEOJSON_URL','HydroBASINS Africa clipped to Limpopo'),
 'river_network': ('HydroRIVERS / SWAT River Network','limpopo_river_network.geojson','RIVER_NETWORK_GEOJSON_URL','HydroRIVERS or SWAT reach network'),
 'stations': ('Official Monitoring Stations','limpopo_monitoring_stations.geojson','STATIONS_GEOJSON_URL','LIMCOM / national agencies / verified project stations'),
 'swat_subbasins': ('SWAT Delineated Sub-basins','swat_subbasins.geojson','SWAT_SUBBASINS_GEOJSON_URL','SWAT/SWAT+ delineated geometry'),
}
CATALOGUE = [
 ('Live APIs',[('live','Open-Meteo Forecast'),('history','Open-Meteo Historical Climate'),('live','Open-Meteo Flood / GloFAS'),('live','Open-Meteo Air Quality'),('nasa','NASA POWER Climate')]),
 ('Official Geometry',[(f'geo:{k}',v[0]) for k,v in GEOMETRIES.items()]),
 ('Raster Processing',[('credential:gee','Google Earth Engine'),('credential:cds','Copernicus CDS / ERA5-Land'),('credential:copernicus','Copernicus Data Space / Sentinel'),('upload','Upload GeoTIFF / COG / NetCDF')]),
 ('SWAT and Observations',[('upload','Upload SWAT / SWAT+ Outputs'),('upload','Upload Station CSV / Excel'),('swat','View Uploaded SWAT Files')]),
 ('Analysis and Prediction',[('analysis','Control-period Anomaly Analysis'),('analysis','Up-to-10-year Scenario Projection'),('risk','Flood / Drought Screening Risk')]),
 ('Downloads and Metadata',[('download','Download Live API Summary'),('uploads','Uploaded Dataset Register'),('methods','Methods and Data Sources')]),
]

def nums(v): return [float(x) for x in (v or []) if isinstance(x,(int,float)) and not isinstance(x,bool) and not math.isnan(float(x))]
def avg(v):
 a=nums(v); return round(sum(a)/len(a),3) if a else 0.0
def tot(v): return round(sum(nums(v)),3)
def mx(v):
 a=nums(v); return round(max(a),3) if a else 0.0
def risk(s): return 'Very high' if s>=80 else 'High' if s>=60 else 'Moderate' if s>=35 else 'Low'

async def get_json(client,url,params,ttl=7200):
 key=url+'?'+ '&'.join(f'{k}={params[k]}' for k in sorted(params)); now=time.time()
 if key in CACHE and now-CACHE[key][0]<ttl: return CACHE[key][1]
 r=await client.get(url,params=params,timeout=90); r.raise_for_status(); data=r.json(); CACHE[key]=(now,data); return data

def statuses():
 return [
  {'module':'Open-Meteo Forecast','status':'Active','need':'No key'}, {'module':'Open-Meteo Historical','status':'Active','need':'No key'},
  {'module':'Open-Meteo Flood','status':'Active','need':'No key'}, {'module':'Open-Meteo Air Quality','status':'Active','need':'No key'},
  {'module':'NASA POWER','status':'Active','need':'No key'},
  {'module':'Google Earth Engine','status':'Configured' if os.getenv('GEE_PROJECT_ID') else 'Credential required','need':'GEE project/service account'},
  {'module':'Copernicus CDS','status':'Configured' if os.getenv('CDS_API_KEY') else 'Credential required','need':'CDS_API_KEY'},
  {'module':'Copernicus Data Space','status':'Configured' if os.getenv('COPERNICUS_CLIENT_ID') else 'Credential required','need':'Client ID and secret'},
  {'module':'SWAT / SWAT+','status':'Upload required','need':'Actual calibrated model output'},
 ]

def file_index():
 out=[]
 for folder,group in ((VECTOR_DIR,'Vector'),(UPLOAD_DIR,'Upload'),(SWAT_DIR,'SWAT')):
  for f in folder.rglob('*'):
   if f.is_file(): out.append({'name':f.name,'group':group,'path':str(f.relative_to(DATA_DIR)).replace('\\','/'),'size_kb':round(f.stat().st_size/1024,2),'url':'/files/'+str(f.relative_to(DATA_DIR)).replace('\\','/')})
 return out

async def geometry(ident):
 if ident not in GEOMETRIES: raise HTTPException(404,'Unknown geometry.')
 name,filename,env,source=GEOMETRIES[ident]; f=VECTOR_DIR/filename
 if f.exists():
  try:
   g=json.loads(f.read_text());
   if g.get('type') not in ('Feature','FeatureCollection'): raise ValueError('Invalid GeoJSON')
   return {'available':True,'origin':'Local GeoJSON','name':name,'source':source,'geojson':g}
  except Exception as e: return {'available':False,'name':name,'source':source,'reason':f'Invalid local GeoJSON: {e}'}
 url=os.getenv(env,'').strip()
 if url:
  try:
   async with httpx.AsyncClient(headers={'User-Agent':'LimpopoHybrid/5.0'}) as c: g=await get_json(c,url,{},86400)
   if g.get('type') not in ('Feature','FeatureCollection'): raise ValueError('Configured URL did not return GeoJSON')
   return {'available':True,'origin':'Official online GeoJSON','name':name,'source':source,'geojson':g}
  except Exception as e: return {'available':False,'name':name,'source':source,'reason':f'Cannot load URL: {e}'}
 return {'available':False,'name':name,'source':source,'reason':f'Upload data/vector/{filename} or configure Render variable {env} with a direct GeoJSON URL.'}

async def node_live(node_id,days=16,flood_days=30):
 if node_id not in NODES: raise HTTPException(404,'Unknown analysis node.')
 n=NODES[node_id]
 fp={'latitude':n['lat'],'longitude':n['lon'],'daily':'precipitation_sum,temperature_2m_max,temperature_2m_min,et0_fao_evapotranspiration,wind_speed_10m_max,relative_humidity_2m_mean','forecast_days':days,'timezone':'auto'}
 flp={'latitude':n['lat'],'longitude':n['lon'],'daily':'river_discharge','forecast_days':flood_days,'timezone':'auto'}
 ap={'latitude':n['lat'],'longitude':n['lon'],'hourly':'pm10,pm2_5,dust,uv_index','forecast_days':min(days,7),'timezone':'auto'}
 async with httpx.AsyncClient(headers={'User-Agent':'LimpopoHybrid/5.0'}) as c:
  f,fl,a=await asyncio.gather(get_json(c,'https://api.open-meteo.com/v1/forecast',fp),get_json(c,'https://flood-api.open-meteo.com/v1/flood',flp),get_json(c,'https://air-quality-api.open-meteo.com/v1/air-quality',ap),return_exceptions=True)
 d=f.get('daily',{}) if isinstance(f,dict) else {}; fd=fl.get('daily',{}) if isinstance(fl,dict) else {}; ah=a.get('hourly',{}) if isinstance(a,dict) else {}
 rain=d.get('precipitation_sum',[]) or []; et=d.get('et0_fao_evapotranspiration',[]) or []; tmax=d.get('temperature_2m_max',[]) or []; tmin=d.get('temperature_2m_min',[]) or []; q=fd.get('river_discharge',[]) or []
 r=tot(rain); e=tot(et); bal=round(r-e,2); peak=mx(q); ds=0
 if bal<-45: ds+=52
 elif bal<-20: ds+=32
 elif bal<0: ds+=16
 if mx(tmax)>=38: ds+=22
 if r<10: ds+=18
 ds=min(ds,100); fs=95 if peak>=120 else 75 if peak>=50 else 50 if peak>=20 else 20; comp=round(ds*.6+fs*.4,2)
 return {'node':{'id':node_id,**n},'forecast':{'rainfall_mm':r,'et0_mm':e,'water_balance_mm':bal,'mean_temp_c':round((avg(tmax)+avg(tmin))/2,2),'max_temp_c':mx(tmax),'max_wind_kmh':mx(d.get('wind_speed_10m_max',[])),'mean_humidity_pct':avg(d.get('relative_humidity_2m_mean',[]))},'flood':{'peak_discharge_m3s':peak,'mean_discharge_m3s':avg(q),'score':fs,'class':risk(fs)},'drought':{'score':ds,'class':risk(ds)},'composite':{'score':comp,'class':risk(comp)},'air':{'pm2_5':avg(ah.get('pm2_5',[])),'pm10':avg(ah.get('pm10',[])),'dust':avg(ah.get('dust',[])),'uv_max':mx(ah.get('uv_index',[]))},'series':{'forecast_dates':d.get('time',[]),'rainfall':rain,'et0':et,'tmax':tmax,'tmin':tmin,'flood_dates':fd.get('time',[]),'discharge':q},'note':'Analysis nodes are not official gauges unless replaced by verified uploaded station data.'}

async def historic(node_id,start,end):
 if node_id not in NODES: raise HTTPException(404,'Unknown analysis node.')
 try:
  if date.fromisoformat(end)<=date.fromisoformat(start): raise ValueError
 except ValueError: raise HTTPException(400,'Use valid YYYY-MM-DD dates with end later than start.')
 n=NODES[node_id]; p={'latitude':n['lat'],'longitude':n['lon'],'start_date':start,'end_date':end,'daily':'precipitation_sum,temperature_2m_mean,et0_fao_evapotranspiration','timezone':'auto'}
 async with httpx.AsyncClient(headers={'User-Agent':'LimpopoHybrid/5.0'}) as c: raw=await get_json(c,'https://archive-api.open-meteo.com/v1/archive',p,86400)
 d=raw.get('daily',{}); m=defaultdict(lambda:{'rain':[],'temp':[],'et':[]}); dates=d.get('time',[]); rs=d.get('precipitation_sum',[]); ts=d.get('temperature_2m_mean',[]); es=d.get('et0_fao_evapotranspiration',[])
 for i,s in enumerate(dates):
  if i>=len(rs) or i>=len(ts) or i>=len(es): continue
  try: x=date.fromisoformat(s)
  except ValueError: continue
  k=f'{x.year}-{x.month:02d}'; m[k]['rain'].append(rs[i]);m[k]['temp'].append(ts[i]);m[k]['et'].append(es[i])
 out=[]
 for k in sorted(m):
  rr=tot(m[k]['rain']); ee=tot(m[k]['et']); out.append({'period':k,'rainfall_mm':rr,'temp_c':avg(m[k]['temp']),'et0_mm':ee,'water_balance_mm':round(rr-ee,2)})
 return out

@app.get('/',response_class=HTMLResponse)
def home(): return HTMLResponse(HTML)
@app.get('/health')
def health(): return {'status':'ok'}
@app.get('/api/catalogue')
def api_catalogue(): return {'catalogue':CATALOGUE,'modules':statuses()}
@app.get('/api/status')
def api_status(): return {'modules':statuses(),'files':file_index(),'warning':'Render local uploads can disappear on redeployment. Use cloud storage for production data.'}
@app.get('/api/geometry/{ident}')
async def api_geometry(ident:str): return await geometry(ident)
@app.get('/api/live/basin')
async def api_basin(forecast_days:int=Query(16,ge=1,le=16),flood_days:int=Query(30,ge=1,le=30)):
 res=await asyncio.gather(*(node_live(k,forecast_days,flood_days) for k in NODES),return_exceptions=True); nodes=[x for x in res if isinstance(x,dict)]
 if not nodes: raise HTTPException(502,'No live data returned.')
 return {'nodes':nodes,'summary':{'rainfall_mm':avg([x['forecast']['rainfall_mm'] for x in nodes]),'et0_mm':avg([x['forecast']['et0_mm'] for x in nodes]),'balance_mm':avg([x['forecast']['water_balance_mm'] for x in nodes]),'peak_discharge_m3s':avg([x['flood']['peak_discharge_m3s'] for x in nodes]),'risk':avg([x['composite']['score'] for x in nodes])}}
@app.get('/api/live/node/{node_id}')
async def api_node(node_id:str): return await node_live(node_id)
@app.get('/api/history/{node_id}')
async def api_history(node_id:str,start:str='1991-01-01',end:str='2020-12-31'): return {'node':{'id':node_id,**NODES[node_id]},'monthly':await historic(node_id,start,end)}
@app.get('/api/projection/{node_id}')
async def api_projection(node_id:str,control_start:str='1991-01-01',control_end:str='2020-12-31',start_year:int=2027,end_year:int=2036,scenario:str=Query('baseline',pattern='^(dry|baseline|wet|high_demand)$')):
 if end_year<start_year or end_year-start_year>9: raise HTTPException(400,'Maximum scenario period is 10 years.')
 h=await historic(node_id,control_start,control_end)
 if not h: raise HTTPException(400,'No historical control-period data.')
 c=defaultdict(lambda:{'rain':[],'temp':[],'et':[]})
 for row in h:
  mon=int(row['period'].split('-')[1]);c[mon]['rain'].append(row['rainfall_mm']);c[mon]['temp'].append(row['temp_c']);c[mon]['et'].append(row['et0_mm'])
 f={'dry':.8,'baseline':1,'wet':1.2,'high_demand':.9}[scenario];p=[]
 for y in range(start_year,end_year+1):
  for m in range(1,13):
   r=avg(c[m]['rain'])*f;e=avg(c[m]['et'])*(1.08 if scenario=='high_demand' else 1);p.append({'period':f'{y}-{m:02d}','rainfall_mm':round(r,2),'temp_c':avg(c[m]['temp']),'et0_mm':round(e,2),'water_balance_mm':round(r-e,2)})
 return {'node':{'id':node_id,**NODES[node_id]},'historical':h,'projection':p,'scenario':scenario,'method':'Monthly hydroclimate scenario from control-period climatology. It is not an exact daily weather forecast.'}
@app.get('/api/nasa/{node_id}')
async def api_nasa(node_id:str,start:str='2020-01-01',end:str='2020-12-31'):
 if node_id not in NODES: raise HTTPException(404,'Unknown node.')
 n=NODES[node_id];p={'parameters':'PRECTOTCORR,T2M,EVPTRNS,WS2M,ALLSKY_SFC_SW_DWN','community':'AG','longitude':n['lon'],'latitude':n['lat'],'start':start.replace('-',''),'end':end.replace('-',''),'format':'JSON'}
 async with httpx.AsyncClient(headers={'User-Agent':'LimpopoHybrid/5.0'}) as c: raw=await get_json(c,'https://power.larc.nasa.gov/api/temporal/daily/point',p,86400)
 return {'node':{'id':node_id,**n},'parameters':raw.get('properties',{}).get('parameter',{})}
@app.post('/api/upload')
async def api_upload(file:UploadFile=File(...),dataset_type:str=Form(...),source:str=Form('User upload'),description:str=Form('')):
 allowed={'.geojson','.json','.zip','.csv','.xlsx','.xls','.tif','.tiff','.nc','.txt','.sqlite','.db'}; name=Path(file.filename or 'upload').name
 if Path(name).suffix.lower() not in allowed: raise HTTPException(400,'Unsupported file format.')
 content=await file.read()
 if len(content)>100*1024*1024: raise HTTPException(413,'Maximum prototype upload is 100 MB.')
 kind=dataset_type.lower(); folder=VECTOR_DIR if kind in ('vector','geometry') and Path(name).suffix.lower() in ('.geojson','.json') else SWAT_DIR if kind in ('swat','station') else UPLOAD_DIR
 target=folder/f"{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{name}";target.write_bytes(content)
 return {'message':'Uploaded. Validate CRS, source, metadata and scientific suitability before analysis.','file':str(target.relative_to(DATA_DIR)).replace('\\','/'),'source':source,'description':description}
@app.get('/api/uploads')
def api_uploads(): return {'files':file_index()}
@app.get('/download/live-summary.csv')
async def download_csv():
 live=await api_basin(); rows=[]
 for x in live['nodes']: rows.append({'node_id':x['node']['id'],'node':x['node']['name'],'country':x['node']['country'],'rainfall_mm':x['forecast']['rainfall_mm'],'et0_mm':x['forecast']['et0_mm'],'water_balance_mm':x['forecast']['water_balance_mm'],'peak_discharge_m3s':x['flood']['peak_discharge_m3s'],'drought_risk':x['drought']['class'],'flood_risk':x['flood']['class'],'composite_risk':x['composite']['class']})
 s=io.StringIO();w=csv.DictWriter(s,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
 return StreamingResponse(iter([s.getvalue()]),media_type='text/csv',headers={'Content-Disposition':'attachment; filename="limpopo_live_summary.csv"'})
@app.get('/api/methods')
def api_methods(): return {'title':'Methods and source rules','api':'Live APIs are used first for monitoring. Uploaded official stations and calibrated SWAT outputs should take priority for formal scientific analysis.','geometry':'The portal never creates artificial basin polygons. Use HydroBASINS/HydroATLAS, LIMCOM/national authorities, or SWAT delineated geometry.','projection':'Ten-year outputs are monthly scenario projections based on control-period climatology; they are not deterministic daily forecasts.','storage':'Render local upload storage is temporary. Use cloud/object storage in production.'}

HTML = """<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>Limpopo Hybrid Digital Twin</title><script src='https://cdn.plot.ly/plotly-2.35.2.min.js'></script><link rel='stylesheet' href='https://unpkg.com/leaflet@1.9.4/dist/leaflet.css'><script src='https://unpkg.com/leaflet@1.9.4/dist/leaflet.js'></script><style>
:root{--n:#101b2d;--c:#42a9c6;--l:#dce2e9;--bg:#eef3f6;--t:#182130;--m:#687487}*{box-sizing:border-box}body{margin:0;font:14px Arial;background:var(--bg);color:var(--t)}header{height:78px;background:var(--n);color:#fff;padding:12px 18px;display:flex;gap:10px;align-items:center}.title{flex:1}h1{margin:0;font-size:20px}header p{margin:5px 0 0;font-size:11px;color:#c1ccda}.top,.btn{border:1px solid #dae3ed;border-radius:4px;padding:8px 10px;background:transparent;color:#fff;font-weight:bold;cursor:pointer;text-decoration:none}.app{display:grid;grid-template-columns:350px 1fr;height:calc(100vh - 78px)}.side{overflow:auto;background:#fafbfc;border-right:1px solid var(--l)}.sidehead{padding:13px;background:#fff;border-bottom:1px solid var(--l)}.tabs{display:flex;gap:7px;margin-bottom:10px}.tabs button{border:1px solid var(--l);padding:7px 9px;background:#fff;border-radius:4px;font-weight:bold;cursor:pointer}.tabs button:first-child{background:var(--n);color:#fff}.search{width:100%;border:0;background:#f1f3f6;padding:11px}.catalogue{padding:7px 4px}.group{border-bottom:1px solid var(--l)}.ghead{padding:13px 10px;font-weight:bold;display:flex;justify-content:space-between;cursor:pointer}.ghead.active{background:var(--c);color:white}.items{display:none;padding:5px 9px 10px}.items.show{display:block}.item{display:flex;gap:8px;padding:9px 3px;cursor:pointer;border-radius:4px}.item:hover{background:#e9eef3}.ico{width:15px;height:13px;border:1.5px solid #7c8796;border-radius:2px;margin-top:2px;flex:none}.item b{font-size:12px}.tag{display:block;color:var(--m);font-size:10px;margin-top:3px}.live{color:#078656}.work{position:relative}.map{height:100%;width:100%}.notice{position:absolute;z-index:600;top:14px;left:14px;background:#fff;padding:10px 12px;border-radius:4px;box-shadow:0 2px 12px #0002;max-width:500px;font-size:12px}.panel{display:none;position:absolute;z-index:800;right:16px;top:16px;width:410px;max-height:calc(100% - 32px);overflow:auto;background:#fff;border-radius:5px;box-shadow:0 8px 28px #0003}.panel.show,.bottom.show{display:block}.phead{background:var(--n);color:#fff;padding:13px 15px;display:flex;justify-content:space-between}.phead h2{margin:0;font-size:17px}.close{background:transparent;color:#fff;border:1px solid #dbe4ef;border-radius:3px;padding:5px 8px}.pbody{padding:15px;line-height:1.45}.grid{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin:9px 0}.card{background:#f3f5f7;padding:8px;border-radius:3px}.card small{display:block;font-size:10px;color:var(--m)}.btn{width:100%;background:#258eab;border:0;margin-top:8px}.gray{background:#536177}.bottom{display:none;position:absolute;z-index:800;left:16px;right:16px;bottom:16px;max-height:50%;overflow:auto;background:#fff;border-radius:5px;box-shadow:0 8px 28px #0003}.bhead{padding:12px 14px;border-bottom:1px solid var(--l);display:flex;justify-content:space-between}.bhead h3{margin:0}.bbody{padding:14px}.controls{display:grid;grid-template-columns:repeat(4,1fr);gap:8px}.controls label{display:block;font-size:10px;color:var(--m);font-weight:bold}.controls input,.controls select,.drop input,.drop select,.drop textarea{width:100%;padding:7px;border:1px solid var(--l);border-radius:3px}.drop{border:2px dashed #a6b2c1;padding:14px;border-radius:5px;background:#f8fafc}.note{font-size:12px;color:#4d5b6e;margin:9px 0}table{border-collapse:collapse;width:100%;font-size:11px}th,td{padding:7px;border-bottom:1px solid var(--l);text-align:left}th{background:#f2f5f7}.legend{background:#fff;padding:9px;border-radius:4px;box-shadow:0 2px 10px #0002;font-size:11px}@media(max-width:850px){.app{grid-template-columns:1fr}.side{display:none}.panel{left:10px;right:10px;width:auto}.controls{grid-template-columns:repeat(2,1fr)}}
</style></head><body><header><div class='title'><h1>Limpopo Hybrid Digital Twin Portal</h1><p>Live APIs first • actual boundaries only from official sources • upload where required • SWAT and scenario integration.</p></div><button class='top' onclick='methods()'>Methods</button><button class='top' onclick='upload()'>Upload data</button><a class='top' href='/download/live-summary.csv' target='_blank'>Live CSV</a></header><div class='app'><aside class='side'><div class='sidehead'><div class='tabs'><button>Data</button><button onclick='status()'>System status</button><button onclick='upload()'>Upload</button></div><input id='search' class='search' placeholder='Search components' oninput='filter()'></div><div id='catalogue' class='catalogue'></div></aside><main class='work'><div id='map' class='map'></div><div id='notice' class='notice'>Connecting to live APIs…</div><section id='panel' class='panel'><div class='phead'><h2 id='ptitle'>Information</h2><button class='close' onclick='closePanel()'>Done</button></div><div id='pbody' class='pbody'></div></section><section id='bottom' class='bottom'><div class='bhead'><h3 id='btitle'>Analysis</h3><button onclick='closeBottom()'>Close</button></div><div id='bbody' class='bbody'></div></section></main></div><script>
let map,catalogue,live,markers,layers={},active='upper_limpopo';const $=id=>document.getElementById(id);function e(v){return String(v??'').replace(/[&<>"']/g,x=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#039;'}[x]))}function col(s){return s>=80?'#c64044':s>=60?'#ed7b2b':s>=35?'#daa40d':'#078656'}function panel(t,h){$('ptitle').textContent=t;$('pbody').innerHTML=h;$('panel').classList.add('show')}function closePanel(){$('panel').classList.remove('show')}function bottom(t,h){$('btitle').textContent=t;$('bbody').innerHTML=h;$('bottom').classList.add('show')}function closeBottom(){$('bottom').classList.remove('show')}
function init(){map=L.map('map').setView([-23.7,30.1],6);let a=L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',{attribution:'© OpenStreetMap contributors'}).addTo(map),b=L.tileLayer('https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png',{attribution:'© OpenTopoMap'});L.control.layers({'Standard':a,'Topographic':b}).addTo(map);markers=L.layerGroup().addTo(map);let x=L.control({position:'bottomleft'});x.onAdd=()=>{let d=L.DomUtil.create('div','legend');d.innerHTML='<b>Rule:</b> live circles are API nodes.<br>Actual boundaries appear only from verified GeoJSON.<br>No fake polygons.';return d};x.addTo(map)}
async function start(){init();catalogue=await (await fetch('/api/catalogue')).json();render();await loadLive()}function render(){let root=$('catalogue');root.innerHTML='';catalogue.catalogue.forEach((g,i)=>{let w=document.createElement('div');w.className='group';let h=document.createElement('div');h.className='ghead'+(i===0?' active':'');h.innerHTML=`<span>${e(g[0])}</span><span>⌃</span>`;let ls=document.createElement('div');ls.className='items'+(i===0?' show':'');h.onclick=()=>{ls.classList.toggle('show');h.classList.toggle('active');h.lastChild.textContent=ls.classList.contains('show')?'⌃':'⌄'};g[1].forEach(z=>{let r=document.createElement('div');r.className='item';r.dataset.q=(z[1]+' '+g[0]).toLowerCase();let li=z[0]==='live'||z[0]==='nasa'||z[0]==='history';r.innerHTML=`<span class='ico'></span><span><b>${e(z[1])}</b><span class='tag ${li?'live':''}'>${li?'Direct API':'Select module'}</span></span>`;r.onclick=()=>click(z[0],z[1]);ls.appendChild(r)});w.append(h,ls);root.appendChild(w)})}function filter(){let q=$('search').value.trim().toLowerCase();document.querySelectorAll('.item').forEach(x=>x.style.display=!q||x.dataset.q.includes(q)?'flex':'none')}
async function loadLive(){try{let r=await fetch('/api/live/basin');live=await r.json();if(!r.ok)throw Error(live.detail);markers.clearLayers();live.nodes.forEach(n=>{let m=L.circleMarker([n.node.lat,n.node.lon],{radius:7+n.composite.score/14,color:'#142034',weight:2,fillColor:col(n.composite.score),fillOpacity:.87}).bindPopup(`<b>${e(n.node.name)}</b><br>${e(n.node.country)}<hr>Rainfall: ${n.forecast.rainfall_mm} mm<br>ET0: ${n.forecast.et0_mm} mm<br>Balance: ${n.forecast.water_balance_mm} mm<br>Peak discharge: ${n.flood.peak_discharge_m3s} m³/s<br>Risk: ${e(n.composite.class)}<br><button onclick="liveNode('${n.node.id}')">Charts</button> <button onclick="control('${n.node.id}')">Scenario</button>`).addTo(markers)});let b=markers.getBounds();if(b.isValid())map.fitBounds(b,{padding:[35,35]});let s=live.summary;$('notice').innerHTML=`<b>Live APIs active.</b> Mean rainfall ${s.rainfall_mm} mm; ET0 ${s.et0_mm} mm; balance ${s.balance_mm} mm; peak discharge ${s.peak_discharge_m3s} m³/s. <button onclick='risk()'>Risk overview</button>`}catch(err){$('notice').innerHTML=`<b>Live API error:</b> ${e(err.message)}. Refresh after the service wakes up.`}}
async function click(id,name){if(id.startsWith('geo:'))return geo(id.slice(4));if(id==='live')return liveNode(active);if(id==='history'||id==='analysis')return control(active);if(id==='nasa')return nasa(active);if(id==='risk')return risk();if(id==='upload'||id==='uploads')return upload();if(id==='download')return window.open('/download/live-summary.csv','_blank');if(id==='methods')return methods();if(id==='swat')return swat();if(id.startsWith('credential:'))return credential(id.slice(11))}
async function geo(id){if(layers[id]){map.removeLayer(layers[id]);delete layers[id];return}let r=await fetch(`/api/geometry/${id}`),d=await r.json();if(!d.available)return panel(d.name,`<p><b>Actual geometry is not configured.</b></p><p>${e(d.reason)}</p><p>Expected source: ${e(d.source)}</p>`);let st={basin_boundary:{color:'#111b2d',weight:4,fill:false},subbasins_level4:{color:'#2563eb',weight:2,fillColor:'#60a5fa',fillOpacity:.08},subbasins_level6:{color:'#0d9488',weight:1,fillColor:'#5eead4',fillOpacity:.06},river_network:{color:'#0284c7',weight:2},stations:{color:'#111827',fillColor:'#fff',radius:6,weight:2},swat_subbasins:{color:'#7c3aed',weight:1.2,fillColor:'#c4b5fd',fillOpacity:.08}}[id]||{color:'#334155'};let l=L.geoJSON(d.geojson,{style:()=>st,pointToLayer:(f,ll)=>L.circleMarker(ll,st),onEachFeature:(f,ly)=>{let p=f.properties||{};ly.bindPopup(Object.entries(p).slice(0,10).map(([k,v])=>`<b>${e(k)}:</b> ${e(v)}`).join('<br>')||'No properties')}}).addTo(map);layers[id]=l;if(l.getBounds&&l.getBounds().isValid())map.fitBounds(l.getBounds(),{padding:[20,20]});panel(d.name,`<p><b>Loaded: ${e(d.origin)}</b></p><p>Source: ${e(d.source)}</p>`)}
async function liveNode(id){active=id;bottom('Loading live API charts…','');let r=await fetch(`/api/live/node/${id}`),d=await r.json();if(!r.ok)return bottom('Error',`<p>${e(d.detail)}</p>`);bottom(`Live API outputs: ${d.node.name}`,`<div class='grid'><div class='card'><small>Rainfall</small><strong>${d.forecast.rainfall_mm} mm</strong></div><div class='card'><small>ET0</small><strong>${d.forecast.et0_mm} mm</strong></div><div class='card'><small>Water balance</small><strong>${d.forecast.water_balance_mm} mm</strong></div><div class='card'><small>Peak discharge</small><strong>${d.flood.peak_discharge_m3s} m³/s</strong></div><div class='card'><small>Drought risk</small><strong>${e(d.drought.class)}</strong></div><div class='card'><small>Flood risk</small><strong>${e(d.flood.class)}</strong></div><div class='card'><small>PM2.5</small><strong>${d.air.pm2_5}</strong></div><div class='card'><small>Maximum UV</small><strong>${d.air.uv_max}</strong></div></div><div id='chart' style='height:330px'></div><button class='btn gray' onclick="control('${id}')">Control period and scenario</button>`);Plotly.newPlot('chart',[{x:d.series.forecast_dates,y:d.series.rainfall,type:'bar',name:'Rainfall mm'},{x:d.series.forecast_dates,y:d.series.et0,type:'scatter',mode:'lines',name:'ET0 mm'},{x:d.series.forecast_dates,y:d.series.tmax,type:'scatter',mode:'lines',name:'Maximum temperature °C'},{x:d.series.flood_dates,y:d.series.discharge,type:'scatter',mode:'lines',name:'Discharge m³/s',yaxis:'y2'}],{title:'Forecast climate and flood screening',margin:{l:55,r:55,t:45,b:65},yaxis:{title:'Climate'},yaxis2:{title:'Discharge',overlaying:'y',side:'right'},legend:{orientation:'h'}},{responsive:true})}
function control(id){active=id;bottom('Control period and up-to-10-year scenario',`<div class='controls'><div><label>Control start</label><input id='cs' type='date' value='1991-01-01'></div><div><label>Control end</label><input id='ce' type='date' value='2020-12-31'></div><div><label>Projection start</label><input id='sy' type='number' value='2027'></div><div><label>Projection end</label><input id='ey' type='number' value='2036'></div><div><label>Scenario</label><select id='sc'><option value='baseline'>Baseline</option><option value='dry'>Dry</option><option value='wet'>Wet</option><option value='high_demand'>High demand</option></select></div></div><button class='btn' onclick="run('${id}')">Run analysis</button><div id='note' class='note'></div><div id='pchart' style='height:345px'></div>`)}
async function run(id){let q=new URLSearchParams({control_start:$('cs').value,control_end:$('ce').value,start_year:$('sy').value,end_year:$('ey').value,scenario:$('sc').value}),r=await fetch(`/api/projection/${id}?${q}`),d=await r.json();if(!r.ok){$('note').innerHTML=`<span style='color:#c64044'>${e(d.detail)}</span>`;return}$('note').innerHTML=`<b>Method:</b> ${e(d.method)}`;Plotly.newPlot('pchart',[{x:d.historical.map(z=>z.period),y:d.historical.map(z=>z.rainfall_mm),type:'scatter',mode:'lines',name:'Historical rainfall'},{x:d.historical.map(z=>z.period),y:d.historical.map(z=>z.et0_mm),type:'scatter',mode:'lines',name:'Historical ET0'},{x:d.projection.map(z=>z.period),y:d.projection.map(z=>z.rainfall_mm),type:'scatter',mode:'lines',name:'Projected rainfall'},{x:d.projection.map(z=>z.period),y:d.projection.map(z=>z.et0_mm),type:'scatter',mode:'lines',name:'Projected ET0'},{x:d.projection.map(z=>z.period),y:d.projection.map(z=>z.water_balance_mm),type:'scatter',mode:'lines',name:'Projected water balance',yaxis:'y2'}],{title:`${e(d.scenario)} scenario`,margin:{l:55,r:55,t:45,b:65},yaxis:{title:'mm/month'},yaxis2:{title:'Water balance',overlaying:'y',side:'right'},legend:{orientation:'h'}},{responsive:true})}
async function nasa(id){active=id;panel('NASA POWER Climate','Loading NASA POWER…');let r=await fetch(`/api/nasa/${id}`),d=await r.json();if(!r.ok)return panel('NASA POWER Climate',`<p>${e(d.detail)}</p>`);panel('NASA POWER Climate',`<p><b>${e(d.node.name)}</b></p><p>Available variables: ${e(Object.keys(d.parameters||{}).join(', '))}</p><p>NASA POWER is an independent climate and agricultural-meteorology comparison source.</p><button class='btn' onclick="control('${id}')">Open control-period analysis</button>`)}
function risk(){let rows=live.nodes.map(n=>`<tr><td>${e(n.node.name)}</td><td>${n.forecast.rainfall_mm}</td><td>${n.forecast.water_balance_mm}</td><td>${n.flood.peak_discharge_m3s}</td><td>${e(n.drought.class)}</td><td>${e(n.flood.class)}</td><td>${n.composite.score}</td></tr>`).join('');panel('Live flood–drought screening',`<p>These are API screening outputs. Use verified gauges, official flood mapping, and calibrated SWAT results for formal decisions.</p><table><thead><tr><th>Node</th><th>Rain</th><th>Balance</th><th>Peak Q</th><th>Drought</th><th>Flood</th><th>Risk</th></tr></thead><tbody>${rows}</tbody></table><button class='btn' onclick="window.open('/download/live-summary.csv','_blank')">Download CSV</button>`)}
async function status(){let d=await (await fetch('/api/status')).json(),m=d.modules.map(x=>`<tr><td>${e(x.module)}</td><td>${e(x.status)}</td><td>${e(x.need)}</td></tr>`).join(''),f=d.files.map(x=>`<tr><td>${e(x.name)}</td><td>${e(x.group)}</td><td>${x.size_kb} KB</td><td><a href='${x.url}' target='_blank'>Open</a></td></tr>`).join('');panel('System status',`<h3>API and model modules</h3><table><thead><tr><th>Module</th><th>Status</th><th>Requirement</th></tr></thead><tbody>${m}</tbody></table><h3>Uploaded files</h3>${f?`<table><thead><tr><th>Name</th><th>Group</th><th>Size</th><th>File</th></tr></thead><tbody>${f}</tbody></table>`:'<p>No files uploaded.</p>'}<p class='note'>${e(d.warning)}</p>`)}
function upload(){panel('Upload data',`<p>Upload official geometry, agency station data, rasters, SWAT/SWAT+ outputs, or metadata. Use cloud/object storage for persistent production datasets.</p><form id='up' class='drop'><label>Dataset type</label><select name='dataset_type'><option value='vector'>Vector / GeoJSON</option><option value='station'>Station CSV / Excel</option><option value='swat'>SWAT / SWAT+ output</option><option value='raster'>Raster / GeoTIFF / NetCDF</option><option value='other'>Other</option></select><label>Source / agency</label><input name='source' placeholder='HydroBASINS, LIMCOM, DWS, project output'><label>Description</label><textarea name='description' placeholder='Period, resolution, CRS, validation notes'></textarea><label>Select file</label><input name='file' type='file' required><button class='btn' type='submit'>Upload and register</button></form><div id='upnote' class='note'></div>`);$('up').onsubmit=async ev=>{ev.preventDefault();let r=await fetch('/api/upload',{method:'POST',body:new FormData(ev.target)}),d=await r.json();$('upnote').innerHTML=r.ok?`<span style='color:#078656'>${e(d.message)}</span>`:`<span style='color:#c64044'>${e(d.detail)}</span>`}}
function swat(){panel('SWAT / SWAT+ integration',`<p>Upload actual output.rch, output.sub, output.hru, output.rsv, SWAT+ SQLite/CSV exports, and a crosswalk between model IDs and GeoJSON feature IDs.</p><p>The next SWAT module can parse basin, sub-basin, reach, HRU, reservoir, observed-vs-simulated discharge, sediment, and nutrient outputs after valid files are available.</p><button class='btn' onclick='upload()'>Upload SWAT outputs</button>`)}
function credential(id){let x={gee:'Google Earth Engine needs GEE_PROJECT_ID and a configured service account before it can process CHIRPS, ERA5-Land, NDVI, WorldCover, WorldPop, and zonal statistics.',cds:'Copernicus CDS needs CDS_API_KEY and acceptance of each dataset licence before ERA5-Land or seasonal downloads.',copernicus:'Copernicus Data Space needs an OAuth client ID and secret before Sentinel searches and downloads.'}[id];panel('Credential-based module',`<p>${e(x)}</p><p>Keep credentials out of GitHub. Add them only in Render Environment Variables.</p><button class='btn gray' onclick='status()'>View status</button>`)}
async function methods(){let d=await (await fetch('/api/methods')).json();panel(d.title,`<h3>API-first workflow</h3><p>${e(d.api)}</p><h3>Geometry rule</h3><p>${e(d.geometry)}</p><h3>Projection rule</h3><p>${e(d.projection)}</p><h3>Storage note</h3><p>${e(d.storage)}</p>`)}document.addEventListener('DOMContentLoaded',start);
</script></body></html>"""

if __name__ == '__main__':
 import uvicorn
 uvicorn.run(app, host='0.0.0.0', port=8000)
