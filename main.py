"""
Limpopo Hybrid Digital Twin Portal
==================================
API-first basin portal with real GeoJSON boundaries, live API monitoring,
control-period charts, scenario projections, raster-like API grid visualization,
uploaded data registry, and SWAT-ready data integration.

Important scientific rules:
- The portal never creates fake basin or sub-basin polygons.
- Actual boundaries are loaded only from valid GeoJSON files or direct GeoJSON URLs.
- Live Open-Meteo outputs are node/grid model values, not official gauge observations.
- 1-10 year outputs are climatology/scenario projections, not exact daily forecasts.
"""

import asyncio
import csv
import io
import json
import math
import os
import time
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any

import httpx
import pandas as pd
from fastapi import FastAPI, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from raster_api import router as raster_router

APP_TITLE = "Limpopo Hybrid Digital Twin Portal"
APP_VERSION = "7.0.0"

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
VECTOR_DIR = DATA_DIR / "vector"
UPLOAD_DIR = DATA_DIR / "uploads"
SWAT_DIR = DATA_DIR / "swat"
RASTER_DIR = DATA_DIR / "raster"
METADATA_DIR = DATA_DIR / "metadata"

for folder in (VECTOR_DIR, UPLOAD_DIR, SWAT_DIR, RASTER_DIR, METADATA_DIR):
    folder.mkdir(parents=True, exist_ok=True)

app = FastAPI(title=APP_TITLE, version=APP_VERSION)
app.include_router(raster_router)
app.mount("/files", StaticFiles(directory=str(DATA_DIR)), name="files")

# Cache external API responses to reduce API calls and improve Render stability.
CACHE: dict[str, tuple[float, Any]] = {}
CACHE_TTL_SECONDS = 60 * 60

# Strategic live API analysis nodes. These are explicitly labelled as analysis nodes
# until verified agency stations are supplied through GeoJSON/CSV uploads.
NODES: dict[str, dict[str, Any]] = {
    "upper_limpopo": {
        "name": "Upper Limpopo Analysis Node",
        "lat": -25.20,
        "lon": 26.90,
        "country": "South Africa / Botswana",
        "group": "Upper Limpopo",
    },
    "gaborone": {
        "name": "Gaborone Catchment Analysis Node",
        "lat": -24.65,
        "lon": 25.91,
        "country": "Botswana",
        "group": "Upper Limpopo",
    },
    "shashe": {
        "name": "Shashe Tributary Analysis Node",
        "lat": -21.17,
        "lon": 27.51,
        "country": "Botswana / Zimbabwe",
        "group": "Shashe",
    },
    "polokwane": {
        "name": "Polokwane / Mogalakwena Analysis Node",
        "lat": -23.90,
        "lon": 29.45,
        "country": "South Africa",
        "group": "Mogalakwena",
    },
    "beitbridge": {
        "name": "Beitbridge Main-stem Analysis Node",
        "lat": -22.22,
        "lon": 30.00,
        "country": "Zimbabwe / South Africa",
        "group": "Middle Limpopo",
    },
    "olifants": {
        "name": "Olifants Analysis Node",
        "lat": -24.00,
        "lon": 31.50,
        "country": "South Africa / Mozambique",
        "group": "Olifants",
    },
    "massingir": {
        "name": "Massingir Analysis Node",
        "lat": -23.88,
        "lon": 32.16,
        "country": "Mozambique",
        "group": "Lower Limpopo",
    },
    "xai_xai": {
        "name": "Xai-Xai Basin Outlet Analysis Node",
        "lat": -25.05,
        "lon": 33.65,
        "country": "Mozambique",
        "group": "Lower Limpopo",
    },
}

# Exact filenames expected by the portal. These must be valid GeoJSON text.
GEOMETRIES: dict[str, dict[str, str]] = {
    "basin_boundary": {
        "name": "Actual Limpopo Basin Boundary",
        "filename": "limpopo_basin_boundary.geojson",
        "env": "BASIN_BOUNDARY_GEOJSON_URL",
        "source": "HydroBASINS / HydroATLAS / LIMCOM / SWAT delineation",
    },
    "subbasins_level4": {
        "name": "HydroBASINS Level 4 Sub-basins",
        "filename": "limpopo_subbasins_level4.geojson",
        "env": "SUBBASINS_L4_GEOJSON_URL",
        "source": "HydroBASINS Africa clipped to Limpopo",
    },
    "subbasins_level6": {
        "name": "HydroBASINS Level 6 Sub-basins",
        "filename": "limpopo_subbasins_level6.geojson",
        "env": "SUBBASINS_L6_GEOJSON_URL",
        "source": "HydroBASINS Africa clipped to Limpopo",
    },
    "river_network": {
        "name": "HydroRIVERS / SWAT River Network",
        "filename": "limpopo_river_network.geojson",
        "env": "RIVER_NETWORK_GEOJSON_URL",
        "source": "HydroRIVERS or SWAT reach network",
    },
    "stations": {
        "name": "Official Monitoring Stations",
        "filename": "limpopo_monitoring_stations.geojson",
        "env": "STATIONS_GEOJSON_URL",
        "source": "LIMCOM / national water agencies / verified project stations",
    },
    "swat_subbasins": {
        "name": "SWAT Delineated Sub-basins",
        "filename": "swat_subbasins.geojson",
        "env": "SWAT_SUBBASINS_GEOJSON_URL",
        "source": "SWAT / SWAT+ delineated sub-basin geometry",
    },
}

# Dropdown outputs, their source class and source recommendation.
OUTPUTS: list[dict[str, str]] = [
    {"id": "composite_risk", "name": "Composite risk", "source_type": "derived_live", "source": "Live climate + flood screening; add population/LULC/ecosystem layers when available"},
    {"id": "climate_risk", "name": "Climate risk", "source_type": "derived_live", "source": "Open-Meteo Forecast/Historical + NASA POWER comparison"},
    {"id": "flood_risk", "name": "Flood risk / discharge", "source_type": "derived_live", "source": "Open-Meteo Flood API / GloFAS screening + official gauges/SWAT"},
    {"id": "drought_risk", "name": "Drought risk", "source_type": "derived_live", "source": "Rainfall, ET0 and temperature; strengthen using CHIRPS/ERA5-Land/NDVI"},
    {"id": "population_exposed", "name": "Population exposed", "source_type": "gee_upload", "source": "WorldPop/GHSL raster overlaid with risk raster"},
    {"id": "population_exposure_percent", "name": "Population exposure percent", "source_type": "gee_upload", "source": "WorldPop/GHSL population exposure calculation"},
    {"id": "lulc_pressure", "name": "LULC pressure", "source_type": "gee_upload", "source": "ESA WorldCover/Dynamic World change analysis"},
    {"id": "urban_land_share", "name": "Urban land share", "source_type": "gee_upload", "source": "ESA WorldCover/Dynamic World zonal statistics"},
    {"id": "cropland_share", "name": "Cropland share", "source_type": "gee_upload", "source": "ESA WorldCover/Dynamic World zonal statistics"},
    {"id": "forest_share", "name": "Forest share", "source_type": "gee_upload", "source": "ESA WorldCover/Dynamic World zonal statistics"},
    {"id": "water_wetland_share", "name": "Water/wetland share", "source_type": "gee_upload", "source": "JRC Global Surface Water + wetland data"},
    {"id": "bare_land_share", "name": "Bare land share", "source_type": "gee_upload", "source": "ESA WorldCover/Dynamic World zonal statistics"},
    {"id": "forecast_rainfall", "name": "Forecast rainfall", "source_type": "live_grid", "source": "Open-Meteo Forecast API"},
    {"id": "forecast_et0", "name": "Forecast ET0", "source_type": "live_grid", "source": "Open-Meteo Forecast API"},
    {"id": "forecast_water_balance", "name": "Forecast water balance", "source_type": "live_grid", "source": "Forecast rainfall minus forecast ET0"},
    {"id": "mean_temperature", "name": "Mean temperature", "source_type": "live_grid", "source": "Open-Meteo Forecast API / NASA POWER comparison"},
    {"id": "apparent_temperature", "name": "Apparent temperature", "source_type": "live_grid", "source": "Open-Meteo Forecast API"},
    {"id": "wind_speed", "name": "Wind speed", "source_type": "live_grid", "source": "Open-Meteo Forecast API / NASA POWER comparison"},
    {"id": "wind_gust", "name": "Wind gust", "source_type": "live_grid", "source": "Open-Meteo Forecast API"},
    {"id": "solar_radiation", "name": "Solar radiation", "source_type": "live_grid", "source": "Open-Meteo Forecast API / NASA POWER comparison"},
    {"id": "soil_saturation_proxy", "name": "Soil saturation proxy", "source_type": "live_grid", "source": "Open-Meteo soil moisture + precipitation screening; validate with ERA5-Land/SMAP"},
    {"id": "reservoir_storage", "name": "Reservoir storage", "source_type": "swat_upload", "source": "Authority storage data or SWAT reservoir output"},
    {"id": "reservoir_stress", "name": "Reservoir stress", "source_type": "swat_upload", "source": "Storage, inflow/outflow, evaporation and demand"},
    {"id": "groundwater_dependency", "name": "Groundwater dependency", "source_type": "swat_upload", "source": "HydroATLAS + national data + SWAT recharge/abstraction"},
    {"id": "irrigation_pressure", "name": "Irrigation pressure", "source_type": "swat_upload", "source": "Irrigated area, ET0, demand and SWAT water availability"},
    {"id": "ecosystem_sensitivity", "name": "Ecosystem sensitivity", "source_type": "gee_upload", "source": "Wetlands, protected areas, NDVI and environmental flow"},
    {"id": "predicted_rainfall_1y", "name": "1-year predicted rainfall", "source_type": "scenario", "source": "Control-period monthly climatology and dry/baseline/wet scenario"},
    {"id": "predicted_water_balance_1y", "name": "1-year predicted water balance", "source_type": "scenario", "source": "Scenario rainfall minus ET0; improve using SWAT"},
]

CATALOGUE = [
    ("Live APIs", [
        ("live", "Open-Meteo Forecast"),
        ("history", "Open-Meteo Historical Climate"),
        ("live", "Open-Meteo Flood / GloFAS"),
        ("live", "Open-Meteo Air Quality"),
        ("nasa", "NASA POWER Climate"),
    ]),
    ("Official Geometry", [(f"geo:{key}", value["name"]) for key, value in GEOMETRIES.items()]),
    ("Basin Raster Outputs", [("raster", output["name"]) for output in OUTPUTS]),
    ("SWAT and Observations", [
        ("upload", "Upload SWAT / SWAT+ Outputs"),
        ("upload", "Upload Station CSV / Excel"),
        ("swat", "View Uploaded SWAT Files"),
    ]),
    ("Analysis and Prediction", [
        ("analysis", "Control-period Anomaly Analysis"),
        ("analysis", "Up-to-10-year Scenario Projection"),
        ("risk", "Flood / Drought Screening Risk"),
    ]),
    ("Downloads and Metadata", [
        ("download", "Download Live API Summary"),
        ("uploads", "Uploaded Dataset Register"),
        ("methods", "Methods and Data Sources"),
    ]),
]


# ---------------------------------------------------------------------------
# BASIC HELPERS
# ---------------------------------------------------------------------------

def numbers(values: list[Any] | None) -> list[float]:
    result: list[float] = []
    for value in values or []:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            value_float = float(value)
            if not math.isnan(value_float):
                result.append(value_float)
    return result


def avg(values: list[Any] | None) -> float:
    vals = numbers(values)
    return round(sum(vals) / len(vals), 3) if vals else 0.0


def total(values: list[Any] | None) -> float:
    return round(sum(numbers(values)), 3)


def maximum(values: list[Any] | None) -> float:
    vals = numbers(values)
    return round(max(vals), 3) if vals else 0.0


def bounded(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def risk_label(score: float) -> str:
    if score >= 80:
        return "Very high"
    if score >= 60:
        return "High"
    if score >= 35:
        return "Moderate"
    return "Low"


def cache_key(url: str, params: dict[str, Any]) -> str:
    return url + "?" + "&".join(f"{key}={params[key]}" for key in sorted(params))


async def get_json(client: httpx.AsyncClient, url: str, params: dict[str, Any], ttl: int = CACHE_TTL_SECONDS) -> dict[str, Any]:
    key = cache_key(url, params)
    now = time.time()
    if key in CACHE and now - CACHE[key][0] < ttl:
        return CACHE[key][1]

    response = await client.get(url, params=params, timeout=90.0)
    response.raise_for_status()
    data = response.json()
    CACHE[key] = (now, data)
    return data


def geojson_valid(payload: Any) -> bool:
    return isinstance(payload, dict) and payload.get("type") in {"Feature", "FeatureCollection"}


def data_file_index() -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for folder, group in ((VECTOR_DIR, "Vector"), (UPLOAD_DIR, "Upload"), (SWAT_DIR, "SWAT"), (RASTER_DIR, "Raster")):
        for file_path in folder.rglob("*"):
            if file_path.is_file():
                relative = str(file_path.relative_to(DATA_DIR)).replace("\\", "/")
                records.append({
                    "name": file_path.name,
                    "group": group,
                    "path": relative,
                    "size_kb": round(file_path.stat().st_size / 1024, 2),
                    "url": "/files/" + relative,
                })
    return sorted(records, key=lambda row: (row["group"], row["name"]))


def module_statuses() -> list[dict[str, str]]:
    return [
        {"module": "Open-Meteo Forecast", "status": "Active", "need": "No key"},
        {"module": "Open-Meteo Historical", "status": "Active", "need": "No key"},
        {"module": "Open-Meteo Flood", "status": "Active", "need": "No key"},
        {"module": "Open-Meteo Air Quality", "status": "Active", "need": "No key"},
        {"module": "NASA POWER", "status": "Active", "need": "No key"},
        {"module": "Google Earth Engine", "status": "Configured" if os.getenv("GEE_PROJECT_ID") else "Credential required", "need": "GEE project/service-account workflow"},
        {"module": "Google Drive Raster Store", "status": "Configured" if os.getenv("GOOGLE_DRIVE_FOLDER_ID") else "Credential required", "need": "Drive folder ID, service-account JSON and shared folder access"},
        {"module": "Copernicus CDS", "status": "Configured" if os.getenv("CDS_API_KEY") else "Credential required", "need": "CDS API key and dataset licence"},
        {"module": "Copernicus Data Space", "status": "Configured" if os.getenv("COPERNICUS_CLIENT_ID") else "Credential required", "need": "OAuth client ID and secret"},
        {"module": "SWAT / SWAT+", "status": "Upload required", "need": "Actual calibrated model outputs"},
    ]


# ---------------------------------------------------------------------------
# GEOJSON HANDLING
# ---------------------------------------------------------------------------

async def get_geometry(geometry_id: str) -> dict[str, Any]:
    if geometry_id not in GEOMETRIES:
        raise HTTPException(status_code=404, detail="Unknown geometry layer.")

    definition = GEOMETRIES[geometry_id]
    local_file = VECTOR_DIR / definition["filename"]

    if local_file.exists():
        try:
            content = local_file.read_text(encoding="utf-8-sig")
            geojson = json.loads(content)
            if not geojson_valid(geojson):
                raise ValueError("File must have GeoJSON type Feature or FeatureCollection.")
            return {
                "available": True,
                "origin": "Local GeoJSON",
                "name": definition["name"],
                "source": definition["source"],
                "geojson": geojson,
            }
        except Exception as error:
            return {
                "available": False,
                "name": definition["name"],
                "source": definition["source"],
                "reason": f"Invalid local GeoJSON: {error}",
            }

    remote_url = os.getenv(definition["env"], "").strip()
    if remote_url:
        try:
            async with httpx.AsyncClient(headers={"User-Agent": "LimpopoHybridDigitalTwin/6.0"}) as client:
                geojson = await get_json(client, remote_url, {}, ttl=86400)
            if not geojson_valid(geojson):
                raise ValueError("Configured URL did not return valid GeoJSON.")
            return {
                "available": True,
                "origin": "Official online GeoJSON",
                "name": definition["name"],
                "source": definition["source"],
                "geojson": geojson,
            }
        except Exception as error:
            return {
                "available": False,
                "name": definition["name"],
                "source": definition["source"],
                "reason": f"Unable to load configured GeoJSON URL: {error}",
            }

    return {
        "available": False,
        "name": definition["name"],
        "source": definition["source"],
        "reason": (
            f"Upload a valid file at data/vector/{definition['filename']} or configure "
            f"Render variable {definition['env']} with a direct official GeoJSON URL."
        ),
    }


# ---------------------------------------------------------------------------
# LIVE API DATA
# ---------------------------------------------------------------------------

def derived_risk_metrics(rainfall: float, et0: float, max_temp: float, peak_q: float) -> dict[str, float]:
    water_balance = rainfall - et0
    drought_score = 0.0
    if water_balance < -45:
        drought_score += 52
    elif water_balance < -20:
        drought_score += 32
    elif water_balance < 0:
        drought_score += 16
    if max_temp >= 38:
        drought_score += 22
    if rainfall < 10:
        drought_score += 18
    drought_score = bounded(drought_score)

    flood_score = 95.0 if peak_q >= 120 else 75.0 if peak_q >= 50 else 50.0 if peak_q >= 20 else 20.0
    climate_score = bounded((drought_score * 0.70) + (max(0, max_temp - 25) * 2.0) + (max(0, -water_balance) * 0.15))
    composite_score = bounded((climate_score * 0.35) + (flood_score * 0.35) + (drought_score * 0.30))

    return {
        "water_balance": round(water_balance, 2),
        "drought_score": round(drought_score, 2),
        "flood_score": round(flood_score, 2),
        "climate_score": round(climate_score, 2),
        "composite_score": round(composite_score, 2),
    }


async def live_location(lat: float, lon: float, days: int = 16, flood_days: int = 30) -> dict[str, Any]:
    forecast_params = {
        "latitude": lat,
        "longitude": lon,
        "daily": (
            "precipitation_sum,temperature_2m_max,temperature_2m_min,"
            "apparent_temperature_max,et0_fao_evapotranspiration,"
            "wind_speed_10m_max,wind_gusts_10m_max,shortwave_radiation_sum,"
            "relative_humidity_2m_mean,soil_moisture_0_to_7cm_mean"
        ),
        "forecast_days": days,
        "timezone": "auto",
    }
    flood_params = {
        "latitude": lat,
        "longitude": lon,
        "daily": "river_discharge",
        "forecast_days": flood_days,
        "timezone": "auto",
    }
    air_params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "pm10,pm2_5,dust,uv_index",
        "forecast_days": min(days, 7),
        "timezone": "auto",
    }

    async with httpx.AsyncClient(headers={"User-Agent": "LimpopoHybridDigitalTwin/6.0"}) as client:
        forecast, flood, air = await asyncio.gather(
            get_json(client, "https://api.open-meteo.com/v1/forecast", forecast_params),
            get_json(client, "https://flood-api.open-meteo.com/v1/flood", flood_params),
            get_json(client, "https://air-quality-api.open-meteo.com/v1/air-quality", air_params),
            return_exceptions=True,
        )

    daily = forecast.get("daily", {}) if isinstance(forecast, dict) else {}
    flood_daily = flood.get("daily", {}) if isinstance(flood, dict) else {}
    air_hourly = air.get("hourly", {}) if isinstance(air, dict) else {}

    rainfall = total(daily.get("precipitation_sum", []))
    et0 = total(daily.get("et0_fao_evapotranspiration", []))
    tmax = maximum(daily.get("temperature_2m_max", []))
    tmin = avg(daily.get("temperature_2m_min", []))
    peak_q = maximum(flood_daily.get("river_discharge", []))
    risk_values = derived_risk_metrics(rainfall, et0, tmax, peak_q)

    soil_moisture = avg(daily.get("soil_moisture_0_to_7cm_mean", []))
    soil_saturation_proxy = bounded(soil_moisture * 100 if soil_moisture <= 1 else soil_moisture)

    return {
        "lat": lat,
        "lon": lon,
        "forecast": {
            "rainfall_mm": rainfall,
            "et0_mm": et0,
            "water_balance_mm": risk_values["water_balance"],
            "mean_temp_c": round((avg(daily.get("temperature_2m_max", [])) + tmin) / 2, 2),
            "max_temp_c": tmax,
            "apparent_temp_c": maximum(daily.get("apparent_temperature_max", [])),
            "wind_speed_kmh": maximum(daily.get("wind_speed_10m_max", [])),
            "wind_gust_kmh": maximum(daily.get("wind_gusts_10m_max", [])),
            "solar_radiation_mj_m2": total(daily.get("shortwave_radiation_sum", [])),
            "mean_humidity_pct": avg(daily.get("relative_humidity_2m_mean", [])),
            "soil_saturation_proxy": soil_saturation_proxy,
        },
        "flood": {
            "peak_discharge_m3s": peak_q,
            "mean_discharge_m3s": avg(flood_daily.get("river_discharge", [])),
            "score": risk_values["flood_score"],
            "class": risk_label(risk_values["flood_score"]),
        },
        "drought": {
            "score": risk_values["drought_score"],
            "class": risk_label(risk_values["drought_score"]),
        },
        "climate": {
            "score": risk_values["climate_score"],
            "class": risk_label(risk_values["climate_score"]),
        },
        "composite": {
            "score": risk_values["composite_score"],
            "class": risk_label(risk_values["composite_score"]),
        },
        "air": {
            "pm2_5": avg(air_hourly.get("pm2_5", [])),
            "pm10": avg(air_hourly.get("pm10", [])),
            "dust": avg(air_hourly.get("dust", [])),
            "uv_max": maximum(air_hourly.get("uv_index", [])),
        },
        "series": {
            "forecast_dates": daily.get("time", []),
            "rainfall": daily.get("precipitation_sum", []),
            "et0": daily.get("et0_fao_evapotranspiration", []),
            "tmax": daily.get("temperature_2m_max", []),
            "tmin": daily.get("temperature_2m_min", []),
            "apparent_temp": daily.get("apparent_temperature_max", []),
            "wind_speed": daily.get("wind_speed_10m_max", []),
            "wind_gust": daily.get("wind_gusts_10m_max", []),
            "solar": daily.get("shortwave_radiation_sum", []),
            "soil_moisture": daily.get("soil_moisture_0_to_7cm_mean", []),
            "flood_dates": flood_daily.get("time", []),
            "discharge": flood_daily.get("river_discharge", []),
        },
    }


async def node_live(node_id: str, days: int = 16, flood_days: int = 30) -> dict[str, Any]:
    if node_id not in NODES:
        raise HTTPException(status_code=404, detail="Unknown analysis node.")
    node = NODES[node_id]
    payload = await live_location(node["lat"], node["lon"], days, flood_days)
    payload["node"] = {"id": node_id, **node}
    payload["note"] = "Analysis node values are live API model outputs unless replaced by verified official station observations."
    return payload


async def historical_monthly(node_id: str, start: str, end: str) -> list[dict[str, Any]]:
    if node_id not in NODES:
        raise HTTPException(status_code=404, detail="Unknown analysis node.")
    try:
        if date.fromisoformat(end) <= date.fromisoformat(start):
            raise ValueError
    except ValueError as error:
        raise HTTPException(status_code=400, detail="Use valid YYYY-MM-DD dates with the end later than the start.") from error

    node = NODES[node_id]
    params = {
        "latitude": node["lat"],
        "longitude": node["lon"],
        "start_date": start,
        "end_date": end,
        "daily": "precipitation_sum,temperature_2m_mean,et0_fao_evapotranspiration",
        "timezone": "auto",
    }
    async with httpx.AsyncClient(headers={"User-Agent": "LimpopoHybridDigitalTwin/6.0"}) as client:
        payload = await get_json(client, "https://archive-api.open-meteo.com/v1/archive", params, ttl=86400)

    daily = payload.get("daily", {})
    months: dict[str, dict[str, list[float]]] = defaultdict(lambda: {"rain": [], "temp": [], "et0": []})
    dates = daily.get("time", [])
    rain = daily.get("precipitation_sum", [])
    temp = daily.get("temperature_2m_mean", [])
    et0 = daily.get("et0_fao_evapotranspiration", [])

    for index, text_date in enumerate(dates):
        if index >= len(rain) or index >= len(temp) or index >= len(et0):
            continue
        try:
            current_date = date.fromisoformat(text_date)
        except ValueError:
            continue
        key = f"{current_date.year}-{current_date.month:02d}"
        months[key]["rain"].append(rain[index])
        months[key]["temp"].append(temp[index])
        months[key]["et0"].append(et0[index])

    rows: list[dict[str, Any]] = []
    for key in sorted(months):
        rainfall = total(months[key]["rain"])
        evap = total(months[key]["et0"])
        rows.append({
            "period": key,
            "rainfall_mm": rainfall,
            "temp_c": avg(months[key]["temp"]),
            "et0_mm": evap,
            "water_balance_mm": round(rainfall - evap, 2),
        })
    return rows


# ---------------------------------------------------------------------------
# API ROUTES
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
def home() -> HTMLResponse:
    return HTMLResponse(HTML)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "version": APP_VERSION}


@app.get("/api/config")
def api_config() -> dict[str, Any]:
    return {
        "title": APP_TITLE,
        "version": APP_VERSION,
        "catalogue": CATALOGUE,
        "outputs": OUTPUTS,
        "geometries": GEOMETRIES,
        "modules": module_statuses(),
    }


@app.get("/api/status")
def api_status() -> dict[str, Any]:
    return {
        "modules": module_statuses(),
        "files": data_file_index(),
        "warning": "Render local upload storage can be erased after a redeploy. Use GitHub for small core GeoJSON files and cloud/object storage for production rasters and model outputs.",
    }


@app.get("/api/geometry/{geometry_id}")
async def api_geometry(geometry_id: str) -> dict[str, Any]:
    return await get_geometry(geometry_id)


@app.get("/api/live/basin")
async def api_live_basin(
    forecast_days: int = Query(16, ge=1, le=16),
    flood_days: int = Query(30, ge=1, le=30),
) -> dict[str, Any]:
    responses = await asyncio.gather(
        *(node_live(node_id, forecast_days, flood_days) for node_id in NODES),
        return_exceptions=True,
    )
    nodes = [item for item in responses if isinstance(item, dict)]
    if not nodes:
        raise HTTPException(status_code=502, detail="No live API data could be retrieved.")
    return {
        "created_at": datetime.utcnow().isoformat() + "Z",
        "nodes": nodes,
        "summary": {
            "rainfall_mm": avg([item["forecast"]["rainfall_mm"] for item in nodes]),
            "et0_mm": avg([item["forecast"]["et0_mm"] for item in nodes]),
            "balance_mm": avg([item["forecast"]["water_balance_mm"] for item in nodes]),
            "peak_discharge_m3s": avg([item["flood"]["peak_discharge_m3s"] for item in nodes]),
            "risk": avg([item["composite"]["score"] for item in nodes]),
        },
    }


@app.get("/api/live/node/{node_id}")
async def api_live_node(node_id: str) -> dict[str, Any]:
    return await node_live(node_id)


@app.get("/api/live/grid")
async def api_live_grid(
    south: float = Query(..., ge=-90, le=90),
    west: float = Query(..., ge=-180, le=180),
    north: float = Query(..., ge=-90, le=90),
    east: float = Query(..., ge=-180, le=180),
    metric: str = Query("forecast_rainfall"),
    size: int = Query(5, ge=3, le=7),
    forecast_days: int = Query(7, ge=1, le=16),
) -> dict[str, Any]:
    if metric not in {item["id"] for item in OUTPUTS}:
        raise HTTPException(status_code=400, detail="Unknown output metric.")
    if south >= north or west >= east:
        raise HTTPException(status_code=400, detail="Invalid raster extent.")

    supported = {
        "forecast_rainfall", "forecast_et0", "forecast_water_balance", "mean_temperature",
        "apparent_temperature", "wind_speed", "wind_gust", "solar_radiation",
        "soil_saturation_proxy", "flood_risk", "drought_risk", "climate_risk", "composite_risk",
    }
    if metric not in supported:
        output = next(item for item in OUTPUTS if item["id"] == metric)
        return {
            "available": False,
            "metric": metric,
            "message": f"{output['name']} requires {output['source']}. Use the upload/GEE/SWAT workflow rather than a live API grid.",
            "cells": [],
        }

    lat_step = (north - south) / size
    lon_step = (east - west) / size
    requests = []
    grid_points: list[tuple[int, int, float, float]] = []
    for row in range(size):
        for col in range(size):
            lat = south + (row + 0.5) * lat_step
            lon = west + (col + 0.5) * lon_step
            grid_points.append((row, col, lat, lon))
            requests.append(live_location(lat, lon, forecast_days, min(16, forecast_days)))

    raw = await asyncio.gather(*requests, return_exceptions=True)
    cells: list[dict[str, Any]] = []

    def metric_value(payload: dict[str, Any]) -> float:
        if metric == "forecast_rainfall":
            return payload["forecast"]["rainfall_mm"]
        if metric == "forecast_et0":
            return payload["forecast"]["et0_mm"]
        if metric == "forecast_water_balance":
            return payload["forecast"]["water_balance_mm"]
        if metric == "mean_temperature":
            return payload["forecast"]["mean_temp_c"]
        if metric == "apparent_temperature":
            return payload["forecast"]["apparent_temp_c"]
        if metric == "wind_speed":
            return payload["forecast"]["wind_speed_kmh"]
        if metric == "wind_gust":
            return payload["forecast"]["wind_gust_kmh"]
        if metric == "solar_radiation":
            return payload["forecast"]["solar_radiation_mj_m2"]
        if metric == "soil_saturation_proxy":
            return payload["forecast"]["soil_saturation_proxy"]
        if metric == "flood_risk":
            return payload["flood"]["score"]
        if metric == "drought_risk":
            return payload["drought"]["score"]
        if metric == "climate_risk":
            return payload["climate"]["score"]
        return payload["composite"]["score"]

    for (row, col, lat, lon), payload in zip(grid_points, raw):
        if not isinstance(payload, dict):
            continue
        cells.append({
            "row": row,
            "col": col,
            "lat": lat,
            "lon": lon,
            "south": south + row * lat_step,
            "north": south + (row + 1) * lat_step,
            "west": west + col * lon_step,
            "east": west + (col + 1) * lon_step,
            "value": round(metric_value(payload), 3),
        })

    output = next(item for item in OUTPUTS if item["id"] == metric)
    return {
        "available": True,
        "metric": metric,
        "name": output["name"],
        "source": output["source"],
        "method": "Raster-like live API grid: weather-model values queried at regular grid-centre points. It is not a satellite raster.",
        "bounds": {"south": south, "west": west, "north": north, "east": east},
        "cells": cells,
        "min": min([cell["value"] for cell in cells], default=0),
        "max": max([cell["value"] for cell in cells], default=0),
    }


@app.get("/api/history/{node_id}")
async def api_history(
    node_id: str,
    start: str = Query("1991-01-01"),
    end: str = Query("2020-12-31"),
) -> dict[str, Any]:
    return {
        "node": {"id": node_id, **NODES[node_id]},
        "control_period": {"start": start, "end": end},
        "monthly": await historical_monthly(node_id, start, end),
    }


@app.get("/api/projection/{node_id}")
async def api_projection(
    node_id: str,
    control_start: str = Query("1991-01-01"),
    control_end: str = Query("2020-12-31"),
    start_year: int = Query(2027, ge=1900, le=2200),
    end_year: int = Query(2036, ge=1900, le=2200),
    scenario: str = Query("baseline", pattern="^(dry|baseline|wet|high_demand)$"),
) -> dict[str, Any]:
    if end_year < start_year or end_year - start_year > 9:
        raise HTTPException(status_code=400, detail="Scenario period must be no more than 10 years.")

    historical = await historical_monthly(node_id, control_start, control_end)
    if not historical:
        raise HTTPException(status_code=400, detail="No historical control-period data available.")

    climatology: dict[int, dict[str, list[float]]] = defaultdict(lambda: {"rain": [], "temp": [], "et0": []})
    for row in historical:
        month = int(row["period"].split("-")[1])
        climatology[month]["rain"].append(row["rainfall_mm"])
        climatology[month]["temp"].append(row["temp_c"])
        climatology[month]["et0"].append(row["et0_mm"])

    scenario_factor = {"dry": 0.80, "baseline": 1.0, "wet": 1.20, "high_demand": 0.90}[scenario]
    projection: list[dict[str, Any]] = []
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            rainfall = avg(climatology[month]["rain"]) * scenario_factor
            et0 = avg(climatology[month]["et0"]) * (1.08 if scenario == "high_demand" else 1.0)
            projection.append({
                "period": f"{year}-{month:02d}",
                "rainfall_mm": round(rainfall, 2),
                "temp_c": avg(climatology[month]["temp"]),
                "et0_mm": round(et0, 2),
                "water_balance_mm": round(rainfall - et0, 2),
            })

    return {
        "node": {"id": node_id, **NODES[node_id]},
        "control_period": {"start": control_start, "end": control_end},
        "projection_period": {"start_year": start_year, "end_year": end_year},
        "scenario": scenario,
        "method": "Monthly hydroclimate scenario generated from control-period climatology. It is not an exact daily weather forecast.",
        "historical": historical,
        "projection": projection,
    }


@app.get("/api/nasa/{node_id}")
async def api_nasa(
    node_id: str,
    start: str = Query("2020-01-01"),
    end: str = Query("2020-12-31"),
) -> dict[str, Any]:
    if node_id not in NODES:
        raise HTTPException(status_code=404, detail="Unknown node.")
    node = NODES[node_id]
    params = {
        "parameters": "PRECTOTCORR,T2M,EVPTRNS,WS2M,ALLSKY_SFC_SW_DWN",
        "community": "AG",
        "longitude": node["lon"],
        "latitude": node["lat"],
        "start": start.replace("-", ""),
        "end": end.replace("-", ""),
        "format": "JSON",
    }
    async with httpx.AsyncClient(headers={"User-Agent": "LimpopoHybridDigitalTwin/6.0"}) as client:
        payload = await get_json(client, "https://power.larc.nasa.gov/api/temporal/daily/point", params, ttl=86400)
    return {
        "node": {"id": node_id, **node},
        "parameters": payload.get("properties", {}).get("parameter", {}),
    }


# ---------------------------------------------------------------------------
# UPLOADS AND SWAT-READY DATA
# ---------------------------------------------------------------------------

@app.post("/api/upload")
async def api_upload(
    file: UploadFile = File(...),
    dataset_type: str = Form(...),
    target_layer: str = Form("general"),
    source: str = Form("User upload"),
    description: str = Form(""),
) -> dict[str, Any]:
    allowed = {".geojson", ".json", ".zip", ".csv", ".xlsx", ".xls", ".tif", ".tiff", ".nc", ".txt", ".sqlite", ".db"}
    original_name = Path(file.filename or "uploaded_file").name
    suffix = Path(original_name).suffix.lower()
    if suffix not in allowed:
        raise HTTPException(status_code=400, detail="Allowed formats: GeoJSON, ZIP, CSV, Excel, GeoTIFF, NetCDF, TXT, SQLite or DB.")

    payload = await file.read()
    if len(payload) > 100 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="Prototype upload limit is 100 MB.")

    # Core geometry files are saved using exact names, fixing the old timestamp issue.
    if target_layer in GEOMETRIES:
        if suffix not in {".geojson", ".json"}:
            raise HTTPException(status_code=400, detail="Core geometry layers must be uploaded as valid GeoJSON.")
        try:
            geojson = json.loads(payload.decode("utf-8-sig"))
            if not geojson_valid(geojson):
                raise ValueError("GeoJSON must contain type Feature or FeatureCollection.")
        except Exception as error:
            raise HTTPException(status_code=400, detail=f"Invalid GeoJSON upload: {error}") from error
        target_path = VECTOR_DIR / GEOMETRIES[target_layer]["filename"]
        target_path.write_bytes(payload)
        saved_group = "Vector"
    else:
        kind = dataset_type.lower()
        if kind in {"swat", "station"}:
            folder = SWAT_DIR
            saved_group = "SWAT"
        elif kind in {"raster", "geotiff", "netcdf"}:
            folder = RASTER_DIR
            saved_group = "Raster"
        else:
            folder = UPLOAD_DIR
            saved_group = "Upload"
        stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        target_path = folder / f"{stamp}_{original_name}"
        target_path.write_bytes(payload)

    register = METADATA_DIR / "upload_register.json"
    existing: list[dict[str, Any]] = []
    if register.exists():
        try:
            existing = json.loads(register.read_text(encoding="utf-8"))
        except Exception:
            existing = []
    record = {
        "name": original_name,
        "saved_path": str(target_path.relative_to(DATA_DIR)).replace("\\", "/"),
        "group": saved_group,
        "dataset_type": dataset_type,
        "target_layer": target_layer,
        "source": source,
        "description": description,
        "size_kb": round(len(payload) / 1024, 2),
        "uploaded_at": datetime.utcnow().isoformat() + "Z",
    }
    existing.append(record)
    register.write_text(json.dumps(existing, indent=2), encoding="utf-8")

    return {
        "message": "Upload completed. Validate CRS, source, metadata and scientific suitability before use.",
        "record": record,
    }


@app.get("/api/uploads")
def api_uploads() -> dict[str, Any]:
    return {"files": data_file_index()}


@app.get("/api/swat/summary")
def api_swat_summary() -> dict[str, Any]:
    files = [item for item in data_file_index() if item["group"] == "SWAT"]
    return {
        "files": files,
        "message": (
            "Upload actual output.rch, output.sub, output.hru, output.rsv, SWAT+ SQLite/CSV, "
            "or exported summaries. A future parser can connect fields to sub-basin/reach/HRU IDs."
        ),
    }


@app.get("/download/live-summary.csv")
async def download_live_summary() -> StreamingResponse:
    live = await api_live_basin()
    rows = []
    for item in live["nodes"]:
        rows.append({
            "node_id": item["node"]["id"],
            "node_name": item["node"]["name"],
            "country": item["node"]["country"],
            "latitude": item["node"]["lat"],
            "longitude": item["node"]["lon"],
            "forecast_rainfall_mm": item["forecast"]["rainfall_mm"],
            "forecast_et0_mm": item["forecast"]["et0_mm"],
            "forecast_water_balance_mm": item["forecast"]["water_balance_mm"],
            "peak_discharge_m3s": item["flood"]["peak_discharge_m3s"],
            "flood_risk": item["flood"]["class"],
            "drought_risk": item["drought"]["class"],
            "composite_risk": item["composite"]["class"],
        })
    stream = io.StringIO()
    writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
    writer.writeheader()
    writer.writerows(rows)
    return StreamingResponse(
        iter([stream.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="limpopo_live_summary.csv"'},
    )


@app.get("/api/methods")
def api_methods() -> dict[str, Any]:
    return {
        "title": "Methods and source rules",
        "api_first": "Use live APIs for current monitoring. Use official observed stations, gridded raster datasets, and calibrated SWAT outputs for formal scientific analysis.",
        "geometry": "The portal never creates fake polygons. It only loads valid HydroBASINS/HydroATLAS, LIMCOM/national authority, or SWAT-delineated GeoJSON.",
        "raster": "Live API grids are raster-like weather-model point samples on a regular grid. CHIRPS, ERA5-Land, MODIS/Sentinel, WorldCover, WorldPop and SWAT products remain the recommended scientific raster layers.",
        "projection": "Up-to-10-year outputs are monthly control-period scenario projections, not deterministic daily forecasts.",
        "storage": "Render local upload storage is temporary. Use GitHub for small core GeoJSON and cloud/object storage for large rasters, NetCDF and full SWAT outputs.",
    }


# ---------------------------------------------------------------------------
# FRONT END
# ---------------------------------------------------------------------------

HTML = r'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Limpopo Hybrid Digital Twin Portal</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://unpkg.com/@turf/turf@6/turf.min.js"></script>
<style>
:root{--navy:#101b2d;--cyan:#38a5c2;--line:#dce3ea;--bg:#edf2f5;--text:#152033;--muted:#627084;--green:#078a58;--orange:#d97706;--red:#c43d43}
*{box-sizing:border-box}body{margin:0;font-family:Arial,Helvetica,sans-serif;background:var(--bg);color:var(--text)}
header{height:76px;background:var(--navy);color:#fff;display:flex;align-items:center;gap:9px;padding:12px 16px}.title{flex:1}h1{font-size:19px;margin:0}header p{margin:5px 0 0;font-size:11px;color:#c8d2de}.top{border:1px solid #d7e1eb;border-radius:4px;background:transparent;color:#fff;padding:8px 10px;font-weight:700;font-size:12px;cursor:pointer;text-decoration:none}
.app{display:grid;grid-template-columns:350px 1fr;height:calc(100vh - 76px)}.sidebar{background:#fafbfd;border-right:1px solid var(--line);overflow:auto}.sidebar-head{padding:12px;background:#fff;border-bottom:1px solid var(--line)}.tabs{display:flex;gap:7px;margin-bottom:10px}.tabs button{padding:7px 9px;border:1px solid var(--line);background:#fff;border-radius:4px;font-weight:700;cursor:pointer;font-size:12px}.tabs button:first-child{background:var(--navy);color:#fff}.search{width:100%;border:0;background:#f1f4f7;padding:11px;font-size:13px}.catalogue{padding:6px 4px 20px}.group{border-bottom:1px solid var(--line)}.group-head{padding:13px 10px;font-weight:700;display:flex;justify-content:space-between;cursor:pointer;font-size:13px}.group-head.active{background:var(--cyan);color:#fff}.items{display:none;padding:4px 8px 10px}.items.show{display:block}.item{display:flex;gap:8px;padding:8px 3px;cursor:pointer;border-radius:4px}.item:hover{background:#e9eef3}.ico{width:15px;height:13px;border:1.5px solid #778497;border-radius:2px;margin-top:2px;flex:none}.item b{font-size:12px}.tag{font-size:10px;color:var(--muted);display:block;margin-top:3px}.tag.live{color:var(--green)}
.workspace{position:relative}.map{height:100%;width:100%}.notice{position:absolute;z-index:600;top:13px;left:13px;max-width:550px;background:#fff;padding:9px 11px;border-radius:4px;box-shadow:0 2px 12px #0002;font-size:12px}.map-controls{position:absolute;z-index:600;top:13px;right:13px;width:295px;background:#fff;border-radius:4px;box-shadow:0 2px 12px #0002;padding:11px}.map-controls label{font-size:11px;font-weight:700;color:var(--muted);display:block;margin-bottom:3px}.map-controls select,.map-controls button{width:100%;padding:8px;border:1px solid var(--line);border-radius:3px;margin-bottom:7px}.map-controls button{background:#218da9;color:#fff;border:0;font-weight:700;cursor:pointer}.map-controls small{font-size:10px;color:var(--muted);line-height:1.35;display:block}
.panel{display:none;position:absolute;z-index:800;right:16px;top:16px;width:420px;max-height:calc(100% - 32px);overflow:auto;background:#fff;border-radius:5px;box-shadow:0 9px 30px #0003}.panel.show,.bottom.show{display:block}.panel-head{display:flex;align-items:center;justify-content:space-between;background:var(--navy);color:#fff;padding:13px 15px}.panel-head h2{margin:0;font-size:17px}.close{background:transparent;border:1px solid #dbe5ef;color:#fff;border-radius:3px;padding:5px 8px;cursor:pointer}.panel-body{padding:15px;font-size:13px;line-height:1.45}.panel-body h3{font-size:14px;margin:15px 0 5px}.grid{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin:10px 0}.card{background:#f3f6f8;border-radius:3px;padding:8px}.card small{font-size:10px;color:var(--muted);display:block}.card strong{font-size:13px}.btn{width:100%;border:0;border-radius:4px;background:#218da9;color:#fff;padding:10px;font-weight:700;cursor:pointer;margin-top:8px}.btn.gray{background:#526177}.btn.orange{background:#b56b00}
.bottom{display:none;position:absolute;z-index:800;left:16px;right:16px;bottom:16px;max-height:49%;overflow:auto;background:#fff;border-radius:5px;box-shadow:0 9px 30px #0003}.bottom-head{display:flex;align-items:center;justify-content:space-between;padding:12px 14px;border-bottom:1px solid var(--line)}.bottom-head h3{margin:0;font-size:16px}.bottom-body{padding:14px}.controls{display:grid;grid-template-columns:repeat(4,1fr);gap:8px}.controls label{font-size:10px;color:var(--muted);font-weight:700;display:block;margin-bottom:3px}.controls input,.controls select,.drop input,.drop select,.drop textarea{width:100%;padding:7px;border:1px solid var(--line);border-radius:3px}.drop{border:2px dashed #a8b5c5;padding:14px;border-radius:5px;background:#f8fafc}.note{font-size:12px;color:#4f5d70;margin:10px 0}table{border-collapse:collapse;width:100%;font-size:11px}th,td{padding:7px;border-bottom:1px solid var(--line);text-align:left}th{background:#f2f5f7}.legend{background:#fff;padding:9px;border-radius:4px;box-shadow:0 2px 11px #0002;font-size:11px}.raster-legend{position:absolute;z-index:650;bottom:28px;right:15px;background:#fff;border-radius:4px;padding:8px;box-shadow:0 2px 11px #0002;font-size:11px;display:none}.grad{height:12px;width:190px;background:linear-gradient(90deg,#2c7bb6,#abd9e9,#ffffbf,#fdae61,#d7191c);margin:4px 0}
@media(max-width:900px){.app{grid-template-columns:1fr}.sidebar{display:none}.panel{left:10px;right:10px;width:auto}.controls{grid-template-columns:repeat(2,1fr)}.map-controls{width:250px}}
</style>
</head>
<body>
<header><div class="title"><h1>Limpopo Hybrid Digital Twin Portal</h1><p>Live APIs • real official geometry • Google Drive GEE rasters • SWAT and station data • control-period scenarios.</p></div><a class="top" href="/rasters" target="_blank">GEE Raster Explorer</a><button class="top" onclick="methods()">Methods</button><button class="top" onclick="upload()">Upload data</button><a class="top" href="/download/live-summary.csv" target="_blank">Live CSV</a></header>
<div class="app"><aside class="sidebar"><div class="sidebar-head"><div class="tabs"><button>Data</button><button onclick="statusPanel()">System status</button><button onclick="upload()">Upload</button></div><input id="search" class="search" placeholder="Search components" oninput="filterItems()"></div><div id="catalogue" class="catalogue"></div></aside>
<main class="workspace"><div id="map" class="map"></div><div id="notice" class="notice">Connecting to live API services…</div>
<div class="map-controls"><label for="outputSelect">Basin output map</label><select id="outputSelect"></select><label for="gridSize">Live grid resolution</label><select id="gridSize"><option value="4">4 × 4 (fast)</option><option value="5" selected>5 × 5 (standard)</option><option value="6">6 × 6 (detailed)</option></select><button onclick="loadOutputMap()">Load selected map</button><button class="btn gray" onclick="clearRaster()">Clear raster grid</button><small>Live maps are regular weather-model grid samples. Official CHIRPS/ERA5-Land/Sentinel/SWAT rasters should be added through GEE or upload.</small></div>
<div id="rasterLegend" class="raster-legend"><b id="legendTitle">Raster</b><div class="grad"></div><span id="legendMin"></span> <span style="float:right" id="legendMax"></span></div>
<section id="panel" class="panel"><div class="panel-head"><h2 id="panelTitle">Information</h2><button class="close" onclick="closePanel()">Done</button></div><div id="panelBody" class="panel-body"></div></section>
<section id="bottom" class="bottom"><div class="bottom-head"><h3 id="bottomTitle">Analysis</h3><button onclick="closeBottom()">Close</button></div><div id="bottomBody" class="bottom-body"></div></section></main></div>
<script>
let map, config, live, markers, rasterLayer, geometryLayers = {}, activeNode = 'upper_limpopo', basinGeojson = null;
const $ = id => document.getElementById(id);
function esc(value){return String(value ?? '').replace(/[&<>"']/g, char => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#039;'}[char]));}
function colorForRisk(score){return score>=80?'#c43d43':score>=60?'#e87922':score>=35?'#d7aa10':'#078a58';}
function openPanel(title, html){$('panelTitle').textContent=title;$('panelBody').innerHTML=html;$('panel').classList.add('show');}
function closePanel(){$('panel').classList.remove('show');}
function openBottom(title, html){$('bottomTitle').textContent=title;$('bottomBody').innerHTML=html;$('bottom').classList.add('show');}
function closeBottom(){$('bottom').classList.remove('show');}
function initMap(){map=L.map('map').setView([-23.7,30.1],6);const standard=L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',{attribution:'© OpenStreetMap contributors'}).addTo(map);const topo=L.tileLayer('https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png',{attribution:'© OpenTopoMap'});L.control.layers({'Standard':standard,'Topographic':topo}).addTo(map);markers=L.featureGroup().addTo(map);const legend=L.control({position:'bottomleft'});legend.onAdd=function(){const div=L.DomUtil.create('div','legend');div.innerHTML='<b>Map rule</b><br>Live circles = API analysis nodes.<br>Boundary layers = verified GeoJSON only.<br>Raster-like grids = sampled weather-model values.';return div;};legend.addTo(map);}
async function start(){initMap();config=await (await fetch('/api/config')).json();renderCatalogue();populateOutputs();await loadLive();await autoLoadBoundary();}
function renderCatalogue(){const root=$('catalogue');root.innerHTML='';config.catalogue.forEach((group,index)=>{const wrap=document.createElement('div');wrap.className='group';const head=document.createElement('div');head.className='group-head'+(index===0?' active':'');head.innerHTML=`<span>${esc(group[0])}</span><span>⌃</span>`;const list=document.createElement('div');list.className='items'+(index===0?' show':'');head.onclick=()=>{list.classList.toggle('show');head.classList.toggle('active');head.lastChild.textContent=list.classList.contains('show')?'⌃':'⌄';};group[1].forEach(item=>{const row=document.createElement('div');row.className='item';row.dataset.q=(item[1]+' '+group[0]).toLowerCase();const liveItem=['live','history','nasa'].includes(item[0]);row.innerHTML=`<span class='ico'></span><span><b>${esc(item[1])}</b><span class='tag ${liveItem?'live':''}'>${liveItem?'Direct API':'Select module'}</span></span>`;row.onclick=()=>catalogueClick(item[0],item[1]);list.appendChild(row);});wrap.append(head,list);root.appendChild(wrap);});}
function filterItems(){const query=$('search').value.trim().toLowerCase();document.querySelectorAll('.item').forEach(row=>row.style.display=(!query||row.dataset.q.includes(query))?'flex':'none');}
function populateOutputs(){const select=$('outputSelect');config.outputs.forEach(item=>{const option=document.createElement('option');option.value=item.id;option.textContent=item.name;select.appendChild(option);});}
async function autoLoadBoundary(){try{const response=await fetch('/api/geometry/basin_boundary');const data=await response.json();if(data.available){basinGeojson=data.geojson;}}catch(error){console.warn(error);}}
async function loadLive(){try{const response=await fetch('/api/live/basin');live=await response.json();if(!response.ok)throw Error(live.detail||'Live request failed');if(!markers){markers=L.featureGroup().addTo(map);}markers.clearLayers();live.nodes.forEach(node=>{const marker=L.circleMarker([node.node.lat,node.node.lon],{radius:7+node.composite.score/14,color:'#142034',weight:2,fillColor:colorForRisk(node.composite.score),fillOpacity:.88}).bindPopup(`<b>${esc(node.node.name)}</b><br>${esc(node.node.country)}<hr><b>Rainfall:</b> ${node.forecast.rainfall_mm} mm<br><b>ET0:</b> ${node.forecast.et0_mm} mm<br><b>Water balance:</b> ${node.forecast.water_balance_mm} mm<br><b>Peak discharge:</b> ${node.flood.peak_discharge_m3s} m³/s<br><b>Risk:</b> ${esc(node.composite.class)}<br><button onclick="liveNode('${node.node.id}')">Charts</button> <button onclick="control('${node.node.id}')">Scenario</button>`).addTo(markers);});const bounds=markers.getBounds();if(bounds.isValid())map.fitBounds(bounds,{padding:[35,35]});const summary=live.summary;$('notice').innerHTML=`<b>Live APIs active.</b> Mean rainfall ${summary.rainfall_mm} mm; ET0 ${summary.et0_mm} mm; balance ${summary.balance_mm} mm; peak discharge ${summary.peak_discharge_m3s} m³/s. <button onclick="riskOverview()">Risk overview</button>`;}catch(error){$('notice').innerHTML=`<b>Live API error:</b> ${esc(error.message)}. Refresh after the service wakes up.`;}}
function catalogueClick(id,name){if(id.startsWith('geo:'))return toggleGeometry(id.slice(4));if(id==='live')return liveNode(activeNode);if(id==='history'||id==='analysis')return control(activeNode);if(id==='nasa')return nasa(activeNode);if(id==='raster'){$('outputSelect').value=config.outputs.find(x=>x.name===name)?.id||'forecast_rainfall';return loadOutputMap();}if(id==='risk')return riskOverview();if(id==='upload'||id==='uploads')return upload();if(id==='download')return window.open('/download/live-summary.csv','_blank');if(id==='methods')return methods();if(id==='swat')return swat();}
async function toggleGeometry(id){if(geometryLayers[id]){map.removeLayer(geometryLayers[id]);delete geometryLayers[id];return;}const response=await fetch(`/api/geometry/${id}`);const data=await response.json();if(!data.available){openPanel(data.name,`<p><b>Actual geometry is not configured.</b></p><p>${esc(data.reason)}</p><p>Expected source: ${esc(data.source)}</p>`);return;}const styles={basin_boundary:{color:'#111b2d',weight:4,fill:false},subbasins_level4:{color:'#2563eb',weight:2,fillColor:'#60a5fa',fillOpacity:.08},subbasins_level6:{color:'#0d9488',weight:1,fillColor:'#5eead4',fillOpacity:.05},river_network:{color:'#0284c7',weight:2},stations:{color:'#111827',fillColor:'#fff',radius:6,weight:2},swat_subbasins:{color:'#7c3aed',weight:1.2,fillColor:'#c4b5fd',fillOpacity:.08}}[id]||{color:'#334155'};const layer=L.geoJSON(data.geojson,{style:()=>styles,pointToLayer:(feature,latlng)=>L.circleMarker(latlng,styles),onEachFeature:(feature,leafletLayer)=>{const properties=feature.properties||{};leafletLayer.bindPopup(Object.entries(properties).slice(0,12).map(([key,value])=>`<b>${esc(key)}:</b> ${esc(value)}`).join('<br>')||'No attributes');}}).addTo(map);geometryLayers[id]=layer;if(id==='basin_boundary'){basinGeojson=data.geojson;}if(layer.getBounds&&layer.getBounds().isValid())map.fitBounds(layer.getBounds(),{padding:[20,20]});openPanel(data.name,`<p><b>Loaded ${esc(data.origin)}.</b></p><p>${esc(data.source)}</p>`);}
function colorRamp(value,min,max){if(max===min)return '#2c7bb6';let t=(value-min)/(max-min);t=Math.max(0,Math.min(1,t));const stops=[[44,123,182],[171,217,233],[255,255,191],[253,174,97],[215,25,28]];const scaled=t*(stops.length-1),index=Math.min(stops.length-2,Math.floor(scaled)),f=scaled-index;const a=stops[index],b=stops[index+1];return `rgb(${Math.round(a[0]+(b[0]-a[0])*f)},${Math.round(a[1]+(b[1]-a[1])*f)},${Math.round(a[2]+(b[2]-a[2])*f)})`;}
function cellInsideBasin(cell){if(!basinGeojson||!window.turf)return true;try{const point=turf.point([cell.lon,cell.lat]);if(basinGeojson.type==='FeatureCollection'){return basinGeojson.features.some(feature=>turf.booleanPointInPolygon(point,feature));}return turf.booleanPointInPolygon(point,basinGeojson);}catch(error){return true;}}
async function loadOutputMap(){const metric=$('outputSelect').value;const output=config.outputs.find(x=>x.id===metric);if(!output)return;const liveSupported=['live_grid','derived_live'];if(!liveSupported.includes(output.source_type)){openPanel(output.name,`<p><b>This output is not available from a no-key live API.</b></p><p>Recommended source: ${esc(output.source)}</p><p>Use the Upload Data option or configure Google Earth Engine/Copernicus/SWAT data. The portal will not fabricate a scientific raster.</p>`);return;}if(!basinGeojson){openPanel(output.name,`<p><b>Load a valid Actual Limpopo Basin Boundary first.</b></p><p>The live grid should be clipped to verified basin geometry.</p><button class='btn' onclick="toggleGeometry('basin_boundary')">Load basin boundary</button>`);return;}let bounds;try{const temp=L.geoJSON(basinGeojson);bounds=temp.getBounds();}catch(error){openPanel(output.name,`<p>Cannot calculate valid basin extent: ${esc(error.message)}</p>`);return;}const size=$('gridSize').value;const params=new URLSearchParams({south:bounds.getSouth(),west:bounds.getWest(),north:bounds.getNorth(),east:bounds.getEast(),metric,size,forecast_days:7});$('notice').innerHTML=`<b>Building live ${esc(output.name)} grid…</b> This may take a short time.`;const response=await fetch(`/api/live/grid?${params}`);const data=await response.json();if(!response.ok||!data.available){openPanel(output.name,`<p>${esc(data.message||data.detail||'Grid request failed.')}</p><p>Recommended source: ${esc(output.source)}</p>`);return;}clearRaster();rasterLayer=L.layerGroup().addTo(map);data.cells.filter(cellInsideBasin).forEach(cell=>{L.rectangle([[cell.south,cell.west],[cell.north,cell.east]],{stroke:false,fillColor:colorRamp(cell.value,data.min,data.max),fillOpacity:.62,interactive:true}).bindPopup(`<b>${esc(data.name)}</b><br>${cell.value}<br><small>${esc(data.method)}</small>`).addTo(rasterLayer);});$('legendTitle').textContent=data.name;$('legendMin').textContent=data.min;$('legendMax').textContent=data.max;$('rasterLegend').style.display='block';$('notice').innerHTML=`<b>Loaded ${esc(data.name)}.</b> ${esc(data.method)} Source: ${esc(data.source)}`;}
function clearRaster(){if(rasterLayer){map.removeLayer(rasterLayer);rasterLayer=null;}$('rasterLegend').style.display='none';}
async function liveNode(id){activeNode=id;openBottom('Loading live API charts…','');const response=await fetch(`/api/live/node/${id}`);const data=await response.json();if(!response.ok){openBottom('Error',`<p>${esc(data.detail||'Request failed')}</p>`);return;}openBottom(`Live API outputs: ${data.node.name}`,`<div class='grid'><div class='card'><small>Rainfall</small><strong>${data.forecast.rainfall_mm} mm</strong></div><div class='card'><small>ET0</small><strong>${data.forecast.et0_mm} mm</strong></div><div class='card'><small>Water balance</small><strong>${data.forecast.water_balance_mm} mm</strong></div><div class='card'><small>Peak discharge</small><strong>${data.flood.peak_discharge_m3s} m³/s</strong></div><div class='card'><small>Drought risk</small><strong>${esc(data.drought.class)}</strong></div><div class='card'><small>Flood risk</small><strong>${esc(data.flood.class)}</strong></div><div class='card'><small>PM2.5</small><strong>${data.air.pm2_5}</strong></div><div class='card'><small>Maximum UV</small><strong>${data.air.uv_max}</strong></div></div><div id='chart' style='height:330px'></div><button class='btn gray' onclick="control('${id}')">Control period and scenario</button>`);Plotly.newPlot('chart',[{x:data.series.forecast_dates,y:data.series.rainfall,type:'bar',name:'Rainfall mm'},{x:data.series.forecast_dates,y:data.series.et0,type:'scatter',mode:'lines',name:'ET0 mm'},{x:data.series.forecast_dates,y:data.series.tmax,type:'scatter',mode:'lines',name:'Maximum temperature °C'},{x:data.series.flood_dates,y:data.series.discharge,type:'scatter',mode:'lines',name:'Discharge m³/s',yaxis:'y2'}],{title:'Forecast climate and flood screening',margin:{l:55,r:55,t:45,b:65},yaxis:{title:'Climate variables'},yaxis2:{title:'Discharge m³/s',overlaying:'y',side:'right'},legend:{orientation:'h'}},{responsive:true});}
function control(id){activeNode=id;openBottom('Control period and up-to-10-year scenario',`<div class='controls'><div><label>Control start</label><input id='cs' type='date' value='1991-01-01'></div><div><label>Control end</label><input id='ce' type='date' value='2020-12-31'></div><div><label>Projection start</label><input id='sy' type='number' value='2027'></div><div><label>Projection end</label><input id='ey' type='number' value='2036'></div><div><label>Scenario</label><select id='sc'><option value='baseline'>Baseline</option><option value='dry'>Dry</option><option value='wet'>Wet</option><option value='high_demand'>High demand</option></select></div></div><button class='btn' onclick="runProjection('${id}')">Run analysis</button><div id='note' class='note'></div><div id='pchart' style='height:345px'></div>`);}
async function runProjection(id){const query=new URLSearchParams({control_start:$('cs').value,control_end:$('ce').value,start_year:$('sy').value,end_year:$('ey').value,scenario:$('sc').value});const response=await fetch(`/api/projection/${id}?${query}`);const data=await response.json();if(!response.ok){$('note').innerHTML=`<span style='color:#c43d43'>${esc(data.detail||'Analysis failed')}</span>`;return;}$('note').innerHTML=`<b>Method:</b> ${esc(data.method)}`;Plotly.newPlot('pchart',[{x:data.historical.map(row=>row.period),y:data.historical.map(row=>row.rainfall_mm),type:'scatter',mode:'lines',name:'Historical rainfall'},{x:data.historical.map(row=>row.period),y:data.historical.map(row=>row.et0_mm),type:'scatter',mode:'lines',name:'Historical ET0'},{x:data.projection.map(row=>row.period),y:data.projection.map(row=>row.rainfall_mm),type:'scatter',mode:'lines',name:'Projected rainfall'},{x:data.projection.map(row=>row.period),y:data.projection.map(row=>row.et0_mm),type:'scatter',mode:'lines',name:'Projected ET0'},{x:data.projection.map(row=>row.period),y:data.projection.map(row=>row.water_balance_mm),type:'scatter',mode:'lines',name:'Projected water balance',yaxis:'y2'}],{title:`${esc(data.scenario)} scenario`,margin:{l:55,r:55,t:45,b:65},yaxis:{title:'mm/month'},yaxis2:{title:'Water balance',overlaying:'y',side:'right'},legend:{orientation:'h'}},{responsive:true});}
async function nasa(id){activeNode=id;openPanel('NASA POWER Climate','Loading NASA POWER…');const response=await fetch(`/api/nasa/${id}`);const data=await response.json();if(!response.ok){openPanel('NASA POWER Climate',`<p>${esc(data.detail||'Request failed')}</p>`);return;}openPanel('NASA POWER Climate',`<p><b>${esc(data.node.name)}</b></p><p>Available variables: ${esc(Object.keys(data.parameters||{}).join(', '))}</p><p>NASA POWER is an independent climate and agricultural-meteorology comparison source.</p><button class='btn' onclick="control('${id}')">Open control-period analysis</button>`);}
function riskOverview(){if(!live)return;const rows=live.nodes.map(node=>`<tr><td>${esc(node.node.name)}</td><td>${node.forecast.rainfall_mm}</td><td>${node.forecast.water_balance_mm}</td><td>${node.flood.peak_discharge_m3s}</td><td>${esc(node.drought.class)}</td><td>${esc(node.flood.class)}</td><td>${node.composite.score}</td></tr>`).join('');openPanel('Live flood–drought screening',`<p>These are API-based screening outputs. Use verified gauges, official flood mapping and calibrated SWAT for formal decisions.</p><table><thead><tr><th>Node</th><th>Rain</th><th>Balance</th><th>Peak Q</th><th>Drought</th><th>Flood</th><th>Risk</th></tr></thead><tbody>${rows}</tbody></table><button class='btn' onclick="window.open('/download/live-summary.csv','_blank')">Download CSV</button>`);}
async function statusPanel(){const data=await (await fetch('/api/status')).json();const modules=data.modules.map(row=>`<tr><td>${esc(row.module)}</td><td>${esc(row.status)}</td><td>${esc(row.need)}</td></tr>`).join('');const files=data.files.map(row=>`<tr><td>${esc(row.name)}</td><td>${esc(row.group)}</td><td>${row.size_kb} KB</td><td><a href='${row.url}' target='_blank'>Open</a></td></tr>`).join('');openPanel('System status',`<h3>API and model modules</h3><table><thead><tr><th>Module</th><th>Status</th><th>Requirement</th></tr></thead><tbody>${modules}</tbody></table><h3>Files</h3>${files?`<table><thead><tr><th>Name</th><th>Group</th><th>Size</th><th>File</th></tr></thead><tbody>${files}</tbody></table>`:'<p>No files uploaded.</p>'}<p class='note'>${esc(data.warning)}</p>`);}
function upload(){const targets=Object.entries(config.geometries).map(([id,item])=>`<option value='${id}'>Replace ${esc(item.name)}</option>`).join('');openPanel('Upload data',`<p>For permanent core geometry, select the exact target layer. The portal will save it using the correct filename. For GeoJSON, the app validates the file before saving.</p><form id='uploadForm' class='drop'><label>Dataset type</label><select name='dataset_type'><option value='vector'>Vector / GeoJSON</option><option value='station'>Station CSV / Excel</option><option value='swat'>SWAT / SWAT+ output</option><option value='raster'>Raster / GeoTIFF / NetCDF</option><option value='other'>Other</option></select><label>Target portal layer</label><select name='target_layer'><option value='general'>General upload / register only</option>${targets}</select><label>Source / agency</label><input name='source' placeholder='HydroBASINS, LIMCOM, DWS, project output'><label>Description</label><textarea name='description' placeholder='Period, spatial resolution, CRS, validation notes'></textarea><label>Select file</label><input name='file' type='file' required><button class='btn' type='submit'>Upload and register</button></form><div id='uploadNote' class='note'></div>`);$('uploadForm').onsubmit=async event=>{event.preventDefault();const response=await fetch('/api/upload',{method:'POST',body:new FormData(event.target)});const data=await response.json();$('uploadNote').innerHTML=response.ok?`<span style='color:#078a58'>${esc(data.message)}</span>`:`<span style='color:#c43d43'>${esc(data.detail||'Upload failed')}</span>`;if(response.ok&&data.record.target_layer==='basin_boundary'){basinGeojson=null;await autoLoadBoundary();}};}
async function swat(){const data=await (await fetch('/api/swat/summary')).json();const files=data.files.map(row=>`<li>${esc(row.name)} — ${row.size_kb} KB</li>`).join('');openPanel('SWAT / SWAT+ integration',`<p>${esc(data.message)}</p>${files?`<h3>Uploaded SWAT files</h3><ul>${files}</ul>`:'<p>No SWAT files uploaded yet.</p>'}<button class='btn' onclick='upload()'>Upload SWAT outputs</button>`);}
async function methods(){const data=await (await fetch('/api/methods')).json();openPanel(data.title,`<h3>API-first workflow</h3><p>${esc(data.api_first)}</p><h3>Geometry rule</h3><p>${esc(data.geometry)}</p><h3>Raster rule</h3><p>${esc(data.raster)}</p><h3>Projection rule</h3><p>${esc(data.projection)}</p><h3>Storage</h3><p>${esc(data.storage)}</p>`);}
document.addEventListener('DOMContentLoaded',start);
</script>
</body>
</html>'''

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
