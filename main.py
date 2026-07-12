


from __future__ import annotations

"""
Limpopo Hybrid Digital Twin Portal
==================================

Clean deployment-ready main application.

This file:
- preserves the existing FastAPI portal;
- loads verified local GeoJSON layers;
- provides live Open-Meteo monitoring endpoints;
- supports uploads and SWAT-ready files;
- includes the Google Drive raster router from raster_api.py;
- avoids duplicate app creation and invalid plain-text lines.
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
from fastapi import (
    FastAPI,
    File,
    Form,
    HTTPException,
    Query,
    UploadFile,
)
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from raster_api import router as raster_router


APP_TITLE = "Limpopo Hybrid Digital Twin Portal"
APP_VERSION = "7.1.0"

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
VECTOR_DIR = DATA_DIR / "vector"
UPLOAD_DIR = DATA_DIR / "uploads"
SWAT_DIR = DATA_DIR / "swat"
RASTER_DIR = DATA_DIR / "raster"
METADATA_DIR = DATA_DIR / "metadata"
TIMESERIES_DIR = DATA_DIR / "timeseries"
ZONAL_DIR = DATA_DIR / "zonal"

for folder in (
    DATA_DIR,
    VECTOR_DIR,
    UPLOAD_DIR,
    SWAT_DIR,
    RASTER_DIR,
    METADATA_DIR,
    TIMESERIES_DIR,
    ZONAL_DIR,
):
    folder.mkdir(parents=True, exist_ok=True)


app = FastAPI(
    title=APP_TITLE,
    version=APP_VERSION,
)

# Google Drive raster catalogue, metadata, histogram, preview and /rasters page.
app.include_router(raster_router)

# Lightweight repository files can be opened through /files.
app.mount(
    "/files",
    StaticFiles(directory=str(DATA_DIR)),
    name="files",
)


CACHE: dict[str, tuple[float, Any]] = {}
CACHE_TTL_SECONDS = 60 * 60


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


GEOMETRIES: dict[str, dict[str, str]] = {
    "basin_boundary": {
        "name": "Actual Limpopo Basin Boundary",
        "filename": "limpopo_basin_boundary.geojson",
        "env": "BASIN_BOUNDARY_GEOJSON_URL",
        "source": "HydroBASINS / LIMCOM / verified SWAT delineation",
    },
    "subbasins_level4": {
        "name": "HydroBASINS Level 4 Sub-basins",
        "filename": "limpopo_subbasins_level4.geojson",
        "env": "SUBBASINS_L4_GEOJSON_URL",
        "source": "HydroBASINS Level 4 clipped to Limpopo",
    },
    "subbasins_level6": {
        "name": "HydroBASINS Level 6 Sub-basins",
        "filename": "limpopo_subbasins_level6.geojson",
        "env": "SUBBASINS_L6_GEOJSON_URL",
        "source": "HydroBASINS Level 6 clipped to Limpopo",
    },
    "river_network": {
        "name": "HydroSHEDS / SWAT River Network",
        "filename": "limpopo_river_network.geojson",
        "env": "RIVER_NETWORK_GEOJSON_URL",
        "source": "HydroSHEDS or SWAT reach network",
    },
    "stations": {
        "name": "Official Monitoring Stations",
        "filename": "limpopo_monitoring_stations.geojson",
        "env": "STATIONS_GEOJSON_URL",
        "source": "Verified LIMCOM or national-agency stations",
    },
    "swat_subbasins": {
        "name": "SWAT Delineated Sub-basins",
        "filename": "swat_subbasins.geojson",
        "env": "SWAT_SUBBASINS_GEOJSON_URL",
        "source": "SWAT / SWAT+ delineation",
    },
}


OUTPUTS: list[dict[str, str]] = [
    {
        "id": "composite_risk",
        "name": "Composite risk",
        "source_type": "derived_live",
        "source": "Live climate, flood and drought screening",
    },
    {
        "id": "climate_risk",
        "name": "Climate risk",
        "source_type": "derived_live",
        "source": "Open-Meteo climate screening",
    },
    {
        "id": "flood_risk",
        "name": "Flood risk / discharge",
        "source_type": "derived_live",
        "source": "Open-Meteo Flood API",
    },
    {
        "id": "drought_risk",
        "name": "Drought risk",
        "source_type": "derived_live",
        "source": "Rainfall, ET0 and temperature screening",
    },
    {
        "id": "forecast_rainfall",
        "name": "Forecast rainfall",
        "source_type": "live_grid",
        "source": "Open-Meteo Forecast API",
    },
    {
        "id": "forecast_et0",
        "name": "Forecast ET0",
        "source_type": "live_grid",
        "source": "Open-Meteo Forecast API",
    },
    {
        "id": "forecast_water_balance",
        "name": "Forecast water balance",
        "source_type": "live_grid",
        "source": "Forecast rainfall minus ET0",
    },
    {
        "id": "mean_temperature",
        "name": "Mean temperature",
        "source_type": "live_grid",
        "source": "Open-Meteo Forecast API",
    },
    {
        "id": "apparent_temperature",
        "name": "Apparent temperature",
        "source_type": "live_grid",
        "source": "Open-Meteo Forecast API",
    },
    {
        "id": "wind_speed",
        "name": "Wind speed",
        "source_type": "live_grid",
        "source": "Open-Meteo Forecast API",
    },
    {
        "id": "wind_gust",
        "name": "Wind gust",
        "source_type": "live_grid",
        "source": "Open-Meteo Forecast API",
    },
    {
        "id": "solar_radiation",
        "name": "Solar radiation",
        "source_type": "live_grid",
        "source": "Open-Meteo Forecast API",
    },
    {
        "id": "soil_saturation_proxy",
        "name": "Soil saturation proxy",
        "source_type": "live_grid",
        "source": "Open-Meteo soil-moisture screening",
    },
]


def numeric_values(values: list[Any] | None) -> list[float]:
    result: list[float] = []
    for value in values or []:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            converted = float(value)
            if not math.isnan(converted):
                result.append(converted)
    return result


def average(values: list[Any] | None) -> float:
    valid = numeric_values(values)
    return round(sum(valid) / len(valid), 3) if valid else 0.0


def total(values: list[Any] | None) -> float:
    return round(sum(numeric_values(values)), 3)


def maximum(values: list[Any] | None) -> float:
    valid = numeric_values(values)
    return round(max(valid), 3) if valid else 0.0


def bounded(
    value: float,
    lower: float = 0.0,
    upper: float = 100.0,
) -> float:
    return max(lower, min(upper, value))


def risk_label(score: float) -> str:
    if score >= 80:
        return "Very high"
    if score >= 60:
        return "High"
    if score >= 35:
        return "Moderate"
    return "Low"


def cache_key(url: str, parameters: dict[str, Any]) -> str:
    return url + "?" + "&".join(
        f"{key}={parameters[key]}"
        for key in sorted(parameters)
    )


async def get_json(
    client: httpx.AsyncClient,
    url: str,
    parameters: dict[str, Any],
    ttl: int = CACHE_TTL_SECONDS,
) -> dict[str, Any]:
    key = cache_key(url, parameters)
    now = time.time()

    if key in CACHE and now - CACHE[key][0] < ttl:
        return CACHE[key][1]

    response = await client.get(
        url,
        params=parameters,
        timeout=90.0,
    )
    response.raise_for_status()

    payload = response.json()
    CACHE[key] = (now, payload)
    return payload


def valid_geojson(payload: Any) -> bool:
    return (
        isinstance(payload, dict)
        and payload.get("type") in {"Feature", "FeatureCollection"}
    )


def indexed_files() -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []

    groups = (
        (VECTOR_DIR, "Vector"),
        (UPLOAD_DIR, "Upload"),
        (SWAT_DIR, "SWAT"),
        (RASTER_DIR, "Raster"),
        (METADATA_DIR, "Metadata"),
        (TIMESERIES_DIR, "Time series"),
        (ZONAL_DIR, "Zonal"),
    )

    for folder, group in groups:
        for file_path in folder.rglob("*"):
            if not file_path.is_file():
                continue

            relative = str(
                file_path.relative_to(DATA_DIR)
            ).replace("\\", "/")

            records.append(
                {
                    "name": file_path.name,
                    "group": group,
                    "path": relative,
                    "size_kb": round(
                        file_path.stat().st_size / 1024,
                        2,
                    ),
                    "url": "/files/" + relative,
                }
            )

    return sorted(
        records,
        key=lambda row: (row["group"], row["name"]),
    )


def module_statuses() -> list[dict[str, str]]:
    return [
        {
            "module": "Open-Meteo Forecast",
            "status": "Active",
            "need": "No key",
        },
        {
            "module": "Open-Meteo Historical",
            "status": "Active",
            "need": "No key",
        },
        {
            "module": "Open-Meteo Flood",
            "status": "Active",
            "need": "No key",
        },
        {
            "module": "Google Drive Raster Store",
            "status": (
                "Configured"
                if os.getenv("GOOGLE_DRIVE_FOLDER_ID")
                else "Configuration required"
            ),
            "need": (
                "Folder ID, Drive API service account and Render secret file"
            ),
        },
        {
            "module": "SWAT / SWAT+",
            "status": "Upload required",
            "need": "Calibrated SWAT outputs",
        },
    ]


async def geometry_payload(
    geometry_id: str,
) -> dict[str, Any]:
    if geometry_id not in GEOMETRIES:
        raise HTTPException(
            status_code=404,
            detail="Unknown geometry layer.",
        )

    definition = GEOMETRIES[geometry_id]
    local_path = VECTOR_DIR / definition["filename"]

    if local_path.exists():
        try:
            payload = json.loads(
                local_path.read_text(
                    encoding="utf-8-sig",
                )
            )

            if not valid_geojson(payload):
                raise ValueError(
                    "Expected Feature or FeatureCollection."
                )

            return {
                "available": True,
                "origin": "Repository GeoJSON",
                "name": definition["name"],
                "source": definition["source"],
                "geojson": payload,
            }

        except Exception as error:
            return {
                "available": False,
                "name": definition["name"],
                "source": definition["source"],
                "reason": f"Invalid local GeoJSON: {error}",
            }

    remote_url = os.getenv(
        definition["env"],
        "",
    ).strip()

    if remote_url:
        try:
            async with httpx.AsyncClient(
                headers={
                    "User-Agent": "LimpopoDigitalTwin/7.1"
                }
            ) as client:
                payload = await get_json(
                    client,
                    remote_url,
                    {},
                    ttl=86400,
                )

            if not valid_geojson(payload):
                raise ValueError(
                    "Remote URL did not return GeoJSON."
                )

            return {
                "available": True,
                "origin": "Configured online GeoJSON",
                "name": definition["name"],
                "source": definition["source"],
                "geojson": payload,
            }

        except Exception as error:
            return {
                "available": False,
                "name": definition["name"],
                "source": definition["source"],
                "reason": f"Remote GeoJSON failed: {error}",
            }

    return {
        "available": False,
        "name": definition["name"],
        "source": definition["source"],
        "reason": (
            f"Upload data/vector/{definition['filename']} "
            f"or configure {definition['env']}."
        ),
    }


def risk_metrics(
    rainfall: float,
    et0: float,
    maximum_temperature: float,
    peak_discharge: float,
) -> dict[str, float]:
    balance = rainfall - et0

    drought = 0.0
    if balance < -45:
        drought += 52
    elif balance < -20:
        drought += 32
    elif balance < 0:
        drought += 16

    if maximum_temperature >= 38:
        drought += 22

    if rainfall < 10:
        drought += 18

    drought = bounded(drought)

    flood = (
        95.0
        if peak_discharge >= 120
        else 75.0
        if peak_discharge >= 50
        else 50.0
        if peak_discharge >= 20
        else 20.0
    )

    climate = bounded(
        drought * 0.70
        + max(0, maximum_temperature - 25) * 2.0
        + max(0, -balance) * 0.15
    )

    composite = bounded(
        climate * 0.35
        + flood * 0.35
        + drought * 0.30
    )

    return {
        "water_balance": round(balance, 2),
        "drought_score": round(drought, 2),
        "flood_score": round(flood, 2),
        "climate_score": round(climate, 2),
        "composite_score": round(composite, 2),
    }


async def live_location(
    latitude: float,
    longitude: float,
    forecast_days: int = 16,
    flood_days: int = 30,
) -> dict[str, Any]:
    weather_parameters = {
        "latitude": latitude,
        "longitude": longitude,
        "daily": (
            "precipitation_sum,"
            "temperature_2m_max,"
            "temperature_2m_min,"
            "apparent_temperature_max,"
            "et0_fao_evapotranspiration,"
            "wind_speed_10m_max,"
            "wind_gusts_10m_max,"
            "shortwave_radiation_sum,"
            "relative_humidity_2m_mean,"
            "soil_moisture_0_to_7cm_mean"
        ),
        "forecast_days": forecast_days,
        "timezone": "auto",
    }

    flood_parameters = {
        "latitude": latitude,
        "longitude": longitude,
        "daily": "river_discharge",
        "forecast_days": flood_days,
        "timezone": "auto",
    }

    async with httpx.AsyncClient(
        headers={"User-Agent": "LimpopoDigitalTwin/7.1"}
    ) as client:
        weather, flood = await asyncio.gather(
            get_json(
                client,
                "https://api.open-meteo.com/v1/forecast",
                weather_parameters,
            ),
            get_json(
                client,
                "https://flood-api.open-meteo.com/v1/flood",
                flood_parameters,
            ),
            return_exceptions=True,
        )

    weather_daily = (
        weather.get("daily", {})
        if isinstance(weather, dict)
        else {}
    )

    flood_daily = (
        flood.get("daily", {})
        if isinstance(flood, dict)
        else {}
    )

    rainfall = total(
        weather_daily.get("precipitation_sum", [])
    )

    et0 = total(
        weather_daily.get(
            "et0_fao_evapotranspiration",
            [],
        )
    )

    maximum_temperature = maximum(
        weather_daily.get("temperature_2m_max", [])
    )

    minimum_temperature = average(
        weather_daily.get("temperature_2m_min", [])
    )

    peak_discharge = maximum(
        flood_daily.get("river_discharge", [])
    )

    risks = risk_metrics(
        rainfall,
        et0,
        maximum_temperature,
        peak_discharge,
    )

    soil_moisture = average(
        weather_daily.get(
            "soil_moisture_0_to_7cm_mean",
            [],
        )
    )

    soil_proxy = bounded(
        soil_moisture * 100
        if soil_moisture <= 1
        else soil_moisture
    )

    return {
        "lat": latitude,
        "lon": longitude,
        "forecast": {
            "rainfall_mm": rainfall,
            "et0_mm": et0,
            "water_balance_mm": risks["water_balance"],
            "mean_temp_c": round(
                (
                    average(
                        weather_daily.get(
                            "temperature_2m_max",
                            [],
                        )
                    )
                    + minimum_temperature
                )
                / 2,
                2,
            ),
            "max_temp_c": maximum_temperature,
            "apparent_temp_c": maximum(
                weather_daily.get(
                    "apparent_temperature_max",
                    [],
                )
            ),
            "wind_speed_kmh": maximum(
                weather_daily.get(
                    "wind_speed_10m_max",
                    [],
                )
            ),
            "wind_gust_kmh": maximum(
                weather_daily.get(
                    "wind_gusts_10m_max",
                    [],
                )
            ),
            "solar_radiation_mj_m2": total(
                weather_daily.get(
                    "shortwave_radiation_sum",
                    [],
                )
            ),
            "soil_saturation_proxy": soil_proxy,
        },
        "flood": {
            "peak_discharge_m3s": peak_discharge,
            "mean_discharge_m3s": average(
                flood_daily.get("river_discharge", [])
            ),
            "score": risks["flood_score"],
            "class": risk_label(risks["flood_score"]),
        },
        "drought": {
            "score": risks["drought_score"],
            "class": risk_label(risks["drought_score"]),
        },
        "climate": {
            "score": risks["climate_score"],
            "class": risk_label(risks["climate_score"]),
        },
        "composite": {
            "score": risks["composite_score"],
            "class": risk_label(risks["composite_score"]),
        },
        "series": {
            "forecast_dates": weather_daily.get("time", []),
            "rainfall": weather_daily.get(
                "precipitation_sum",
                [],
            ),
            "et0": weather_daily.get(
                "et0_fao_evapotranspiration",
                [],
            ),
            "tmax": weather_daily.get(
                "temperature_2m_max",
                [],
            ),
            "tmin": weather_daily.get(
                "temperature_2m_min",
                [],
            ),
            "flood_dates": flood_daily.get("time", []),
            "discharge": flood_daily.get(
                "river_discharge",
                [],
            ),
        },
    }


async def live_node(
    node_id: str,
    forecast_days: int = 16,
    flood_days: int = 30,
) -> dict[str, Any]:
    if node_id not in NODES:
        raise HTTPException(
            status_code=404,
            detail="Unknown analysis node.",
        )

    node = NODES[node_id]

    result = await live_location(
        node["lat"],
        node["lon"],
        forecast_days,
        flood_days,
    )

    result["node"] = {
        "id": node_id,
        **node,
    }

    return result


@app.get("/", response_class=HTMLResponse)
def home() -> HTMLResponse:
    return HTMLResponse(MAIN_HTML)


@app.get("/health")
def health() -> dict[str, str]:
    return {
        "status": "ok",
        "version": APP_VERSION,
    }


@app.get("/api/config")
def api_config() -> dict[str, Any]:
    return {
        "title": APP_TITLE,
        "version": APP_VERSION,
        "nodes": NODES,
        "outputs": OUTPUTS,
        "geometries": GEOMETRIES,
        "modules": module_statuses(),
    }


@app.get("/api/status")
def api_status() -> dict[str, Any]:
    return {
        "modules": module_statuses(),
        "files": indexed_files(),
        "warning": (
            "Render free-instance storage is temporary. "
            "Google Drive is used for large GeoTIFFs."
        ),
    }


@app.get("/api/geometry/{geometry_id}")
async def api_geometry(
    geometry_id: str,
) -> dict[str, Any]:
    return await geometry_payload(geometry_id)


@app.get("/api/live/basin")
async def api_live_basin(
    forecast_days: int = Query(16, ge=1, le=16),
    flood_days: int = Query(30, ge=1, le=30),
) -> dict[str, Any]:
    responses = await asyncio.gather(
        *(
            live_node(
                node_id,
                forecast_days,
                flood_days,
            )
            for node_id in NODES
        ),
        return_exceptions=True,
    )

    nodes = [
        item
        for item in responses
        if isinstance(item, dict)
    ]

    if not nodes:
        raise HTTPException(
            status_code=502,
            detail="No live API data could be retrieved.",
        )

    return {
        "created_at": datetime.utcnow().isoformat() + "Z",
        "nodes": nodes,
        "summary": {
            "rainfall_mm": average(
                [
                    item["forecast"]["rainfall_mm"]
                    for item in nodes
                ]
            ),
            "et0_mm": average(
                [
                    item["forecast"]["et0_mm"]
                    for item in nodes
                ]
            ),
            "balance_mm": average(
                [
                    item["forecast"]["water_balance_mm"]
                    for item in nodes
                ]
            ),
            "peak_discharge_m3s": average(
                [
                    item["flood"]["peak_discharge_m3s"]
                    for item in nodes
                ]
            ),
            "risk": average(
                [
                    item["composite"]["score"]
                    for item in nodes
                ]
            ),
        },
    }


@app.get("/api/live/node/{node_id}")
async def api_live_node(
    node_id: str,
) -> dict[str, Any]:
    return await live_node(node_id)


@app.post("/api/upload")
async def api_upload(
    file: UploadFile = File(...),
    dataset_type: str = Form(...),
    target_layer: str = Form("general"),
    source: str = Form("User upload"),
    description: str = Form(""),
) -> dict[str, Any]:
    allowed = {
        ".geojson",
        ".json",
        ".zip",
        ".csv",
        ".xlsx",
        ".xls",
        ".tif",
        ".tiff",
        ".nc",
        ".txt",
        ".sqlite",
        ".db",
    }

    original_name = Path(
        file.filename or "uploaded_file"
    ).name

    suffix = Path(original_name).suffix.lower()

    if suffix not in allowed:
        raise HTTPException(
            status_code=400,
            detail="Unsupported file type.",
        )

    payload = await file.read()

    if len(payload) > 100 * 1024 * 1024:
        raise HTTPException(
            status_code=413,
            detail="Prototype upload limit is 100 MB.",
        )

    if target_layer in GEOMETRIES:
        if suffix not in {".geojson", ".json"}:
            raise HTTPException(
                status_code=400,
                detail="Geometry layers require GeoJSON.",
            )

        try:
            parsed = json.loads(
                payload.decode("utf-8-sig")
            )

            if not valid_geojson(parsed):
                raise ValueError(
                    "Expected Feature or FeatureCollection."
                )

        except Exception as error:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid GeoJSON: {error}",
            ) from error

        destination = (
            VECTOR_DIR
            / GEOMETRIES[target_layer]["filename"]
        )
        group = "Vector"

    else:
        if dataset_type.lower() in {"swat", "station"}:
            folder = SWAT_DIR
            group = "SWAT"
        elif dataset_type.lower() in {
            "raster",
            "geotiff",
            "netcdf",
        }:
            folder = RASTER_DIR
            group = "Raster"
        else:
            folder = UPLOAD_DIR
            group = "Upload"

        timestamp = datetime.utcnow().strftime(
            "%Y%m%d_%H%M%S"
        )

        destination = (
            folder / f"{timestamp}_{original_name}"
        )

    destination.write_bytes(payload)

    record = {
        "name": original_name,
        "saved_path": str(
            destination.relative_to(DATA_DIR)
        ).replace("\\", "/"),
        "group": group,
        "dataset_type": dataset_type,
        "target_layer": target_layer,
        "source": source,
        "description": description,
        "size_kb": round(len(payload) / 1024, 2),
        "uploaded_at": datetime.utcnow().isoformat() + "Z",
    }

    register_path = (
        METADATA_DIR / "upload_register.json"
    )

    existing: list[dict[str, Any]] = []

    if register_path.exists():
        try:
            existing = json.loads(
                register_path.read_text(
                    encoding="utf-8",
                )
            )
        except Exception:
            existing = []

    existing.append(record)

    register_path.write_text(
        json.dumps(existing, indent=2),
        encoding="utf-8",
    )

    return {
        "message": "Upload completed.",
        "record": record,
    }


@app.get("/api/uploads")
def api_uploads() -> dict[str, Any]:
    return {"files": indexed_files()}


@app.get("/api/swat/summary")
def api_swat_summary() -> dict[str, Any]:
    files = [
        item
        for item in indexed_files()
        if item["group"] == "SWAT"
    ]

    return {
        "files": files,
        "message": (
            "Upload calibrated SWAT or SWAT+ "
            "reach, sub-basin, HRU and reservoir outputs."
        ),
    }


@app.get("/download/live-summary.csv")
async def download_live_summary() -> StreamingResponse:
    live = await api_live_basin()

    rows = []

    for item in live["nodes"]:
        rows.append(
            {
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
            }
        )

    stream = io.StringIO()

    writer = csv.DictWriter(
        stream,
        fieldnames=list(rows[0]),
    )

    writer.writeheader()
    writer.writerows(rows)

    return StreamingResponse(
        iter([stream.getvalue()]),
        media_type="text/csv",
        headers={
            "Content-Disposition": (
                'attachment; filename="limpopo_live_summary.csv"'
            )
        },
    )


@app.get("/api/methods")
def api_methods() -> dict[str, str]:
    return {
        "title": "Methods and source rules",
        "api_first": (
            "Use APIs for current screening and verified "
            "observations for formal analysis."
        ),
        "geometry": (
            "Only valid uploaded or configured GeoJSON "
            "geometry is displayed."
        ),
        "raster": (
            "GEE GeoTIFFs remain in Google Drive and are "
            "downloaded, cached, mosaicked and basin-masked "
            "through the raster API."
        ),
        "storage": (
            "Large rasters are not stored in GitHub."
        ),
    }


MAIN_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Limpopo Hybrid Digital Twin Portal</title>

<link
  rel="stylesheet"
  href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
>

<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>

<style>
:root{
  --navy:#101b2d;
  --blue:#1483a8;
  --line:#d7e0e8;
  --bg:#eef2f5;
  --text:#172033;
  --muted:#667085;
}
*{box-sizing:border-box}
body{
  margin:0;
  font-family:Arial,Helvetica,sans-serif;
  background:var(--bg);
  color:var(--text);
}
header{
  height:78px;
  background:var(--navy);
  color:white;
  display:flex;
  align-items:center;
  padding:12px 18px;
  gap:10px;
}
header .title{flex:1}
header h1{margin:0;font-size:21px}
header p{margin:5px 0 0;font-size:12px;color:#cbd5e1}
header a{
  color:white;
  text-decoration:none;
  border:1px solid #dbe5ef;
  border-radius:4px;
  padding:8px 10px;
  font-size:12px;
  font-weight:bold;
}
.app{
  display:grid;
  grid-template-columns:320px 1fr;
  height:calc(100vh - 78px);
}
aside{
  background:white;
  border-right:1px solid var(--line);
  overflow:auto;
  padding:14px;
}
aside h2{font-size:15px;margin:0 0 10px}
button,select{
  width:100%;
  padding:9px;
  border:1px solid var(--line);
  border-radius:4px;
  margin:5px 0;
}
button{
  border:0;
  background:var(--blue);
  color:white;
  font-weight:bold;
  cursor:pointer;
}
.status{
  margin-top:12px;
  padding:10px;
  background:#eef6f8;
  font-size:12px;
}
#map{height:100%;width:100%}
.panel{
  position:absolute;
  z-index:800;
  top:95px;
  right:18px;
  width:430px;
  max-height:75vh;
  overflow:auto;
  background:white;
  box-shadow:0 8px 30px #0003;
  padding:14px;
  display:none;
}
.panel.show{display:block}
.card-grid{
  display:grid;
  grid-template-columns:1fr 1fr;
  gap:8px;
}
.card{
  padding:9px;
  background:#f2f5f7;
}
.card small{
  display:block;
  color:var(--muted);
}
@media(max-width:850px){
  .app{grid-template-columns:1fr}
  aside{display:none}
}
</style>
</head>

<body>
<header>
  <div class="title">
    <h1>Limpopo Hybrid Digital Twin Portal</h1>
    <p>
      Live APIs • verified geometry • Google Drive GEE rasters
      • SWAT-ready integration
    </p>
  </div>
  <a href="/rasters">GEE Raster Explorer</a>
  <a href="/download/live-summary.csv">Live CSV</a>
</header>

<div class="app">
  <aside>
    <h2>Geometry</h2>
    <button onclick="toggleGeometry('basin_boundary')">
      Basin boundary
    </button>
    <button onclick="toggleGeometry('subbasins_level4')">
      Level-4 sub-basins
    </button>
    <button onclick="toggleGeometry('subbasins_level6')">
      Level-6 sub-basins
    </button>
    <button onclick="toggleGeometry('river_network')">
      River network
    </button>

    <h2 style="margin-top:18px">Analysis</h2>
    <button onclick="loadLive()">Refresh live monitoring</button>
    <button onclick="openRasterExplorer()">
      Open GEE raster explorer
    </button>
    <button onclick="showStatus()">System status</button>

    <div id="status" class="status">
      Starting portal…
    </div>
  </aside>

  <main style="position:relative">
    <div id="map"></div>

    <section id="panel" class="panel">
      <button onclick="closePanel()">Close</button>
      <h3 id="panelTitle">Information</h3>
      <div id="panelBody"></div>
    </section>
  </main>
</div>

<script>
let map;
let markers;
let geometryLayers = {};

function esc(value){
  return String(value ?? "").replace(
    /[&<>"']/g,
    character => ({
      "&":"&amp;",
      "<":"&lt;",
      ">":"&gt;",
      '"':"&quot;",
      "'":"&#039;"
    })[character]
  );
}

function openPanel(title,html){
  document.getElementById("panelTitle").textContent=title;
  document.getElementById("panelBody").innerHTML=html;
  document.getElementById("panel").classList.add("show");
}

function closePanel(){
  document.getElementById("panel").classList.remove("show");
}

function riskColour(score){
  if(score>=80)return "#c43d43";
  if(score>=60)return "#e87922";
  if(score>=35)return "#d7aa10";
  return "#078a58";
}

function initMap(){
  map=L.map("map").setView([-23.7,30.1],6);

  L.tileLayer(
    "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
    {attribution:"© OpenStreetMap contributors"}
  ).addTo(map);

  markers=L.featureGroup().addTo(map);
}

async function loadLive(){
  const status=document.getElementById("status");
  status.textContent="Loading live API data…";

  try{
    const response=await fetch("/api/live/basin");
    const data=await response.json();

    if(!response.ok){
      throw new Error(data.detail||"Live request failed.");
    }

    markers.clearLayers();

    data.nodes.forEach(item=>{
      L.circleMarker(
        [item.node.lat,item.node.lon],
        {
          radius:7+item.composite.score/14,
          color:"#172033",
          weight:2,
          fillColor:riskColour(item.composite.score),
          fillOpacity:.85
        }
      )
      .bindPopup(
        `<b>${esc(item.node.name)}</b><br>`+
        `${esc(item.node.country)}<hr>`+
        `<b>Rainfall:</b> ${item.forecast.rainfall_mm} mm<br>`+
        `<b>ET0:</b> ${item.forecast.et0_mm} mm<br>`+
        `<b>Water balance:</b> ${item.forecast.water_balance_mm} mm<br>`+
        `<b>Peak discharge:</b> ${item.flood.peak_discharge_m3s} m³/s<br>`+
        `<b>Composite risk:</b> ${esc(item.composite.class)}`
      )
      .addTo(markers);
    });

    if(markers.getBounds().isValid()){
      map.fitBounds(markers.getBounds(),{padding:[30,30]});
    }

    status.innerHTML=
      `<b>Live APIs active.</b><br>`+
      `Mean rainfall: ${data.summary.rainfall_mm} mm<br>`+
      `Mean ET0: ${data.summary.et0_mm} mm<br>`+
      `Mean balance: ${data.summary.balance_mm} mm`;

  }catch(error){
    status.innerHTML=
      `<b>Live API error:</b><br>${esc(error.message)}`;
  }
}

async function toggleGeometry(id){
  if(geometryLayers[id]){
    map.removeLayer(geometryLayers[id]);
    delete geometryLayers[id];
    return;
  }

  const response=await fetch(`/api/geometry/${id}`);
  const data=await response.json();

  if(!data.available){
    openPanel(
      data.name,
      `<p>${esc(data.reason)}</p>`+
      `<p><b>Expected source:</b> ${esc(data.source)}</p>`
    );
    return;
  }

  const style={
    basin_boundary:{
      color:"#101b2d",
      weight:4,
      fill:false
    },
    subbasins_level4:{
      color:"#2563eb",
      weight:2,
      fillColor:"#60a5fa",
      fillOpacity:.06
    },
    subbasins_level6:{
      color:"#0d9488",
      weight:1,
      fillColor:"#5eead4",
      fillOpacity:.04
    },
    river_network:{
      color:"#0284c7",
      weight:2
    }
  }[id]||{color:"#334155"};

  const layer=L.geoJSON(
    data.geojson,
    {
      style:()=>style,
      onEachFeature:(feature,mapLayer)=>{
        const properties=feature.properties||{};
        const content=Object.entries(properties)
          .slice(0,12)
          .map(([key,value])=>
            `<b>${esc(key)}:</b> ${esc(value)}`
          )
          .join("<br>");

        if(content){
          mapLayer.bindPopup(content);
        }
      }
    }
  ).addTo(map);

  geometryLayers[id]=layer;

  if(layer.getBounds().isValid()){
    map.fitBounds(layer.getBounds(),{padding:[20,20]});
  }
}

async function showStatus(){
  const response=await fetch("/api/status");
  const data=await response.json();

  const modules=data.modules.map(item=>
    `<tr>`+
    `<td>${esc(item.module)}</td>`+
    `<td>${esc(item.status)}</td>`+
    `<td>${esc(item.need)}</td>`+
    `</tr>`
  ).join("");

  openPanel(
    "System status",
    `<table style="width:100%;border-collapse:collapse">`+
    `<thead><tr><th>Module</th><th>Status</th><th>Need</th></tr></thead>`+
    `<tbody>${modules}</tbody></table>`+
    `<p>${esc(data.warning)}</p>`
  );
}

function openRasterExplorer(){
  window.location.href="/rasters";
}

document.addEventListener(
  "DOMContentLoaded",
  async ()=>{
    initMap();
    await loadLive();
    await toggleGeometry("basin_boundary");
  }
);
</script>
</body>
</html>"""


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
    )
