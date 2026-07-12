from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any

import numpy as np
import rasterio
from matplotlib import colormaps
from PIL import Image
from rasterio.mask import mask
from rasterio.warp import transform_bounds, transform_geom

def load_geojson(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))

def extract_geometries(payload: dict[str, Any]) -> list[dict[str, Any]]:
    kind = payload.get("type")
    if kind == "FeatureCollection":
        return [
            feature["geometry"]
            for feature in payload.get("features", [])
            if feature.get("geometry")
        ]
    if kind == "Feature":
        return [payload["geometry"]]
    raise ValueError("Expected a GeoJSON Feature or FeatureCollection.")

def read_basin_masked_raster(raster_path: Path, basin_path: Path):
    if not raster_path.exists():
        raise FileNotFoundError(f"Raster not found: {raster_path}")
    if not basin_path.exists():
        raise FileNotFoundError(f"Basin boundary not found: {basin_path}")

    basin_payload = load_geojson(basin_path)
    basin_geometries = extract_geometries(basin_payload)

    with rasterio.open(raster_path) as source:
        if source.crs is None:
            raise ValueError("Raster does not contain a CRS.")

        projected = [
            transform_geom(
                "EPSG:4326",
                source.crs,
                geometry,
                precision=7,
            )
            for geometry in basin_geometries
        ]

        nodata = source.nodata if source.nodata is not None else -9999

        data, output_transform = mask(
            source,
            projected,
            crop=True,
            filled=True,
            nodata=nodata,
            all_touched=False,
        )

        array = data[0].astype("float64")
        valid_mask = np.isfinite(array) & (array != nodata)

        native_bounds = rasterio.transform.array_bounds(
            array.shape[0],
            array.shape[1],
            output_transform,
        )
        wgs84_bounds = transform_bounds(
            source.crs,
            "EPSG:4326",
            *native_bounds,
            densify_pts=21,
        )

        metadata = {
            "crs": str(source.crs),
            "source_width": source.width,
            "source_height": source.height,
            "resolution_x": abs(source.transform.a),
            "resolution_y": abs(source.transform.e),
            "nodata": nodata,
            "dtype": source.dtypes[0],
        }

    return array, valid_mask, wgs84_bounds, metadata

def calculate_statistics(array: np.ndarray, valid_mask: np.ndarray):
    values = array[valid_mask]
    if values.size == 0:
        raise ValueError("No valid pixels occur inside the basin.")

    percentiles = np.nanpercentile(
        values,
        [2, 5, 10, 25, 50, 75, 90, 95, 98],
    )

    return {
        "count": int(values.size),
        "minimum": float(np.nanmin(values)),
        "p02": float(percentiles[0]),
        "p05": float(percentiles[1]),
        "p10": float(percentiles[2]),
        "p25": float(percentiles[3]),
        "median": float(percentiles[4]),
        "mean": float(np.nanmean(values)),
        "p75": float(percentiles[5]),
        "p90": float(percentiles[6]),
        "p95": float(percentiles[7]),
        "p98": float(percentiles[8]),
        "maximum": float(np.nanmax(values)),
        "standard_deviation": float(np.nanstd(values)),
    }

def calculate_histogram(
    array: np.ndarray,
    valid_mask: np.ndarray,
    bins: int = 30,
):
    values = array[valid_mask]
    counts, edges = np.histogram(values, bins=bins)
    centres = (edges[:-1] + edges[1:]) / 2
    return {
        "centres": centres.tolist(),
        "counts": counts.astype(int).tolist(),
        "edges": edges.tolist(),
    }

def create_transparent_png(
    raster_path: Path,
    basin_path: Path,
    palette: str,
    opacity: float = 0.75,
    categorical: bool = False,
    max_dimension: int = 1800,
):
    array, valid_mask, bounds, metadata = read_basin_masked_raster(
        raster_path,
        basin_path,
    )
    stats = calculate_statistics(array, valid_mask)

    if categorical:
        classes = np.unique(array[valid_mask])
        display = np.zeros(array.shape, dtype=float)
        for index, class_value in enumerate(classes):
            display[array == class_value] = index
        normalised = display / max(len(classes) - 1, 1)
    else:
        minimum = stats["p02"]
        maximum = stats["p98"]
        if np.isclose(minimum, maximum):
            minimum = stats["minimum"]
            maximum = stats["maximum"]
        if np.isclose(minimum, maximum):
            maximum = minimum + 1

        normalised = np.clip(
            (array - minimum) / (maximum - minimum),
            0,
            1,
        )

    colour_map = colormaps.get_cmap(palette)
    rgba = colour_map(np.where(valid_mask, normalised, 0))
    rgba[..., 3] = np.where(
        valid_mask,
        np.clip(opacity, 0.0, 1.0),
        0.0,
    )

    image_array = (rgba * 255).astype("uint8")
    image = Image.fromarray(image_array, "RGBA")

    height, width = image_array.shape[:2]
    factor = min(1.0, max_dimension / max(height, width))
    if factor < 1:
        image = image.resize(
            (
                max(1, int(width * factor)),
                max(1, int(height * factor)),
            ),
            Image.Resampling.BILINEAR,
        )

    output = io.BytesIO()
    image.save(output, format="PNG", optimize=True)

    west, south, east, north = bounds
    return {
        "png": output.getvalue(),
        "bounds": [[south, west], [north, east]],
        "statistics": stats,
        "metadata": metadata,
    }
