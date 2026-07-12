from __future__ import annotations

import os
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

import rasterio
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from rasterio.merge import merge

BASE_DIR = Path(__file__).resolve().parent
DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "").strip()
DRIVE_FOLDER_NAME = os.getenv(
    "GOOGLE_DRIVE_FOLDER_NAME",
    "Limpopo_DigitalTwin_Exports",
).strip()
CREDENTIALS_FILE = Path(
    os.getenv(
        "GOOGLE_DRIVE_CREDENTIALS_FILE",
        "/etc/secrets/google-drive-service-account.json",
    )
)
CACHE_DIR = Path(
    os.getenv(
        "RASTER_CACHE_DIR",
        str(BASE_DIR / "data" / "raster_cache"),
    )
)
CACHE_DIR.mkdir(parents=True, exist_ok=True)
TILE_DIR = CACHE_DIR / "_tiles"
TILE_DIR.mkdir(parents=True, exist_ok=True)
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
GEE_TILE_PATTERN = re.compile(
    r"-\d{10}-\d{10}(?=\.tiff?$)",
    flags=re.IGNORECASE,
)

def get_drive_service():
    if not CREDENTIALS_FILE.exists():
        raise FileNotFoundError(
            f"Google Drive credential file was not found: {CREDENTIALS_FILE}"
        )
    credentials = service_account.Credentials.from_service_account_file(
        CREDENTIALS_FILE,
        scopes=SCOPES,
    )
    return build(
        "drive",
        "v3",
        credentials=credentials,
        cache_discovery=False,
    )

def resolve_folder_id() -> str:
    if DRIVE_FOLDER_ID:
        return DRIVE_FOLDER_ID
    if not DRIVE_FOLDER_NAME:
        raise ValueError(
            "Configure GOOGLE_DRIVE_FOLDER_ID or GOOGLE_DRIVE_FOLDER_NAME."
        )
    service = get_drive_service()
    escaped_name = DRIVE_FOLDER_NAME.replace("'", "\\'")
    query = (
        "mimeType = 'application/vnd.google-apps.folder' "
        f"and name = '{escaped_name}' and trashed = false"
    )
    response = service.files().list(
        q=query,
        spaces="drive",
        fields="files(id,name,modifiedTime)",
        pageSize=100,
    ).execute()
    folders = response.get("files", [])
    if not folders:
        raise FileNotFoundError(
            f"Google Drive folder was not found: {DRIVE_FOLDER_NAME}"
        )
    folders.sort(
        key=lambda item: item.get("modifiedTime", ""),
        reverse=True,
    )
    return folders[0]["id"]

def logical_raster_name(filename: str) -> str:
    return GEE_TILE_PATTERN.sub("", Path(filename).name)

def list_drive_rasters() -> list[dict[str, Any]]:
    folder_id = resolve_folder_id()
    service = get_drive_service()
    query = f"'{folder_id}' in parents and trashed = false"
    files: list[dict[str, Any]] = []
    page_token = None
    while True:
        response = service.files().list(
            q=query,
            spaces="drive",
            fields=(
                "nextPageToken,"
                "files(id,name,mimeType,size,modifiedTime,md5Checksum)"
            ),
            pageToken=page_token,
            pageSize=1000,
        ).execute()
        for item in response.get("files", []):
            name = item.get("name", "")
            if name.lower().endswith((".tif", ".tiff")):
                files.append(
                    {
                        "id": item["id"],
                        "name": name,
                        "logical_name": logical_raster_name(name),
                        "size": int(item.get("size", 0) or 0),
                        "modified_time": item.get("modifiedTime"),
                        "md5": item.get("md5Checksum"),
                    }
                )
        page_token = response.get("nextPageToken")
        if not page_token:
            break
    return files

def grouped_drive_rasters() -> dict[str, list[dict[str, Any]]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in list_drive_rasters():
        groups[item["logical_name"]].append(item)
    return dict(groups)

def download_drive_file(file_id: str, destination: Path) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".part")
    service = get_drive_service()
    request = service.files().get_media(fileId=file_id)
    with temporary.open("wb") as output:
        downloader = MediaIoBaseDownload(
            output,
            request,
            chunksize=16 * 1024 * 1024,
        )
        completed = False
        while not completed:
            status, completed = downloader.next_chunk()
            if status:
                print(
                    f"Downloading {destination.name}: "
                    f"{status.progress() * 100:.1f}%"
                )
    temporary.replace(destination)
    return destination

def mosaic_tiles(tile_paths: list[Path], output_path: Path) -> Path:
    if not tile_paths:
        raise ValueError("No raster tiles were supplied.")
    sources = [rasterio.open(path) for path in tile_paths]
    try:
        mosaic_array, mosaic_transform = merge(sources)
        profile = sources[0].profile.copy()
        profile.update(
            driver="GTiff",
            height=mosaic_array.shape[1],
            width=mosaic_array.shape[2],
            transform=mosaic_transform,
            compress="deflate",
            tiled=True,
            BIGTIFF="IF_SAFER",
        )
        temporary = output_path.with_suffix(output_path.suffix + ".part")
        with rasterio.open(temporary, "w", **profile) as destination:
            destination.write(mosaic_array)
        temporary.replace(output_path)
    finally:
        for source in sources:
            source.close()
    return output_path

def ensure_raster_available(logical_filename: str) -> Path:
    logical_filename = logical_raster_name(logical_filename)
    destination = CACHE_DIR / logical_filename
    if destination.exists() and destination.stat().st_size > 0:
        return destination

    groups = grouped_drive_rasters()
    files = groups.get(logical_filename)
    if not files:
        raise FileNotFoundError(
            f"Raster was not found in Google Drive: {logical_filename}"
        )

    if len(files) == 1:
        return download_drive_file(files[0]["id"], destination)

    layer_tile_dir = TILE_DIR / Path(logical_filename).stem
    layer_tile_dir.mkdir(parents=True, exist_ok=True)
    tile_paths: list[Path] = []
    for item in sorted(files, key=lambda row: row["name"]):
        tile_path = layer_tile_dir / item["name"]
        if not tile_path.exists() or tile_path.stat().st_size == 0:
            download_drive_file(item["id"], tile_path)
        tile_paths.append(tile_path)
    return mosaic_tiles(tile_paths, destination)

def drive_raster_catalogue() -> list[dict[str, Any]]:
    result = []
    for logical_name, files in sorted(grouped_drive_rasters().items()):
        total_size = sum(item["size"] for item in files)
        cached_path = CACHE_DIR / logical_name
        result.append(
            {
                "filename": logical_name,
                "drive_file_count": len(files),
                "drive_size_mb": round(total_size / 1024 / 1024, 2),
                "tiled": len(files) > 1,
                "cached": cached_path.exists(),
                "cached_size_mb": (
                    round(cached_path.stat().st_size / 1024 / 1024, 2)
                    if cached_path.exists()
                    else 0
                ),
                "modified_time": max(
                    (item.get("modified_time") or "" for item in files),
                    default="",
                ),
            }
        )
    return result
