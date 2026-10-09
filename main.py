import json
import re
import os
import tempfile
import subprocess
from urllib.parse import urlparse

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from contextlib import asynccontextmanager

from tiktok_search_integration import (
    create_search_pool,
    register_search
)
from curl_cffi import requests


@asynccontextmanager
async def lifespan(app):
    pool, config = create_search_pool()

    app.state.search_pool = pool
    app.state.search_config = config

    yield


app = FastAPI(
    title="Rakib TikTok API",
    version="2.0.0",
    lifespan=lifespan
)

TIMEOUT = 25

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Linux; Android 13; Mobile) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Mobile Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.tiktok.com/",
}


# --------------------------------------------------
# Helpers
# --------------------------------------------------

def is_tiktok_url(url: str):
    try:
        host = urlparse(url).netloc.lower()
        return (
            host == "tiktok.com"
            or host.endswith(".tiktok.com")
        )
    except Exception:
        return False


def extract_script_json(html: str, script_id: str):
    pattern = (
        rf'<script[^>]+id=["\']{re.escape(script_id)}["\']'
        rf'[^>]*>(.*?)</script>'
    )

    match = re.search(pattern, html, re.S | re.I)

    if not match:
        return None

    raw = match.group(1).strip()

    try:
        return json.loads(raw)
    except Exception:
        return None


def recursive_find(obj, keys):
    """
    Recursively search JSON for any object containing
    one of the requested keys.
    """

    if isinstance(obj, dict):

        # Direct match
        for key in keys:
            if key in obj:
                return obj

        # Recursive
        for value in obj.values():
            result = recursive_find(value, keys)

            if result is not None:
                return result

    elif isinstance(obj, list):

        for item in obj:
            result = recursive_find(item, keys)

            if result is not None:
                return result

    return None


def find_video_object(data):
    if not data:
        return None

    # Common TikTok structure
    if isinstance(data, dict):

        item_info = data.get("ItemModule")

        if isinstance(item_info, dict):

            for value in item_info.values():

                if isinstance(value, dict):
                    if (
                        "video" in value
                        or "videoData" in value
                        or "playAddr" in value
                        or "downloadAddr" in value
                    ):
                        return value

        # New universal structure
        scope = data.get("__DEFAULT_SCOPE__")

        if isinstance(scope, dict):

            video_detail = scope.get(
                "webapp.video-detail"
            )

            if isinstance(video_detail, dict):

                item_info = video_detail.get("itemInfo")

                if isinstance(item_info, dict):

                    item_struct = item_info.get("itemStruct")

                    if isinstance(item_struct, dict):
                        return item_struct

    # Generic fallback
    result = recursive_find(
        data,
        [
            "playAddr",
            "downloadAddr",
            "video"
        ]
    )

    return result


def normalize_video(video):
    if not video:
        return None

    video_data = video.get("video")

    if isinstance(video_data, dict):
        source = video_data
    else:
        source = video

    play_addr = (
        source.get("playAddr")
        or source.get("play_addr")
        or source.get("playUrl")
        or source.get("play_url")
    )

    download_addr = (
        source.get("downloadAddr")
        or source.get("download_addr")
        or source.get("downloadUrl")
        or source.get("download_url")
    )

    cover = (
        source.get("cover")
        or source.get("originCover")
        or source.get("dynamicCover")
    )

    author = video.get("author")

    if isinstance(author, dict):
        author_name = (
            author.get("uniqueId")
            or author.get("nickname")
        )
    else:
        author_name = None

    return {
        "id": video.get("id"),
        "desc": video.get("desc"),
        "author": author_name,
        "play_url": play_addr,
        "download_url": download_addr,
        "cover": cover,
        "duration": source.get("duration"),
        "width": source.get("width"),
        "height": source.get("height"),
        "stats": video.get("stats"),
        "music": video.get("music"),
    }


# --------------------------------------------------
# TikTok Fetch
# --------------------------------------------------

def fetch_tiktok(url: str):

    if not is_tiktok_url(url):
        raise HTTPException(
            status_code=400,
            detail="Invalid TikTok URL"
        )

    try:

        session = requests.Session(
            impersonate="chrome131"
        )

        response = session.get(
            url,
            headers=HEADERS,
            timeout=TIMEOUT,
            allow_redirects=True
        )

    except Exception as e:

        raise HTTPException(
            status_code=502,
            detail=f"TikTok request failed: {str(e)}"
        )

    if response.status_code >= 400:

        raise HTTPException(
            status_code=502,
            detail=f"TikTok returned HTTP {response.status_code}"
        )

    html = response.text

    if not html:

        raise HTTPException(
            status_code=502,
            detail="TikTok returned empty page"
        )

    # ----------------------------------------------
    # 1. SIGI_STATE
    # ----------------------------------------------

    data = extract_script_json(
        html,
        "SIGI_STATE"
    )

    video = find_video_object(data)

    if video:
        result = normalize_video(video)

        if result and (
            result.get("play_url")
            or result.get("download_url")
        ):
            return result, response.url

    # ----------------------------------------------
    # 2. UNIVERSAL DATA
    # ----------------------------------------------

    data = extract_script_json(
        html,
        "__UNIVERSAL_DATA_FOR_REHYDRATION__"
    )

    video = find_video_object(data)

    if video:
        result = normalize_video(video)

        if result and (
            result.get("play_url")
            or result.get("download_url")
        ):
            return result, response.url

    # ----------------------------------------------
    # 3. Search raw HTML for JSON-like video object
    # ----------------------------------------------

    for pattern in [
        r'"playAddr"\s*:\s*"([^"]+)"',
        r'"downloadAddr"\s*:\s*"([^"]+)"',
        r'"play_addr"\s*:\s*"([^"]+)"',
        r'"download_addr"\s*:\s*"([^"]+)"',
    ]:

        match = re.search(
            pattern,
            html
        )

        if match:

            url_found = (
                match.group(1)
                .replace("\\u002F", "/")
                .replace("\\/", "/")
                .replace("\\u0026", "&")
            )

            return {
                "id": None,
                "desc": None,
                "author": None,
                "play_url": url_found,
                "download_url": url_found,
                "cover": None,
                "duration": None,
                "width": None,
                "height": None,
                "stats": None,
                "music": None,
            }, response.url

    raise HTTPException(
        status_code=404,
        detail={
            "error": "TikTok video data not found",
            "final_url": response.url,
            "hint": (
                "TikTok returned a page without embedded "
                "video metadata. The video may be blocked, "
                "private, unavailable, or TikTok changed its page structure."
            )
        }
    )


# --------------------------------------------------
# API
# --------------------------------------------------

@app.get("/")
def root():

    return {
        "success": True,
        "name": "Rakib TikTok API",
        "version": "2.0.0",
        "endpoints": [
            "/health",
            "/video",
            "/download",
            "/search"
        ]
    }


@app.get("/health")
def health():

    return {
        "success": True,
        "status": "online"
    }


@app.get("/video")
def video(url: str):

    data, final_url = fetch_tiktok(url)

    return {
        "success": True,
        "original_url": url,
        "final_url": final_url,
        "data": data
    }


@app.get("/download")
def download(url: str):

    if not is_tiktok_url(url):
        raise HTTPException(
            status_code=400,
            detail="Invalid TikTok URL"
        )

    temp_dir = tempfile.mkdtemp(prefix="tiktok_")
    output = os.path.join(temp_dir, "video.%(ext)s")

    try:

        command = [
            "yt-dlp",
            "--no-playlist",
            "--no-warnings",
            "--no-progress",
            "-f",
            "best[ext=mp4]/best",
            "-o",
            output,
            url
        ]

        process = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=120
        )

        if process.returncode != 0:

            error = (
                process.stderr.strip()
                or process.stdout.strip()
                or "yt-dlp download failed"
            )

            raise HTTPException(
                status_code=502,
                detail={
                    "error": "TikTok download failed",
                    "message": error[-2000:]
                }
            )

        files = []

        for name in os.listdir(temp_dir):

            path = os.path.join(
                temp_dir,
                name
            )

            if os.path.isfile(path):
                files.append(path)

        if not files:

            raise HTTPException(
                status_code=502,
                detail="Downloaded file was not created"
            )

        file_path = files[0]

        def stream_file():

            try:

                with open(
                    file_path,
                    "rb"
                ) as f:

                    while True:

                        chunk = f.read(
                            1024 * 1024
                        )

                        if not chunk:
                            break

                        yield chunk

            finally:

                try:
                    os.remove(file_path)
                except Exception:
                    pass

                try:
                    os.rmdir(temp_dir)
                except Exception:
                    pass

        return StreamingResponse(
            stream_file(),
            media_type="video/mp4",
            headers={
                "Content-Disposition":
                    'attachment; filename="tiktok.mp4"'
            }
        )

    except subprocess.TimeoutExpired:

        raise HTTPException(
            status_code=504,
            detail="TikTok download timed out"
        )

    except HTTPException:
        raise

    except Exception as e:

        raise HTTPException(
            status_code=500,
            detail=f"Download error: {str(e)}"
        )


# --------------------------------------------------
# Download Diagnostic
# --------------------------------------------------

@app.get("/debug-download")
def debug_download(url: str):

    data, final_url = fetch_tiktok(url)

    result = {
        "final_url": final_url,
        "urls": {}
    }

    for name in ["download_url", "play_url"]:

        video_url = data.get(name)

        if not video_url:
            result["urls"][name] = {
                "found": False
            }
            continue

        try:

            session = requests.Session(
                impersonate="chrome131"
            )

            r = session.get(
                video_url,
                headers={
                    "User-Agent": HEADERS["User-Agent"],
                    "Accept": "*/*",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Referer": "https://www.tiktok.com/",
                    "Origin": "https://www.tiktok.com",
                },
                timeout=30,
                stream=True,
                allow_redirects=True
            )

            result["urls"][name] = {
                "found": True,
                "status": r.status_code,
                "content_type": r.headers.get("content-type"),
                "content_length": r.headers.get("content-length"),
                "final_url": r.url
            }

            r.close()

        except Exception as e:

            result["urls"][name] = {
                "found": True,
                "error": str(e)
            }

    return result


# --------------------------------------------------
# Search
# --------------------------------------------------






@app.get("/debug-search")
def debug_search(keyword: str = "bangla funny"):
    import re
    import json
    from urllib.parse import unquote
    from curl_cffi import requests as curl_requests

    sess = curl_requests.Session(impersonate="chrome")

    headers = {
        **HEADERS,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.tiktok.com/",
        "Accept-Language": "en-US,en;q=0.9",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
    }

    # Visit homepage first to establish TikTok session
    home = sess.get(
        "https://www.tiktok.com/",
        headers=headers,
        timeout=20
    )

    # Then search page
    r = sess.get(
        "https://www.tiktok.com/search",
        params={
            "q": keyword,
            "t": "0"
        },
        headers=headers,
        timeout=20
    )

    html = r.text
    cookies = sess.cookies.get_dict()

    # Search every possible msToken representation.
    ms_matches = []

    patterns = [
        r'msToken["\']?\s*[:=]\s*["\']([^"\']+)',
        r'"msToken"\s*:\s*"([^"]+)"',
        r"'msToken'\s*:\s*'([^']+)'",
        r'msToken=([^&"\'\\]+)',
        r'msToken%3D([^&"\'\\]+)',
    ]

    for pattern in patterns:
        for m in re.finditer(pattern, html, re.I):
            value = m.group(1)

            if value and value not in ms_matches:
                ms_matches.append(value)

    # Look for token-like strings around "msToken"
    ms_context = []

    for m in list(re.finditer(r'msToken', html, re.I))[:10]:
        a = max(0, m.start() - 500)
        b = min(len(html), m.end() + 1000)

        ms_context.append(html[a:b])

    # Inspect Set-Cookie headers
    set_cookie = []

    try:
        for k, v in r.headers.items():
            if k.lower() == "set-cookie":
                set_cookie.append(v)
    except Exception:
        pass

    # Current session cookies
    safe_cookies = {}

    for k, v in cookies.items():
        if k.lower() in [
            "msToken".lower(),
            "ttwid",
            "tt_csrf_token",
            "tt_chain_token",
            "passport_csrf_token",
            "s_v_web_id"
        ]:
            safe_cookies[k] = (
                v[:40] + "..."
                if len(v) > 40 else v
            )

    # Try adding extracted msToken to session
    if ms_matches and not cookies.get("msToken"):
        try:
            sess.cookies.set(
                "msToken",
                unquote(ms_matches[0]),
                domain=".tiktok.com"
            )
        except Exception:
            pass

    final_cookies = sess.cookies.get_dict()

    # Internal search API
    api_params = {
        "aid": "1988",
        "app_language": "en",
        "app_name": "tiktok_web",
        "keyword": keyword,
        "region": "US",
        "search_source": "normal_search",
        "from_page": "search",
        "device_platform": "web_pc",
        "os": "windows",
        "count": "12",
        "offset": "0",
    }

    api_headers = {
        **HEADERS,
        "Accept": "application/json, text/plain, */*",
        "Referer": f"https://www.tiktok.com/search?q={keyword}",
        "Origin": "https://www.tiktok.com",
        "X-Requested-With": "XMLHttpRequest",
    }

    try:
        api = sess.get(
            "https://www.tiktok.com/api/search/general/full/",
            params=api_params,
            headers=api_headers,
            timeout=20
        )

        return {
            "success": True,
            "page_status": r.status_code,
            "page_length": len(html),

            "cookies": safe_cookies,

            "msToken": {
                "cookie": bool(cookies.get("msToken")),
                "html_found": len(ms_matches),
                "extracted_length": (
                    len(ms_matches[0])
                    if ms_matches else 0
                ),
                "session_after_extract": bool(
                    final_cookies.get("msToken")
                )
            },

            "set_cookie_headers": set_cookie,

            "ms_context": ms_context[:3],

            "internal_api": {
                "status_code": api.status_code,
                "content_type": api.headers.get("content-type"),
                "length": len(api.text),
                "body_start": api.text[:1000]
            }
        }

    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "cookies": safe_cookies,
            "msToken": {
                "cookie": bool(cookies.get("msToken")),
                "html_found": len(ms_matches)
            }
        }

register_search(app)
