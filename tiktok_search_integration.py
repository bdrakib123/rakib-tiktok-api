import asyncio
import base64
import os
import time
from contextlib import contextmanager
from functools import partial

from fastapi import HTTPException

from tiktoksearch.client import TikTokClient
from tiktoksearch.config import PoolConfig
from tiktoksearch.identity_manager import IdentityStore
from tiktoksearch.pool import ClientPool
from tiktoksearch.filters import SearchQuery
from tiktoksearch.paging import query_hash
from tiktoksearch.api.schemas import SearchRequest, SearchResponse


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "config_direct.yaml")


def _prepare_runtime_config():
    """
    Local:
      Uses config_direct.yaml normally.

    Render:
      If TIKTOK_SEARCH_CONFIG_B64 exists, decode it
      and create config_direct.yaml at runtime.
    """
    encoded = os.getenv("TIKTOK_SEARCH_CONFIG_B64")

    if encoded:
        try:
            config_bytes = base64.b64decode(encoded)

            with open(CONFIG_PATH, "wb") as f:
                f.write(config_bytes)

        except Exception as e:
            raise RuntimeError(
                f"Failed to prepare TikTok search config: {e}"
            )

    if not os.path.exists(CONFIG_PATH):
        raise RuntimeError(
            "TikTok search config not found. "
            "Set TIKTOK_SEARCH_CONFIG_B64 on Render."
        )

    return CONFIG_PATH


def create_search_pool():
    config_path = _prepare_runtime_config()

    config = PoolConfig.load_yaml(config_path)

    identities_path = getattr(
        config,
        "identities_path",
        None
    )

    if identities_path and not os.path.isabs(identities_path):
        identities_path = os.path.join(
            os.path.dirname(config_path),
            identities_path
        )

    identities = (
        IdentityStore(identities_path)
        if identities_path
        else None
    )

    pool = ClientPool(
        config,
        identities=identities
    )

    return pool, config


def query_from_request(req, max_results):
    # Reuse the official search API request conversion
    from tiktoksearch.api.app import _to_query

    return _to_query(
        req,
        max_results
    )


def next_page_token(query, handle, page):
    from tiktoksearch.api.app import _next_page_token

    return _next_page_token(
        query,
        handle,
        page
    )


def register_search(app):

    @app.post(
        "/search",
        response_model=SearchResponse,
        tags=["search"]
    )
    async def search(
        req: SearchRequest
    ):
        pool = getattr(
            app.state,
            "search_pool",
            None
        )

        config = getattr(
            app.state,
            "search_config",
            None
        )

        if pool is None or config is None:
            raise HTTPException(
                status_code=503,
                detail="TikTok search engine is not ready"
            )

        query = query_from_request(
            req,
            config.max_results_per_search
        )

        started = time.monotonic()

        loop = asyncio.get_running_loop()

        fan_out = (
            req.fan_out
            if req.fan_out is not None
            else config.default_fan_out
        )

        if query.page_token is not None:
            fan_out = 1

        next_token = None

        try:

            if fan_out > 1:

                devices, page = await loop.run_in_executor(
                    None,
                    pool.run_merged,
                    query,
                    fan_out
                )

                device = "+".join(devices)

            else:

                handle = (
                    query.page_token.device_handle
                    if query.page_token is not None
                    else None
                )

                served, page = await loop.run_in_executor(
                    None,
                    partial(
                        pool.run,
                        query,
                        handle=handle
                    )
                )

                device = served.label

                next_token = next_page_token(
                    query,
                    served.handle,
                    page
                )

        except Exception as e:

            raise HTTPException(
                status_code=502,
                detail={
                    "error": "TikTok search failed",
                    "message": str(e)
                }
            )

        return SearchResponse(
            query=query.term,
            type=req.type,
            device=device,
            count=len(page.records),
            cursor=page.cursor,
            next_cursor=page.next_cursor,
            page_token=next_token,
            has_more=page.has_more,
            elapsed_s=round(
                time.monotonic() - started,
                2
            ),
            results=page.records
        )
