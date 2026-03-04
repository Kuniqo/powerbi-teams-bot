# -*- coding: utf-8 -*-
"""
app.py - Main entry point for the Power BI Teams Bot.

Sets up:
- aiohttp web application
- CloudAdapter with ConfigurationBotFrameworkAuthentication (SingleTenant)
- AI agent and Power BI client singletons
- APScheduler for periodic permission sync (3x daily)
- POST /api/messages route
"""

import logging
import os
import sys
from http import HTTPStatus
from types import SimpleNamespace

from aiohttp import web
from aiohttp.web import Request, Response
from botbuilder.integration.aiohttp import (
    CloudAdapter,
    ConfigurationBotFrameworkAuthentication,
)
from botbuilder.schema import Activity

from ai_agent import AIAgent
from bot import PowerBIBot
from config import load_config
from powerbi_client import PowerBIClient
from sync_permissions import PermissionSyncer

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# ── Module-level placeholders (populated during startup) ──────────────────────

adapter: CloudAdapter = None  # type: ignore[assignment]
pbi_client: PowerBIClient = None     # type: ignore[assignment]
agent: AIAgent = None                # type: ignore[assignment]
bot: PowerBIBot = None               # type: ignore[assignment]
syncer: PermissionSyncer = None      # type: ignore[assignment]
scheduler = None

# ── Route handler ─────────────────────────────────────────────────────────────


async def messages(req: Request) -> Response:
    """Handle all incoming Bot Framework messages on POST /api/messages."""
    return await adapter.process(req, bot)


# ── Health-check endpoint ─────────────────────────────────────────────────────


async def health(_: Request) -> Response:
    """Simple liveness probe with sync status."""
    import json as _json
    config_path = os.path.join(os.path.dirname(__file__), "datasets_config.json")
    try:
        with open(config_path) as f:
            cfg = _json.load(f)
        ds_count = len(cfg.get("datasets", []))
    except Exception:
        ds_count = -1

    body = _json.dumps({
        "status": "ok",
        "datasets_loaded": ds_count,
        "llm_provider": os.environ.get("LLM_PROVIDER", "openai"),
    })
    return Response(text=body, content_type="application/json")


# ── Manual sync endpoint ─────────────────────────────────────────────────────


async def trigger_sync(req: Request) -> Response:
    """POST /api/sync — Manually trigger a permission sync."""
    import json as _json
    try:
        updated_config = await syncer.sync_permissions_only()
        agent.reload_config(updated_config)
        ds_count = len(updated_config.get("datasets", []))
        body = _json.dumps({"status": "ok", "datasets_synced": ds_count})
        return Response(text=body, content_type="application/json")
    except Exception as exc:
        logger.exception("Manual sync failed")
        body = _json.dumps({"status": "error", "message": str(exc)})
        return Response(text=body, status=500, content_type="application/json")


# ── Scheduled sync job ────────────────────────────────────────────────────────


async def _run_permission_sync():
    """Background job: sync permissions and reload the agent config."""
    try:
        logger.info("Starting scheduled permission sync...")
        updated_config = await syncer.sync_permissions_only()
        agent.reload_config(updated_config)
        total_users = set()
        for ds in updated_config.get("datasets", []):
            total_users.update(ds.get("access", {}).get("users", []))
        logger.info(
            "Scheduled sync complete: %d datasets, %d unique users",
            len(updated_config.get("datasets", [])),
            len(total_users),
        )
    except Exception as exc:
        logger.exception("Scheduled permission sync failed: %s", exc)


# ── App factory ───────────────────────────────────────────────────────────────


async def on_startup(app: web.Application) -> None:
    """Initialise all services after the event loop is running."""
    global adapter, pbi_client, agent, bot, syncer, scheduler  # noqa: PLW0603

    config = load_config()

    # ── CloudAdapter with ConfigurationBotFrameworkAuthentication ──────────
    # This is the CORRECT way to handle SingleTenant in the Python SDK.
    # BotFrameworkAdapter + BotFrameworkAdapterSettings does NOT support
    # SingleTenant — it always authenticates against botframework.com.
    # CloudAdapter reads APP_TYPE and APP_TENANTID and authenticates
    # against the correct tenant.
    app_type = os.environ.get("MicrosoftAppType",
               os.environ.get("APP_TYPE", "SingleTenant"))
    app_tenant = os.environ.get("MicrosoftAppTenantId",
                 os.environ.get("APP_TENANTID", ""))

    bot_config = SimpleNamespace(
        APP_ID=config.APP_ID,
        APP_PASSWORD=config.APP_PASSWORD,
        APP_TYPE=app_type,
        APP_TENANTID=app_tenant,
    )

    _adapter = CloudAdapter(ConfigurationBotFrameworkAuthentication(bot_config))

    async def on_error(context, error):
        logger.exception("Unhandled error in bot turn: %s", error)
        try:
            await context.send_activity(
                "Lo siento, ocurrió un error inesperado. Por favor, intenta de nuevo."
            )
        except Exception:  # pylint: disable=broad-except
            pass

    _adapter.on_turn_error = on_error
    adapter = _adapter

    logger.info(
        "CloudAdapter configured: APP_TYPE=%s, APP_TENANTID=%s, APP_ID=%s",
        app_type, app_tenant, config.APP_ID,
    )

    # Services
    pbi_client = PowerBIClient(config)
    agent = AIAgent(config, pbi_client)
    bot = PowerBIBot(agent)

    # Permission Syncer
    syncer = PermissionSyncer(
        tenant_id=config.PBI_TENANT_ID,
        client_id=config.PBI_CLIENT_ID,
        client_secret=config.PBI_CLIENT_SECRET,
    )

    # Run initial sync at startup (permissions only — schemas already in config)
    try:
        logger.info("Running initial permission sync at startup...")
        updated_config = await syncer.sync_permissions_only()
        agent.reload_config(updated_config)
    except Exception as exc:
        logger.warning("Initial permission sync failed (will retry later): %s", exc)

    # Schedule periodic sync: 3 times a day (8:00, 13:00, 18:00 UTC by default)
    # Configurable via SYNC_SCHEDULE env var (comma-separated hours)
    try:
        from apscheduler.schedulers.asyncio import AsyncIOScheduler
        from apscheduler.triggers.cron import CronTrigger

        sync_hours = os.environ.get("SYNC_SCHEDULE", "8,13,18")
        scheduler = AsyncIOScheduler()
        scheduler.add_job(
            _run_permission_sync,
            CronTrigger(hour=sync_hours, minute=0),
            id="permission_sync",
            name="Sincronización de permisos Power BI",
            replace_existing=True,
        )
        scheduler.start()
        logger.info("Permission sync scheduled at hours (UTC): %s", sync_hours)
    except ImportError:
        logger.warning(
            "APScheduler not installed — periodic sync disabled. "
            "Install with: pip install apscheduler"
        )
    except Exception as exc:
        logger.warning("Could not start scheduler: %s", exc)

    logger.info("All services initialised successfully.")


async def on_shutdown(app: web.Application) -> None:
    logger.info("Shutting down...")
    if scheduler is not None:
        try:
            scheduler.shutdown(wait=False)
        except Exception:
            pass
    if pbi_client is not None:
        await pbi_client.close()


def create_app() -> web.Application:
    from botbuilder.core.integration import aiohttp_error_middleware
    application = web.Application(middlewares=[aiohttp_error_middleware])
    application.router.add_post("/api/messages", messages)
    application.router.add_get("/health", health)
    application.router.add_post("/api/sync", trigger_sync)
    application.on_startup.append(on_startup)
    application.on_shutdown.append(on_shutdown)
    return application


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    application = create_app()
    logger.info("Starting Power BI Teams Bot on port 3978...")
    web.run_app(application, host="0.0.0.0", port=3978)
