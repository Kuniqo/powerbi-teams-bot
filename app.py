# -*- coding: utf-8 -*-
"""
app.py - Main entry point for the Power BI Teams Bot.

Sets up:
- aiohttp web application
- Bot Framework adapter with error handler
- AI agent and Power BI client singletons
- POST /api/messages route
"""

import logging
import sys
from http import HTTPStatus

from aiohttp import web
from aiohttp.web import Request, Response
from botbuilder.core import BotFrameworkAdapterSettings, BotFrameworkAdapter
from botbuilder.core.integration import aiohttp_error_middleware
from botbuilder.schema import Activity

from ai_agent import AIAgent
from bot import PowerBIBot
from config import load_config
from powerbi_client import PowerBIClient

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# ── Module-level placeholders (populated during startup) ──────────────────────

adapter: BotFrameworkAdapter = None  # type: ignore[assignment]
pbi_client: PowerBIClient = None     # type: ignore[assignment]
agent: AIAgent = None                # type: ignore[assignment]
bot: PowerBIBot = None               # type: ignore[assignment]

# ── Route handler ─────────────────────────────────────────────────────────────


async def messages(req: Request) -> Response:
    """Handle all incoming Bot Framework messages on POST /api/messages."""
    if "application/json" not in req.content_type:
        return Response(status=HTTPStatus.UNSUPPORTED_MEDIA_TYPE)

    body = await req.json()
    activity = Activity().deserialize(body)

    auth_header = req.headers.get("Authorization", "")

    response = await adapter.process_activity(activity, auth_header, bot.on_turn)
    if response:
        return Response(
            status=response.status,
            body=response.body,
            content_type="application/json",
        )
    return Response(status=HTTPStatus.OK)


# ── Health-check endpoint ─────────────────────────────────────────────────────


async def health(_: Request) -> Response:
    """Simple liveness probe."""
    return Response(text='{"status":"ok"}', content_type="application/json")


# ── App factory ───────────────────────────────────────────────────────────────


async def on_startup(app: web.Application) -> None:
    """Initialise all services after the event loop is running."""
    global adapter, pbi_client, agent, bot  # noqa: PLW0603

    config = load_config()

    # Bot Framework adapter
    _adapter = BotFrameworkAdapter(
        BotFrameworkAdapterSettings(
            app_id=config.APP_ID,
            app_password=config.APP_PASSWORD,
        )
    )

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

    # Services
    pbi_client = PowerBIClient(config)
    agent = AIAgent(config, pbi_client)
    bot = PowerBIBot(agent)

    logger.info("All services initialised successfully.")


async def on_shutdown(app: web.Application) -> None:
    logger.info("Shutting down — closing Power BI HTTP client...")
    if pbi_client is not None:
        await pbi_client.close()


def create_app() -> web.Application:
    application = web.Application(middlewares=[aiohttp_error_middleware])
    application.router.add_post("/api/messages", messages)
    application.router.add_get("/health", health)
    application.on_startup.append(on_startup)
    application.on_shutdown.append(on_shutdown)
    return application


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    application = create_app()
    logger.info("Starting Power BI Teams Bot on port 3978...")
    web.run_app(application, host="0.0.0.0", port=3978)
