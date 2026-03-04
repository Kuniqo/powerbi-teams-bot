# -*- coding: utf-8 -*-
"""
bot.py - Microsoft Teams bot built on the Bot Framework SDK.

Features:
- Welcome message in Spanish when a member joins
- Passes user messages through the AI agent (DAX translation + Power BI execution)
- Shows a typing indicator while processing
- Splits long responses into multiple messages (> 4000 chars)
"""

import logging
from typing import List

from botbuilder.core import ActivityHandler, TurnContext
from botbuilder.schema import Activity, ActivityTypes, ChannelAccount

try:
    from botbuilder.core.teams import TeamsInfo
except ImportError:
    TeamsInfo = None  # type: ignore[assignment, misc]

from ai_agent import AIAgent

logger = logging.getLogger(__name__)

_WELCOME_MESSAGE = (
    "¡Hola! 👋 Soy el **Power BI Assistant** de MyDoctor.\n\n"
    "Puedo ayudarte a consultar datos de Power BI usando lenguaje natural. "
    "Solo hazme una pregunta como:\n\n"
    "- *¿Cuántos pacientes activos hay?*\n"
    "- *¿Cuáles son los pacientes asignados a cada Health Ambassador?*\n"
    "- *¿Cuántos pacientes tienen seguro Devoted?*\n\n"
    "También puedes escribir **datasets** para ver los datos disponibles "
    "o **ayuda** para más información."
)

_HELP_MESSAGE = (
    "**Cómo usar el Power BI Assistant:**\n\n"
    "1. Escribe tu pregunta en lenguaje natural.\n"
    "2. El asistente traduce tu pregunta a DAX y consulta Power BI.\n"
    "3. Recibirás los resultados directamente en el chat.\n\n"
    "**Comandos especiales:**\n"
    "- `datasets` — Muestra los datasets disponibles\n"
    "- `ayuda` — Muestra este mensaje\n\n"
    "**Ejemplos de preguntas:**\n"
    "- ¿Cuántos pacientes hay en total?\n"
    "- Muéstrame los pacientes con seguro Humana\n"
    "- ¿Qué Health Ambassador tiene más pacientes asignados?\n"
    "- Lista los pacientes inactivos\n"
)

_MAX_MESSAGE_LENGTH = 4000


class PowerBIBot(ActivityHandler):
    """Teams bot that answers natural-language Power BI queries."""

    def __init__(self, agent: AIAgent) -> None:
        super().__init__()
        self._agent = agent

    # ── Welcome new members ───────────────────────────────────────────────

    async def on_members_added_activity(
        self, members_added: List[ChannelAccount], turn_context: TurnContext
    ) -> None:
        """Send a welcome message to each newly added member (excluding the bot itself)."""
        bot_id = turn_context.activity.recipient.id
        for member in members_added:
            if member.id != bot_id:
                await turn_context.send_activity(
                    Activity(
                        type=ActivityTypes.message,
                        text=_WELCOME_MESSAGE,
                    )
                )

    # ── Incoming messages ─────────────────────────────────────────────────

    async def on_message_activity(self, turn_context: TurnContext) -> None:
        """Handle every incoming message from a user."""
        user_text = (turn_context.activity.text or "").strip()
        user_id = (
            turn_context.activity.from_property.id
            if turn_context.activity.from_property
            else "default_user"
        )
        # Extract user email from Teams activity (used for dataset access control)
        user_email = await self._get_user_email(turn_context)
        logger.info(
            "Message from user_id=%s, resolved_email=%s, channel=%s",
            user_id, user_email or "(empty)", turn_context.activity.channel_id,
        )

        if not user_text:
            return

        # Handle built-in commands
        lower = user_text.lower()
        if lower in ("ayuda", "help", "/ayuda", "/help"):
            await turn_context.send_activity(
                Activity(type=ActivityTypes.message, text=_HELP_MESSAGE)
            )
            return

        # Show typing indicator to the user
        await self._send_typing(turn_context)

        try:
            response_text = await self._agent.process_message(user_id, user_text, user_email)
        except Exception as exc:  # pylint: disable=broad-except
            logger.exception("Error in AI agent for user %s", user_id)
            response_text = (
                f"❌ Ocurrió un error al procesar tu consulta: {exc}\n\n"
                "Por favor, intenta de nuevo o reformula tu pregunta."
            )

        # Split long responses into chunks ≤ MAX_MESSAGE_LENGTH characters
        chunks = _split_message(response_text, _MAX_MESSAGE_LENGTH)
        for chunk in chunks:
            await turn_context.send_activity(
                Activity(type=ActivityTypes.message, text=chunk)
            )

    # ── Helpers ───────────────────────────────────────────────────────────

    @staticmethod
    async def _get_user_email(turn_context: TurnContext) -> str:
        """Get the user's email/UPN from Teams.
        
        Priority:
        1. TeamsInfo.get_member (most reliable — returns the real UPN/email)
        2. activity.from_property.name (sometimes contains email in Teams)
        3. Empty string (fallback — open access mode)
        """
        # Try Teams roster API first
        if TeamsInfo and turn_context.activity.channel_id == "msteams":
            try:
                member = await TeamsInfo.get_member(
                    turn_context,
                    turn_context.activity.from_property.id,
                )
                # member.user_principal_name is the UPN (email)
                upn = getattr(member, 'user_principal_name', None) or ""
                if upn and '@' in upn:
                    logger.info("Resolved Teams user email via roster: %s", upn)
                    return upn.lower()
                # Fallback to member.email
                email = getattr(member, 'email', None) or ""
                if email and '@' in email:
                    logger.info("Resolved Teams user email via member.email: %s", email)
                    return email.lower()
            except Exception as exc:
                logger.warning("Could not fetch Teams member info: %s", exc)

        # Fallback: check from_property fields
        if turn_context.activity.from_property:
            name = getattr(turn_context.activity.from_property, 'name', '') or ""
            if '@' in name:
                return name.lower()

        logger.warning("Could not determine user email — access control will use open mode")
        return ""

    @staticmethod
    async def _send_typing(turn_context: TurnContext) -> None:
        """Send a typing indicator activity."""
        try:
            await turn_context.send_activity(
                Activity(type=ActivityTypes.typing)
            )
        except Exception:  # pylint: disable=broad-except
            pass  # Typing indicator is best-effort; ignore failures


# ── Utility ──────────────────────────────────────────────────────────────────


def _split_message(text: str, max_len: int) -> list[str]:
    """
    Split text into chunks of at most max_len characters, breaking on
    newlines where possible to avoid cutting mid-sentence.
    """
    if len(text) <= max_len:
        return [text]

    chunks: list[str] = []
    while text:
        if len(text) <= max_len:
            chunks.append(text)
            break
        # Try to find the last newline within the window
        split_at = text.rfind("\n", 0, max_len)
        if split_at == -1:
            # No newline found — hard cut at max_len
            split_at = max_len
        chunks.append(text[:split_at])
        text = text[split_at:].lstrip("\n")
    return chunks
