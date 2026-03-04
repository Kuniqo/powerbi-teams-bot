# -*- coding: utf-8 -*-
"""
ai_agent.py - AI agent that translates natural language to DAX using
multiple LLM providers (OpenAI, Azure OpenAI, Gemini, Claude, Perplexity)
with function calling and executes queries against Power BI.

Flow:
  1. User message arrives.
  2. Build messages list (system prompt + conversation history + new user msg).
  3. Call the configured LLM with tool definitions.
  4. If tool_calls returned, execute each tool and feed results back.
  5. Repeat up to MAX_ITERATIONS or until a text response is received.
  6. Return the final text response to the caller.
"""

import json
import logging
import os
import threading
from collections import deque
from typing import Any

from openai import AsyncAzureOpenAI, AsyncOpenAI

from config import Config
from powerbi_client import PowerBIClient

logger = logging.getLogger(__name__)

_DATASETS_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "datasets_config.json")
MAX_ITERATIONS = 5
MAX_HISTORY = 20  # messages per user (system msg not counted)

# ── System prompt ────────────────────────────────────────────────────────────

_SYSTEM_PROMPT = """\
Eres un analista de datos experto que ayuda a los usuarios a consultar datos de Power BI.
Cuando un usuario te hace una pregunta sobre datos:
1. Primero usa list_available_datasets para ver qué datos hay disponibles
2. Usa get_dataset_schema para entender la estructura de tablas y columnas
3. Traduce la pregunta del usuario a una query DAX correcta
4. Ejecuta la query con execute_dax_query
5. Presenta los resultados de forma clara y amigable

Reglas DAX importantes:
- Siempre usa EVALUATE para retornar una tabla
- Usa SUMMARIZECOLUMNS para agregaciones con agrupamiento
- Usa CALCULATETABLE o FILTER para tablas filtradas
- Las referencias a columnas usan la sintaxis 'NombreTabla'[NombreColumna]
- Las comparaciones de strings en DAX son case-insensitive por defecto
- Usa TOPN para consultas top-N
- Para contar valores distintos usa DISTINCTCOUNT
- Para contar filas usa COUNTROWS
- Para una fila de resultado singular usa ROW("NombreColumna", expresión)

Responde siempre en español. Si hay un error, explícalo de forma simple.\
"""

# ── Tool definitions (OpenAI format — used by OpenAI, Azure, Gemini, Perplexity) ──

_TOOLS_OPENAI: list[dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "list_available_datasets",
            "description": (
                "Lista todos los datasets de Power BI disponibles con su nombre, "
                "descripción, workspace_id y dataset_id."
            ),
            "parameters": {
                "type": "object",
                "properties": {},
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_dataset_schema",
            "description": (
                "Devuelve el esquema completo (tablas, columnas, relaciones) "
                "de un dataset específico."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "dataset_name": {
                        "type": "string",
                        "description": "El nombre exacto del dataset tal como aparece en list_available_datasets.",
                    }
                },
                "required": ["dataset_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "execute_dax_query",
            "description": (
                "Ejecuta una query DAX contra un dataset de Power BI y devuelve "
                "los resultados formateados como tabla Markdown."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "dataset_name": {
                        "type": "string",
                        "description": "El nombre exacto del dataset a consultar.",
                    },
                    "dax_query": {
                        "type": "string",
                        "description": (
                            "La query DAX completa. Debe comenzar con EVALUATE. "
                            "Ejemplo: EVALUATE SUMMARIZECOLUMNS('Patients'[Status], "
                            "\"Total\", COUNTROWS('Patients'))"
                        ),
                    },
                },
                "required": ["dataset_name", "dax_query"],
            },
        },
    },
]

# ── Tool definitions (Anthropic/Claude format) ───────────────────────────────

_TOOLS_CLAUDE: list[dict[str, Any]] = [
    {
        "name": "list_available_datasets",
        "description": (
            "Lista todos los datasets de Power BI disponibles con su nombre, "
            "descripción, workspace_id y dataset_id."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "get_dataset_schema",
        "description": (
            "Devuelve el esquema completo (tablas, columnas, relaciones) "
            "de un dataset específico."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dataset_name": {
                    "type": "string",
                    "description": "El nombre exacto del dataset tal como aparece en list_available_datasets.",
                }
            },
            "required": ["dataset_name"],
        },
    },
    {
        "name": "execute_dax_query",
        "description": (
            "Ejecuta una query DAX contra un dataset de Power BI y devuelve "
            "los resultados formateados como tabla Markdown."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dataset_name": {
                    "type": "string",
                    "description": "El nombre exacto del dataset a consultar.",
                },
                "dax_query": {
                    "type": "string",
                    "description": (
                        "La query DAX completa. Debe comenzar con EVALUATE. "
                        "Ejemplo: EVALUATE SUMMARIZECOLUMNS('Patients'[Status], "
                        "\"Total\", COUNTROWS('Patients'))"
                    ),
                },
            },
            "required": ["dataset_name", "dax_query"],
        },
    },
]


# ── Agent class ──────────────────────────────────────────────────────────────


class AIAgent:
    """Manages per-user conversation history and drives the function-calling loop."""

    def __init__(self, config: Config, pbi_client: PowerBIClient) -> None:
        self._config = config
        self._pbi = pbi_client
        # Per-user conversation history: user_id → deque of message dicts
        self._histories: dict[str, deque] = {}
        # Load datasets config (thread-safe reloading)
        self._config_lock = threading.Lock()
        self._datasets_config = self._load_datasets_config()
        # Build LLM client(s)
        self._provider = config.LLM_PROVIDER
        self._openai_client = None
        self._claude_client = None
        self._init_llm_client(config)

        logger.info(
            "AIAgent initialized with provider=%s, model=%s",
            self._provider, self._model_name,
        )

    # ── LLM client initialization ─────────────────────────────────────────

    def _init_llm_client(self, config: Config) -> None:
        """Create the appropriate LLM client based on the configured provider."""
        provider = config.LLM_PROVIDER

        if provider == "openai":
            self._openai_client = AsyncOpenAI(api_key=config.OPENAI_API_KEY)

        elif provider == "azure_openai":
            self._openai_client = AsyncAzureOpenAI(
                azure_endpoint=config.AZURE_OPENAI_ENDPOINT,
                api_key=config.AZURE_OPENAI_API_KEY,
                api_version=config.AZURE_OPENAI_API_VERSION,
            )

        elif provider == "gemini":
            # Gemini supports OpenAI-compatible API
            self._openai_client = AsyncOpenAI(
                api_key=config.GEMINI_API_KEY,
                base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
            )

        elif provider == "perplexity":
            # Perplexity supports OpenAI-compatible API
            self._openai_client = AsyncOpenAI(
                api_key=config.PERPLEXITY_API_KEY,
                base_url="https://api.perplexity.ai/",
            )

        elif provider == "claude":
            # Claude uses its own SDK (anthropic)
            try:
                import anthropic
                self._claude_client = anthropic.AsyncAnthropic(
                    api_key=config.CLAUDE_API_KEY,
                )
            except ImportError:
                raise ImportError(
                    "Para usar Claude, instala el SDK: pip install anthropic"
                )

    @property
    def _model_name(self) -> str:
        """Return the model name for the active provider."""
        p = self._provider
        c = self._config
        if p == "openai":
            return c.OPENAI_MODEL
        elif p == "azure_openai":
            return c.AZURE_OPENAI_DEPLOYMENT
        elif p == "gemini":
            return c.GEMINI_MODEL
        elif p == "claude":
            return c.CLAUDE_MODEL
        elif p == "perplexity":
            return c.PERPLEXITY_MODEL
        return "unknown"

    # ── Datasets config helpers ───────────────────────────────────────────

    @staticmethod
    def _load_datasets_config() -> dict[str, Any]:
        try:
            with open(_DATASETS_CONFIG_PATH, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except Exception as exc:  # pylint: disable=broad-except
            logger.error("Could not load datasets_config.json: %s", exc)
            return {"datasets": []}

    def reload_config(self, new_config: dict[str, Any] | None = None) -> None:
        """Reload datasets config from disk or from a provided dict (thread-safe)."""
        with self._config_lock:
            if new_config is not None:
                self._datasets_config = new_config
            else:
                self._datasets_config = self._load_datasets_config()
            count = len(self._datasets_config.get("datasets", []))
            logger.info("Datasets config reloaded: %d datasets", count)

    def _find_dataset(self, name: str, user_email: str = "") -> dict[str, Any] | None:
        for ds in self._datasets_config.get("datasets", []):
            if ds["name"].lower() == name.lower():
                if user_email and not self._user_has_access(ds, user_email):
                    return None
                return ds
        return None

    @staticmethod
    def _user_has_access(dataset: dict, user_email: str) -> bool:
        """Check if a user has access to a dataset based on the access config."""
        access = dataset.get("access")
        if not access:
            return True  # No access config = open access

        mode = access.get("mode", "allowlist")
        users = [u.lower() for u in access.get("users", [])]
        # groups support placeholder — extend with Entra ID group lookup if needed
        # groups = access.get("groups", [])

        if "*" in users:
            return True  # Wildcard = everyone has access

        user_lower = user_email.lower()

        if mode == "allowlist":
            return user_lower in users
        elif mode == "denylist":
            return user_lower not in users

        return True

    def _get_accessible_datasets(self, user_email: str = "") -> list[dict]:
        """Return only datasets the user has access to."""
        all_datasets = self._datasets_config.get("datasets", [])
        if not user_email:
            return all_datasets
        return [ds for ds in all_datasets if self._user_has_access(ds, user_email)]

    # ── Tool execution ─────────────────────────────────────────────────────

    def _tool_list_datasets(self, user_email: str = "") -> str:
        datasets = self._get_accessible_datasets(user_email)
        if not datasets:
            return "No tienes acceso a ningún dataset configurado."
        lines = []
        for ds in datasets:
            lines.append(
                f"- **{ds['name']}** — {ds.get('description', '')}\n"
                f"  workspace_id: `{ds.get('workspace_id', '')}`\n"
                f"  dataset_id:   `{ds.get('dataset_id', '')}`"
            )
        return "\n".join(lines)

    def _tool_get_schema(self, dataset_name: str, user_email: str = "") -> str:
        ds = self._find_dataset(dataset_name, user_email)
        if not ds:
            return f"Dataset '{dataset_name}' no encontrado o no tienes acceso."
        schema = ds.get("schema", {})
        tables = schema.get("tables", [])
        relationships = schema.get("relationships", [])

        lines = [f"### Esquema: {ds['name']}\n"]
        for table in tables:
            lines.append(f"**Tabla: {table['name']}** — {table.get('description', '')}")
            for col in table.get("columns", []):
                lines.append(
                    f"  - `{col['name']}` ({col.get('type', '?')}): {col.get('description', '')}"
                )
            lines.append("")

        if relationships:
            lines.append("**Relaciones:**")
            for rel in relationships:
                lines.append(
                    f"  - {rel['from_table']}[{rel['from_column']}] → "
                    f"{rel['to_table']}[{rel['to_column']}]"
                )

        return "\n".join(lines)

    async def _tool_execute_dax(self, dataset_name: str, dax_query: str, user_email: str = "") -> str:
        ds = self._find_dataset(dataset_name, user_email)
        if not ds:
            return f"Dataset '{dataset_name}' no encontrado en la configuración o no tienes acceso."

        workspace_id = ds.get("workspace_id", "")
        dataset_id = ds.get("dataset_id", "")

        if not workspace_id or not dataset_id:
            return "El dataset no tiene workspace_id o dataset_id configurados."

        try:
            response = await self._pbi.execute_dax_query(
                workspace_id=workspace_id,
                dataset_id=dataset_id,
                dax_query=dax_query,
            )
            formatted = PowerBIClient.format_results(response)
            return formatted
        except Exception as exc:  # pylint: disable=broad-except
            logger.exception("Error executing DAX query")
            return f"Error al ejecutar la query DAX: {exc}"

    async def _dispatch_tool(self, name: str, arguments: str | dict, user_email: str = "") -> str:
        """Parse arguments and call the appropriate tool function."""
        try:
            if isinstance(arguments, str):
                args: dict[str, Any] = json.loads(arguments) if arguments else {}
            else:
                args = arguments or {}
        except json.JSONDecodeError:
            return "Error: argumentos inválidos (JSON mal formado)."

        if name == "list_available_datasets":
            return self._tool_list_datasets(user_email)
        elif name == "get_dataset_schema":
            return self._tool_get_schema(args.get("dataset_name", ""), user_email)
        elif name == "execute_dax_query":
            return await self._tool_execute_dax(
                dataset_name=args.get("dataset_name", ""),
                dax_query=args.get("dax_query", ""),
                user_email=user_email,
            )
        else:
            return f"Herramienta desconocida: {name}"

    # ── History management ─────────────────────────────────────────────────

    def _get_history(self, user_id: str) -> deque:
        if user_id not in self._histories:
            self._histories[user_id] = deque(maxlen=MAX_HISTORY)
        return self._histories[user_id]

    def _append_history(self, user_id: str, message: dict) -> None:
        self._get_history(user_id).append(message)

    def clear_history(self, user_id: str) -> None:
        """Clear the conversation history for a user."""
        if user_id in self._histories:
            self._histories[user_id].clear()

    # ── Main entry point ───────────────────────────────────────────────────

    async def process_message(self, user_id: str, user_text: str, user_email: str = "") -> str:
        """
        Translate a user message to a DAX query, execute it, and return a
        human-friendly Spanish response.
        """
        if self._provider == "claude":
            return await self._process_message_claude(user_id, user_text, user_email)
        else:
            return await self._process_message_openai(user_id, user_text, user_email)

    # ── OpenAI-compatible flow (OpenAI, Azure, Gemini, Perplexity) ────────

    async def _process_message_openai(self, user_id: str, user_text: str, user_email: str = "") -> str:
        """Function-calling loop using OpenAI-compatible API."""
        history = self._get_history(user_id)
        self._append_history(user_id, {"role": "user", "content": user_text})

        messages = [{"role": "system", "content": _SYSTEM_PROMPT}] + list(history)

        for iteration in range(MAX_ITERATIONS):
            logger.debug(
                "[%s] LLM call iteration %d for user %s",
                self._provider, iteration + 1, user_id,
            )

            response = await self._openai_client.chat.completions.create(
                model=self._model_name,
                messages=messages,
                tools=_TOOLS_OPENAI,
                tool_choice="auto",
                temperature=0,
            )

            choice = response.choices[0]
            message = choice.message

            # If the model returned a text response, we're done
            if choice.finish_reason == "stop" or (
                not message.tool_calls and message.content
            ):
                final_text = message.content or ""
                self._append_history(user_id, {"role": "assistant", "content": final_text})
                return final_text

            # Handle tool calls
            if message.tool_calls:
                messages.append(message.model_dump(exclude_unset=True))

                for tool_call in message.tool_calls:
                    tool_result = await self._dispatch_tool(
                        name=tool_call.function.name,
                        arguments=tool_call.function.arguments,
                        user_email=user_email,
                    )
                    logger.debug(
                        "Tool '%s' result (truncated): %s",
                        tool_call.function.name,
                        tool_result[:200],
                    )
                    messages.append(
                        {
                            "role": "tool",
                            "tool_call_id": tool_call.id,
                            "content": tool_result,
                        }
                    )
                continue

            logger.warning("Unexpected finish_reason: %s", choice.finish_reason)
            break

        fallback = (
            "Lo siento, no pude completar la consulta en el número máximo de pasos. "
            "Por favor, intenta reformular tu pregunta."
        )
        self._append_history(user_id, {"role": "assistant", "content": fallback})
        return fallback

    # ── Claude/Anthropic flow ─────────────────────────────────────────────

    async def _process_message_claude(self, user_id: str, user_text: str, user_email: str = "") -> str:
        """Function-calling loop using the Anthropic SDK."""
        history = self._get_history(user_id)
        self._append_history(user_id, {"role": "user", "content": user_text})

        # Claude uses a separate 'system' parameter (not in messages list)
        # Build messages without the system prompt
        messages = list(history)

        for iteration in range(MAX_ITERATIONS):
            logger.debug(
                "[claude] LLM call iteration %d for user %s",
                iteration + 1, user_id,
            )

            response = await self._claude_client.messages.create(
                model=self._model_name,
                max_tokens=4096,
                system=_SYSTEM_PROMPT,
                messages=messages,
                tools=_TOOLS_CLAUDE,
                temperature=0,
            )

            # Check stop reason
            if response.stop_reason == "end_turn":
                # Extract text from content blocks
                final_text = ""
                for block in response.content:
                    if block.type == "text":
                        final_text += block.text
                self._append_history(user_id, {"role": "assistant", "content": final_text})
                return final_text

            # Handle tool_use blocks
            if response.stop_reason == "tool_use":
                # Build assistant message with all content blocks
                assistant_content = []
                tool_results = []

                for block in response.content:
                    if block.type == "text":
                        assistant_content.append({
                            "type": "text",
                            "text": block.text,
                        })
                    elif block.type == "tool_use":
                        assistant_content.append({
                            "type": "tool_use",
                            "id": block.id,
                            "name": block.name,
                            "input": block.input,
                        })

                        # Execute the tool
                        tool_result = await self._dispatch_tool(
                            name=block.name,
                            arguments=block.input,
                            user_email=user_email,
                        )
                        logger.debug(
                            "Tool '%s' result (truncated): %s",
                            block.name, tool_result[:200],
                        )
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": tool_result,
                        })

                # Add assistant message and tool results to conversation
                messages.append({"role": "assistant", "content": assistant_content})
                messages.append({"role": "user", "content": tool_results})
                continue

            logger.warning("Unexpected stop_reason: %s", response.stop_reason)
            break

        fallback = (
            "Lo siento, no pude completar la consulta en el número máximo de pasos. "
            "Por favor, intenta reformular tu pregunta."
        )
        self._append_history(user_id, {"role": "assistant", "content": fallback})
        return fallback
