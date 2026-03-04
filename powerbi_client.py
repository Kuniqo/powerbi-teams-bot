# -*- coding: utf-8 -*-
"""
powerbi_client.py - Power BI REST API client with MSAL authentication.

Handles:
- Client credentials OAuth2 flow via MSAL (with token caching)
- Executing DAX queries against Power BI datasets
- Listing workspaces and datasets
- Formatting query results as Markdown tables
"""

import logging
from typing import Any

import httpx
import msal

from config import Config

logger = logging.getLogger(__name__)

_PBI_SCOPE = ["https://analysis.windows.net/powerbi/api/.default"]
_PBI_BASE = "https://api.powerbi.com/v1.0/myorg"


class PowerBIClient:
    """Thin async wrapper around the Power BI REST API."""

    def __init__(self, config: Config) -> None:
        self._config = config
        # MSAL confidential client with an in-memory token cache
        self._msal_app = msal.ConfidentialClientApplication(
            client_id=config.PBI_CLIENT_ID,
            client_credential=config.PBI_CLIENT_SECRET,
            authority=f"https://login.microsoftonline.com/{config.PBI_TENANT_ID}",
        )
        self._http = httpx.AsyncClient(timeout=60.0)

    # ── Authentication ──────────────────────────────────────────────────────

    async def _get_token(self) -> str:
        """Return a valid Bearer token, refreshing from MSAL cache if needed."""
        # Try silent (cache hit first)
        result = self._msal_app.acquire_token_silent(_PBI_SCOPE, account=None)
        if not result:
            result = self._msal_app.acquire_token_for_client(scopes=_PBI_SCOPE)

        if "access_token" not in result:
            error = result.get("error_description", result.get("error", "unknown"))
            raise RuntimeError(f"MSAL token acquisition failed: {error}")

        return result["access_token"]

    async def _headers(self) -> dict[str, str]:
        token = await self._get_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    # ── Public API methods ──────────────────────────────────────────────────

    async def execute_dax_query(
        self, workspace_id: str, dataset_id: str, dax_query: str
    ) -> dict[str, Any]:
        """
        Execute a DAX query against a Power BI dataset.

        Args:
            workspace_id: The Power BI workspace (group) GUID.
            dataset_id:   The dataset GUID.
            dax_query:    A valid DAX query string (must start with EVALUATE).

        Returns:
            The raw JSON response dict from the API.
        """
        url = (
            f"{_PBI_BASE}/groups/{workspace_id}"
            f"/datasets/{dataset_id}/executeQueries"
        )
        payload = {
            "queries": [{"query": dax_query}],
            "serializerSettings": {"includeNulls": True},
        }
        headers = await self._headers()
        response = await self._http.post(url, json=payload, headers=headers)

        if response.status_code != 200:
            logger.error(
                "Power BI executeQueries failed: %s %s",
                response.status_code,
                response.text,
            )
            response.raise_for_status()

        return response.json()

    async def list_workspaces(self) -> list[dict[str, Any]]:
        """Return all workspaces the service principal has access to."""
        headers = await self._headers()
        response = await self._http.get(f"{_PBI_BASE}/groups", headers=headers)
        response.raise_for_status()
        data = response.json()
        return data.get("value", [])

    async def list_datasets(self, workspace_id: str) -> list[dict[str, Any]]:
        """Return all datasets in a workspace."""
        headers = await self._headers()
        url = f"{_PBI_BASE}/groups/{workspace_id}/datasets"
        response = await self._http.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        return data.get("value", [])

    # ── Result formatting ───────────────────────────────────────────────────

    @staticmethod
    def format_results(response: dict[str, Any]) -> str:
        """
        Convert the executeQueries API response into a readable Markdown table.

        Returns a string with one table per result-set, or an informational
        message when the response is empty.
        """
        try:
            results = response.get("results", [])
            if not results:
                return "_No se obtuvieron resultados._"

            output_parts: list[str] = []

            for result_idx, result in enumerate(results):
                tables = result.get("tables", [])
                for table in tables:
                    rows: list[dict] = table.get("rows", [])
                    if not rows:
                        output_parts.append("_La consulta no devolvió filas._")
                        continue

                    # Derive column headers from first row keys, stripping
                    # the "TableName[ColumnName]" Power BI prefix when present.
                    raw_headers = list(rows[0].keys())
                    headers = [_clean_column_name(h) for h in raw_headers]

                    # Build Markdown table
                    header_row = "| " + " | ".join(headers) + " |"
                    separator = "| " + " | ".join(["---"] * len(headers)) + " |"
                    data_rows = []
                    for row in rows:
                        cells = []
                        for key in raw_headers:
                            val = row.get(key)
                            cells.append("" if val is None else str(val))
                        data_rows.append("| " + " | ".join(cells) + " |")

                    table_md = "\n".join([header_row, separator] + data_rows)
                    output_parts.append(table_md)

            return "\n\n".join(output_parts) if output_parts else "_Sin resultados._"

        except Exception as exc:  # pylint: disable=broad-except
            logger.exception("Error formatting Power BI results")
            return f"_Error al formatear los resultados: {exc}_"

    async def close(self) -> None:
        """Close the underlying HTTP client."""
        await self._http.aclose()


# ── Helpers ─────────────────────────────────────────────────────────────────

def _clean_column_name(raw: str) -> str:
    """
    Power BI returns column names as "[TableName].[ColumnName]" or
    "TableName[ColumnName]". Strip the table prefix for cleaner headers.
    """
    if "[" in raw:
        # e.g. "Patients[PatientName]" → "PatientName"
        #       "[Patients].[PatientName]" → "PatientName"
        name = raw.rsplit("[", 1)[-1].rstrip("]")
        return name
    return raw
