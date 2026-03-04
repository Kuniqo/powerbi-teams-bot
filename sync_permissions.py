# -*- coding: utf-8 -*-
"""
sync_permissions.py - Sincroniza permisos de datasets desde la API de Power BI.

Consulta la API /datasets/{id}/users de cada dataset y actualiza
datasets_config.json con los usuarios reales que tienen acceso.

Se ejecuta:
  - Al iniciar el bot (startup)
  - 3 veces al día vía APScheduler (8:00, 13:00, 18:00 UTC)
"""

import json
import logging
import os
from typing import Any

import httpx
import msal

logger = logging.getLogger(__name__)

_PBI_SCOPE = ["https://analysis.windows.net/powerbi/api/.default"]
_PBI_BASE = "https://api.powerbi.com/v1.0/myorg"
_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "datasets_config.json")

# Internal/system table prefixes to exclude from schemas
INTERNAL_PREFIXES = (
    "DateTableTemplate_", "LocalDateTable_", "Calendar_",
    "Measure_", "RowNumber-", "Template_",
)

# Datasets to skip (system/internal)
SKIP_DATASETS = {"Usage Metrics Report", "Refresh Data"}


class PermissionSyncer:
    """Synchronize Power BI dataset permissions into datasets_config.json."""

    def __init__(
        self,
        tenant_id: str,
        client_id: str,
        client_secret: str,
    ) -> None:
        self._tenant_id = tenant_id
        self._client_id = client_id
        self._client_secret = client_secret
        self._msal_app = msal.ConfidentialClientApplication(
            client_id,
            authority=f"https://login.microsoftonline.com/{tenant_id}",
            client_credential=client_secret,
        )

    def _get_token(self) -> str:
        result = self._msal_app.acquire_token_silent(_PBI_SCOPE, account=None)
        if not result:
            result = self._msal_app.acquire_token_for_client(scopes=_PBI_SCOPE)
        if "access_token" not in result:
            raise RuntimeError(f"MSAL token error: {result.get('error_description', 'unknown')}")
        return result["access_token"]

    async def sync(self) -> dict[str, Any]:
        """
        Full sync: discover workspaces → datasets → schemas → permissions.
        Returns the updated config dict and saves it to disk.
        """
        token = self._get_token()
        headers = {"Authorization": f"Bearer {token}"}

        async with httpx.AsyncClient(timeout=30.0) as http:
            # 1. Get workspaces
            resp = await http.get(f"{_PBI_BASE}/groups", headers=headers)
            resp.raise_for_status()
            workspaces = resp.json().get("value", [])

            if not workspaces:
                logger.warning("No workspaces found for service principal")
                return {"datasets": []}

            config = {"datasets": []}

            for ws in workspaces:
                ws_id = ws["id"]
                ws_name = ws.get("name", ws_id)
                logger.info("Syncing workspace: %s (%s)", ws_name, ws_id)

                # 2. Get datasets in workspace
                resp = await http.get(
                    f"{_PBI_BASE}/groups/{ws_id}/datasets", headers=headers
                )
                if resp.status_code != 200:
                    logger.warning("Could not list datasets for workspace %s: %s", ws_id, resp.status_code)
                    continue

                datasets = resp.json().get("value", [])

                for ds in datasets:
                    ds_name = ds.get("name", "")
                    ds_id = ds.get("id", "")

                    if ds_name in SKIP_DATASETS:
                        continue

                    # 3. Get dataset users/permissions
                    users = await self._get_dataset_users(http, headers, ws_id, ds_id)

                    # 4. Get dataset schema (tables + columns)
                    tables = await self._get_dataset_schema(http, headers, ws_id, ds_id)

                    dataset_entry = {
                        "name": ds_name,
                        "workspace_id": ws_id,
                        "dataset_id": ds_id,
                        "description": f"Dataset de {ds_name}",
                        "access": {
                            "mode": "allowlist",
                            "users": sorted(users),
                        },
                        "schema": {
                            "tables": tables,
                            "relationships": [],
                        },
                    }
                    config["datasets"].append(dataset_entry)
                    logger.info(
                        "  %s: %d tables, %d users",
                        ds_name, len(tables), len(users),
                    )

        # Save to disk
        with open(_CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=4, ensure_ascii=False)

        logger.info(
            "Permission sync complete: %d datasets saved to %s",
            len(config["datasets"]), _CONFIG_PATH,
        )
        return config

    async def sync_permissions_only(self) -> dict[str, Any]:
        """
        Lightweight sync: only update the 'access.users' field for each dataset
        in the existing config. Does NOT re-fetch schemas (faster).
        """
        # Load existing config
        try:
            with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
                config = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            logger.warning("No existing config found, running full sync")
            return await self.sync()

        token = self._get_token()
        headers = {"Authorization": f"Bearer {token}"}

        async with httpx.AsyncClient(timeout=30.0) as http:
            updated = 0
            for ds in config.get("datasets", []):
                ws_id = ds.get("workspace_id", "")
                ds_id = ds.get("dataset_id", "")
                ds_name = ds.get("name", ds_id)

                if not ws_id or not ds_id:
                    continue

                try:
                    users = await self._get_dataset_users(http, headers, ws_id, ds_id)
                    ds["access"]["users"] = sorted(users)
                    updated += 1
                    logger.debug("Updated permissions for %s: %d users", ds_name, len(users))
                except Exception as exc:
                    logger.warning("Failed to sync permissions for %s: %s", ds_name, exc)

        # Save updated config
        with open(_CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=4, ensure_ascii=False)

        logger.info("Permission-only sync complete: %d/%d datasets updated", updated, len(config.get("datasets", [])))
        return config

    # ── Private helpers ────────────────────────────────────────────────────

    async def _get_dataset_users(
        self, http: httpx.AsyncClient, headers: dict, ws_id: str, ds_id: str
    ) -> list[str]:
        """Get list of user emails with access to a dataset."""
        url = f"{_PBI_BASE}/groups/{ws_id}/datasets/{ds_id}/users"
        try:
            resp = await http.get(url, headers=headers)
            if resp.status_code != 200:
                logger.warning("Could not get users for dataset %s: HTTP %s", ds_id, resp.status_code)
                return []

            users = []
            for u in resp.json().get("value", []):
                identifier = u.get("identifier", "")
                if "@" in identifier:  # Skip service principal GUIDs
                    users.append(identifier.lower())
            return users

        except Exception as exc:
            logger.warning("Error fetching users for dataset %s: %s", ds_id, exc)
            return []

    async def _get_dataset_schema(
        self, http: httpx.AsyncClient, headers: dict, ws_id: str, ds_id: str
    ) -> list[dict]:
        """Get tables and columns for a dataset, filtering out internal tables."""
        url = f"{_PBI_BASE}/groups/{ws_id}/datasets/{ds_id}"
        try:
            # Get tables via discover
            tables_url = f"{_PBI_BASE}/groups/{ws_id}/datasets/{ds_id}/discover"
            resp = await http.get(tables_url, headers=headers)

            # If discover endpoint not available, try execute a DMV query
            if resp.status_code != 200:
                return await self._get_schema_via_dmv(http, headers, ws_id, ds_id)

            tables_data = resp.json().get("tables", [])
            return self._clean_tables(tables_data)

        except Exception as exc:
            logger.debug("Could not fetch schema for %s: %s, trying DMV", ds_id, exc)
            return await self._get_schema_via_dmv(http, headers, ws_id, ds_id)

    async def _get_schema_via_dmv(
        self, http: httpx.AsyncClient, headers: dict, ws_id: str, ds_id: str
    ) -> list[dict]:
        """Get schema by executing a DAX DMV query against TMSCHEMA tables."""
        try:
            # Get tables
            url = f"{_PBI_BASE}/groups/{ws_id}/datasets/{ds_id}/executeQueries"
            tables_query = {
                "queries": [{"query": "EVALUATE INFO.TABLES()"}],
                "serializerSettings": {"includeNulls": True},
            }
            resp = await http.post(url, json=tables_query, headers=headers)
            if resp.status_code != 200:
                return []

            tables_result = resp.json()
            table_rows = []
            for r in tables_result.get("results", []):
                for t in r.get("tables", []):
                    table_rows.extend(t.get("rows", []))

            # Get columns
            cols_query = {
                "queries": [{"query": "EVALUATE INFO.COLUMNS()"}],
                "serializerSettings": {"includeNulls": True},
            }
            resp = await http.post(url, json=cols_query, headers=headers)
            if resp.status_code != 200:
                return []

            cols_result = resp.json()
            col_rows = []
            for r in cols_result.get("results", []):
                for t in r.get("tables", []):
                    col_rows.extend(t.get("rows", []))

            # Build table ID → name mapping
            table_map = {}
            for row in table_rows:
                tid = row.get("[ID]") or row.get("ID")
                tname = row.get("[Name]") or row.get("Name", "")
                if tid is not None and tname:
                    table_map[tid] = tname

            # Build tables with columns
            tables_cols: dict[str, list[dict]] = {name: [] for name in table_map.values()}
            for row in col_rows:
                tid = row.get("[TableID]") or row.get("TableID")
                cname = row.get("[ExplicitName]") or row.get("ExplicitName", "")
                ctype = row.get("[ExplicitDataType]") or row.get("ExplicitDataType") or row.get("[DataType]") or row.get("DataType", "Unknown")

                tname = table_map.get(tid, "")
                if tname and cname:
                    # Skip internal columns
                    if cname.startswith("RowNumber-") or cname.startswith("_"):
                        continue
                    tables_cols.setdefault(tname, []).append({
                        "name": cname,
                        "type": str(ctype),
                        "description": "",
                    })

            # Filter out internal tables
            result = []
            for tname, columns in tables_cols.items():
                if any(tname.startswith(p) for p in INTERNAL_PREFIXES):
                    continue
                if tname.startswith("_"):
                    continue
                if columns:
                    result.append({
                        "name": tname,
                        "description": "",
                        "columns": columns,
                    })

            return result

        except Exception as exc:
            logger.warning("DMV schema query failed for %s: %s", ds_id, exc)
            return []

    @staticmethod
    def _clean_tables(tables_data: list[dict]) -> list[dict]:
        """Filter internal tables and columns from raw API data."""
        result = []
        for table in tables_data:
            tname = table.get("name", "")
            if any(tname.startswith(p) for p in INTERNAL_PREFIXES):
                continue
            if tname.startswith("_"):
                continue

            columns = []
            for col in table.get("columns", []):
                cname = col.get("name", "")
                if cname.startswith("RowNumber-") or cname.startswith("_"):
                    continue
                columns.append({
                    "name": cname,
                    "type": col.get("dataType", "Unknown"),
                    "description": "",
                })

            if columns:
                result.append({
                    "name": tname,
                    "description": "",
                    "columns": columns,
                })
        return result
