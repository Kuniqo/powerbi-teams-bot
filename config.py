# -*- coding: utf-8 -*-
"""
config.py - Load environment configuration for the Power BI Teams Bot.
Soporta múltiples proveedores LLM: openai, azure_openai, gemini, claude, perplexity.
Se configura con la variable de entorno LLM_PROVIDER.
"""

import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

# Proveedores válidos
VALID_PROVIDERS = ("openai", "azure_openai", "gemini", "claude", "perplexity")


@dataclass
class Config:
    # ── Bot Framework ──────────────────────────────────────────────────────────
    APP_ID: str = ""
    APP_PASSWORD: str = ""

    # ── Proveedor LLM activo ──────────────────────────────────────────────────
    # Valores: openai | azure_openai | gemini | claude | perplexity
    LLM_PROVIDER: str = "openai"

    # ── OpenAI (estándar) ─────────────────────────────────────────────────────
    OPENAI_API_KEY: str = ""
    OPENAI_MODEL: str = "gpt-4o"

    # ── Azure OpenAI ──────────────────────────────────────────────────────────
    AZURE_OPENAI_ENDPOINT: str = ""
    AZURE_OPENAI_API_KEY: str = ""
    AZURE_OPENAI_DEPLOYMENT: str = "gpt-4o"
    AZURE_OPENAI_API_VERSION: str = "2024-02-01"

    # ── Google Gemini ─────────────────────────────────────────────────────────
    GEMINI_API_KEY: str = ""
    GEMINI_MODEL: str = "gemini-2.0-flash"

    # ── Anthropic Claude ──────────────────────────────────────────────────────
    CLAUDE_API_KEY: str = ""
    CLAUDE_MODEL: str = "claude-sonnet-4-20250514"

    # ── Perplexity ────────────────────────────────────────────────────────────
    PERPLEXITY_API_KEY: str = ""
    PERPLEXITY_MODEL: str = "sonar-pro"

    # ── Power BI / MSAL ───────────────────────────────────────────────────────
    PBI_TENANT_ID: str = ""
    PBI_CLIENT_ID: str = ""
    PBI_CLIENT_SECRET: str = ""


def load_config() -> Config:
    """Read all required values from environment variables."""
    provider = os.getenv("LLM_PROVIDER", "openai").strip().lower()
    if provider not in VALID_PROVIDERS:
        raise ValueError(
            f"LLM_PROVIDER='{provider}' no es válido. "
            f"Opciones: {', '.join(VALID_PROVIDERS)}"
        )

    return Config(
        APP_ID=os.getenv("APP_ID", ""),
        APP_PASSWORD=os.getenv("APP_PASSWORD", ""),
        # Proveedor
        LLM_PROVIDER=provider,
        # OpenAI
        OPENAI_API_KEY=os.getenv("OPENAI_API_KEY", ""),
        OPENAI_MODEL=os.getenv("OPENAI_MODEL", "gpt-4o"),
        # Azure OpenAI
        AZURE_OPENAI_ENDPOINT=os.getenv("AZURE_OPENAI_ENDPOINT", ""),
        AZURE_OPENAI_API_KEY=os.getenv("AZURE_OPENAI_API_KEY", ""),
        AZURE_OPENAI_DEPLOYMENT=os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o"),
        AZURE_OPENAI_API_VERSION=os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-01"),
        # Gemini
        GEMINI_API_KEY=os.getenv("GEMINI_API_KEY", ""),
        GEMINI_MODEL=os.getenv("GEMINI_MODEL", "gemini-2.0-flash"),
        # Claude
        CLAUDE_API_KEY=os.getenv("CLAUDE_API_KEY", ""),
        CLAUDE_MODEL=os.getenv("CLAUDE_MODEL", "claude-sonnet-4-20250514"),
        # Perplexity
        PERPLEXITY_API_KEY=os.getenv("PERPLEXITY_API_KEY", ""),
        PERPLEXITY_MODEL=os.getenv("PERPLEXITY_MODEL", "sonar-pro"),
        # Power BI
        PBI_TENANT_ID=os.getenv("PBI_TENANT_ID", ""),
        PBI_CLIENT_ID=os.getenv("PBI_CLIENT_ID", ""),
        PBI_CLIENT_SECRET=os.getenv("PBI_CLIENT_SECRET", ""),
    )
