"""Tool definitions and executors available to the agent."""

from __future__ import annotations

import json
import logging
from contextlib import AsyncExitStack
from typing import Any

import httpx
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

from agent.prompts import CONCEPT_SHEET_SCHEMA
from agent.settings import Settings
from clinical_common.auth import Principal
from clinical_common.events import Citation

log = logging.getLogger(__name__)

SEARCH_DOCUMENTS = {
    "name": "search_documents",
    "description": (
        "Hybrid semantic + keyword search over the organization's clinical document library "
        "(protocols, study reports, concept sheets). Returns chunks with a chunk_id to cite as [[chunk:ID]]."
    ),
    "input_schema": {
        "type": "object",
        "additionalProperties": False,
        "required": ["query"],
        "properties": {
            "query": {
                "type": "string",
                "description": "Natural-language query. Include drug names, indication, endpoint names or NCT ids.",
            },
            "top_k": {"type": "integer", "minimum": 1, "maximum": 20, "default": 8},
            "document_ids": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Restrict to these document ids",
            },
        },
    },
    "strict": True,
}

UPDATE_CONCEPT_SHEET = {
    "name": "update_concept_sheet",
    "description": (
        "Save a new version of the study's concept sheet. Always pass the complete sheet. "
        "Only available when the conversation is attached to a study."
    ),
    "input_schema": {
        "type": "object",
        "additionalProperties": False,
        "required": ["content"],
        "properties": {"content": CONCEPT_SHEET_SCHEMA},
    },
    "strict": True,
}


class ToolBox:
    """Owns per-run connections (documents service, conversations service, MCP server)."""

    def __init__(self, settings: Settings, principal: Principal, study_id: str | None) -> None:
        self.settings = settings
        self.principal = principal
        self.study_id = study_id
        self.seen_chunks: dict[str, Citation] = {}
        self._stack = AsyncExitStack()
        self._mcp: ClientSession | None = None
        self._mcp_tool_names: set[str] = set()
        self.definitions: list[dict[str, Any]] = [SEARCH_DOCUMENTS]
        if study_id:
            self.definitions.append(UPDATE_CONCEPT_SHEET)
        hdrs = principal.internal_headers(settings.internal_token)
        self.documents = httpx.AsyncClient(base_url=settings.documents_url, headers=hdrs, timeout=30)
        self.conversations = httpx.AsyncClient(base_url=settings.conversations_url, headers=hdrs, timeout=30)

    async def __aenter__(self) -> ToolBox:
        await self._stack.enter_async_context(self.documents)
        await self._stack.enter_async_context(self.conversations)
        await self._connect_mcp()
        return self

    async def __aexit__(self, *exc) -> None:
        await self._stack.aclose()

    async def _connect_mcp(self) -> None:
        url = self.settings.clinicaltrials_mcp_url
        if not url:
            return
        try:
            read, write = await self._stack.enter_async_context(streamable_http_client(url))
            session = await self._stack.enter_async_context(ClientSession(read, write))
            await session.initialize()
            listed = await session.list_tools()
            for t in listed.tools:
                self.definitions.append(
                    {"name": t.name, "description": t.description or "", "input_schema": t.inputSchema}
                )
                self._mcp_tool_names.add(t.name)
            self._mcp = session
            log.info("connected MCP %s tools=%s", url, sorted(self._mcp_tool_names))
        except Exception as exc:  # the agent still works without ClinicalTrials.gov
            log.warning("MCP unavailable at %s: %s", url, exc)

    async def execute(self, name: str, args: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        """Returns (tool_result_text, side_info) where side_info feeds run events."""
        if name == "search_documents":
            r = await self.documents.post(
                "/search",
                json={"query": args["query"], "top_k": args.get("top_k", 8), "document_ids": args.get("document_ids")},
            )
            r.raise_for_status()
            hits = r.json()
            for h in hits:
                self.seen_chunks[h["chunk_id"]] = Citation(
                    chunk_id=h["chunk_id"],
                    document_id=h["document_id"],
                    document_title=h.get("document_title", ""),
                    section_path=h.get("section_path", ""),
                    page=h.get("page"),
                    snippet=h["text"][:280],
                )
            compact = [{k: h[k] for k in ("chunk_id", "document_title", "section_path", "page", "text")} for h in hits]
            return json.dumps(compact), {"hit_count": len(hits)}
        if name == "update_concept_sheet":
            if not self.study_id:
                return "No study attached to this conversation; cannot save a concept sheet.", {}
            r = await self.conversations.put(
                f"/studies/{self.study_id}/concept-sheet", json={"content": args["content"]}
            )
            r.raise_for_status()
            sheet = r.json()
            return f"Saved concept sheet version {sheet['version']}.", {"concept_sheet": sheet}
        if name in self._mcp_tool_names and self._mcp is not None:
            result = await self._mcp.call_tool(name, arguments=args)
            text = "\n".join(getattr(c, "text", "") for c in result.content if getattr(c, "type", "") == "text")
            if result.isError:
                raise RuntimeError(text or "MCP tool error")
            return text, {}
        raise RuntimeError(f"Unknown tool {name}")
