"""ClinicalTrials.gov MCP server (streamable HTTP on :8010/mcp).

Wraps the public API v2 (https://clinicaltrials.gov/data-api/api). Results
are trimmed to the fields a trial designer cares about so they fit in
context: design, arms, interventions, enrollment, endpoints, status.
"""

from __future__ import annotations

import json
import os
from typing import Any

import httpx
from mcp.server.mcpserver import MCPServer
from mcp.server.transport_security import TransportSecuritySettings

API = "https://clinicaltrials.gov/api/v2"
FIELDS = ",".join(
    [
        "NCTId",
        "BriefTitle",
        "OverallStatus",
        "Phase",
        "StudyType",
        "Condition",
        "InterventionName",
        "InterventionType",
        "DesignAllocation",
        "DesignInterventionModel",
        "DesignMasking",
        "DesignPrimaryPurpose",
        "EnrollmentCount",
        "EnrollmentType",
        "PrimaryOutcomeMeasure",
        "PrimaryOutcomeTimeFrame",
        "SecondaryOutcomeMeasure",
        "StartDate",
        "PrimaryCompletionDate",
        "LeadSponsorName",
        "ArmGroupLabel",
        "ArmGroupType",
        "ArmGroupInterventionNames",
    ]
)

mcp = MCPServer("clinicaltrials")


def _summarize(study: dict[str, Any]) -> dict[str, Any]:
    p = study.get("protocolSection", {})
    ident = p.get("identificationModule", {})
    status = p.get("statusModule", {})
    design = p.get("designModule", {})
    arms = p.get("armsInterventionsModule", {})
    outcomes = p.get("outcomesModule", {})
    info = design.get("designInfo", {})
    return {
        "nct_id": ident.get("nctId"),
        "title": ident.get("briefTitle"),
        "status": status.get("overallStatus"),
        "start_date": status.get("startDateStruct", {}).get("date"),
        "primary_completion_date": status.get("primaryCompletionDateStruct", {}).get("date"),
        "sponsor": p.get("sponsorCollaboratorsModule", {}).get("leadSponsor", {}).get("name"),
        "phases": design.get("phases"),
        "study_type": design.get("studyType"),
        "conditions": p.get("conditionsModule", {}).get("conditions"),
        "allocation": info.get("allocation"),
        "intervention_model": info.get("interventionModel"),
        "masking": info.get("maskingInfo", {}).get("masking"),
        "primary_purpose": info.get("primaryPurpose"),
        "enrollment": design.get("enrollmentInfo", {}).get("count"),
        "enrollment_type": design.get("enrollmentInfo", {}).get("type"),
        "arms": [
            {"label": a.get("label"), "type": a.get("type"), "interventions": a.get("interventionNames")}
            for a in arms.get("armGroups", [])
        ],
        "interventions": [{"name": i.get("name"), "type": i.get("type")} for i in arms.get("interventions", [])],
        "primary_outcomes": [
            {"measure": o.get("measure"), "time_frame": o.get("timeFrame")} for o in outcomes.get("primaryOutcomes", [])
        ],
        "secondary_outcomes": [
            {"measure": o.get("measure"), "time_frame": o.get("timeFrame")}
            for o in outcomes.get("secondaryOutcomes", [])
        ][:8],
    }


@mcp.tool()
async def search_studies(
    query: str = "",
    condition: str = "",
    intervention: str = "",
    phase: str = "",
    status: str = "",
    sponsor: str = "",
    page_size: int = 10,
    page_token: str = "",
) -> str:
    """Search ClinicalTrials.gov registered studies.

    Args:
        query: free-text terms (matched across title, conditions, interventions, outcomes).
        condition: disease or condition, e.g. "non-small cell lung cancer".
        intervention: drug or intervention name.
        phase: one of EARLY_PHASE1, PHASE1, PHASE2, PHASE3, PHASE4 (comma-separate for several).
        status: e.g. RECRUITING, COMPLETED, ACTIVE_NOT_RECRUITING, TERMINATED (comma-separate for several).
        sponsor: lead sponsor name.
        page_size: 1-50 results per page.
        page_token: token from a previous call to fetch the next page.
    Returns JSON with `studies` (design summaries) and `next_page_token`.
    """
    params: dict[str, Any] = {"fields": FIELDS, "pageSize": max(1, min(page_size, 50)), "countTotal": "true"}
    if query:
        params["query.term"] = query
    if condition:
        params["query.cond"] = condition
    if intervention:
        params["query.intr"] = intervention
    if sponsor:
        params["query.spons"] = sponsor
    if status:
        params["filter.overallStatus"] = status.upper().replace(" ", "")
    if phase:
        params["aggFilters"] = "phase:" + " ".join(p.strip().upper().replace("PHASE", "") for p in phase.split(","))
    if page_token:
        params["pageToken"] = page_token
    async with httpx.AsyncClient(base_url=API, timeout=30) as c:
        r = await c.get("/studies", params=params)
        r.raise_for_status()
        data = r.json()
    return json.dumps(
        {
            "total": data.get("totalCount"),
            "studies": [_summarize(s) for s in data.get("studies", [])],
            "next_page_token": data.get("nextPageToken"),
        }
    )


@mcp.tool()
async def get_study(nct_id: str) -> str:
    """Fetch one registered study by NCT id (e.g. NCT04368728) with full design, arms, outcomes and eligibility."""
    async with httpx.AsyncClient(base_url=API, timeout=30) as c:
        r = await c.get(f"/studies/{nct_id.strip().upper()}")
        r.raise_for_status()
        study = r.json()
    summary = _summarize(study)
    p = study.get("protocolSection", {})
    summary["eligibility"] = p.get("eligibilityModule", {}).get("eligibilityCriteria")
    summary["brief_summary"] = p.get("descriptionModule", {}).get("briefSummary")
    summary["has_results"] = study.get("hasResults")
    return json.dumps(summary)


if __name__ == "__main__":
    mcp.run(
        transport="streamable-http",
        host="0.0.0.0",  # noqa: S104 - container-internal service
        port=int(os.environ.get("MCP_PORT", "8010")),
        # Other services reach this server by its Docker hostname; the default
        # DNS-rebinding protection would reject those Host headers.
        transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
    )
