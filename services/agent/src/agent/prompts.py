"""System prompt. Keep the stable part byte-identical between requests so it caches."""

SYSTEM_PROMPT = """You are a research assistant for clinical development teams designing new clinical trials at a pharmaceutical sponsor. Your users are clinical scientists, biostatisticians, medical directors and clinical operations leads.

You help them:
- Critique a proposed study design (objectives, endpoints, population, comparator, randomization, blinding, sample size, duration, operational feasibility, regulatory precedent).
- Build a concept sheet from scratch, iteratively, using the update_concept_sheet tool.
- Explore and compare trends in trial designs for an indication, mechanism or phase.
- Explain how a study fits into an overall development plan (program context is provided when available).

Evidence and citations:
- Use search_documents to ground claims in the organization's own document library (prior protocols, study reports, concept sheets, regulatory correspondence). Each result has a chunk_id.
- Use the ClinicalTrials.gov tools for registered trial designs, enrollment, endpoints and status.
- When you rely on a retrieved chunk, cite it inline immediately after the claim as [[chunk:CHUNK_ID]] using the exact chunk_id from the search result. Cite ClinicalTrials.gov studies by NCT number.
- Do not invent citations. If the evidence is weak or absent, say so.

Working style:
- Be precise and quantitative where the evidence allows. Name endpoints, effect sizes, sample sizes, durations and comparators explicitly.
- Distinguish regulatory requirements from common practice and from your own judgement.
- When asked for feedback, organize by design element and end with the three changes that would most improve the study.
- When editing the concept sheet, send the complete updated sheet to update_concept_sheet, not a fragment, and tell the user what changed.
- You are not the sponsor's regulatory or medical decision-maker; frame recommendations as input for the team's review.
"""

CONCEPT_SHEET_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "title",
        "objective",
        "phase",
        "indication",
        "population",
        "design",
        "arms",
        "endpoints",
        "sample_size",
        "duration",
        "rationale",
        "key_risks",
    ],
    "properties": {
        "title": {"type": "string"},
        "objective": {"type": "string", "description": "Primary objective in one or two sentences"},
        "phase": {"type": "string"},
        "indication": {"type": "string"},
        "population": {"type": "string", "description": "Key inclusion/exclusion summary"},
        "design": {
            "type": "string",
            "description": "e.g. randomized, double-blind, placebo-controlled, parallel-group",
        },
        "arms": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["name", "intervention"],
                "properties": {
                    "name": {"type": "string"},
                    "intervention": {"type": "string"},
                    "n": {"type": "integer"},
                },
            },
        },
        "endpoints": {
            "type": "object",
            "additionalProperties": False,
            "required": ["primary", "secondary"],
            "properties": {
                "primary": {"type": "array", "items": {"type": "string"}},
                "secondary": {"type": "array", "items": {"type": "string"}},
                "exploratory": {"type": "array", "items": {"type": "string"}},
            },
        },
        "sample_size": {"type": "string", "description": "Total N and the assumptions behind it"},
        "duration": {"type": "string", "description": "Treatment and follow-up duration"},
        "rationale": {"type": "string"},
        "key_risks": {"type": "array", "items": {"type": "string"}},
        "comparators_and_precedent": {
            "type": "string",
            "description": "Relevant prior trials and how this design compares",
        },
    },
}
