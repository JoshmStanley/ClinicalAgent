#!/usr/bin/env bash
# End-to-end local demo through Traefik (:8000) using dev-mode auth headers.
# Requires: `make up` (all services running) and curl + jq.
set -euo pipefail

GATEWAY=${GATEWAY:-http://localhost:8000}
ORG=${ORG:-org_demo}
USER_ID=${USER_ID:-user_demo}
ROLE=${ROLE:-org:admin}
QUESTION=${1:-"Review the design of protocol XYZ-201. Is the sample size assumption reasonable, and what would you change about the primary endpoint? Compare with recent registered Phase 2 ulcerative colitis induction trials."}

H=(-H "X-Dev-User-Id: $USER_ID" -H "X-Dev-Org-Id: $ORG" -H "X-Dev-Role: $ROLE")
need() { command -v "$1" >/dev/null || { echo "missing $1" >&2; exit 1; }; }
need curl; need jq

echo "▶ me";        curl -sf "${H[@]}" "$GATEWAY/api/me" | jq -c .
echo "▶ budget (org, \$50/month)"
curl -sf "${H[@]}" -X PUT "$GATEWAY/api/budgets" -H 'content-type: application/json' \
  -d '{"scope":"org","scope_key":"","monthly_limit_usd":50}' | jq -c .

echo "▶ upload sample protocol"
DOC=$(curl -sf "${H[@]}" -F "file=@$(dirname "$0")/sample/sample_protocol.md;type=text/markdown" "$GATEWAY/api/documents?title=Protocol%20XYZ-201")
DOC_ID=$(echo "$DOC" | jq -r .id); echo "  document $DOC_ID"

echo -n "▶ waiting for ingestion"
for _ in $(seq 1 60); do
  STATUS=$(curl -sf "${H[@]}" "$GATEWAY/api/documents/$DOC_ID" | jq -r .status)
  case "$STATUS" in
    indexed) echo " → indexed"; break ;;
    failed)  echo " → FAILED"; curl -sf "${H[@]}" "$GATEWAY/api/documents/$DOC_ID" | jq .; exit 1 ;;
    *) echo -n "."; sleep 2 ;;
  esac
done

echo "▶ search sanity check"
curl -sf "${H[@]}" -X POST "$GATEWAY/api/search" -H 'content-type: application/json' \
  -d '{"query":"primary endpoint clinical remission modified Mayo score","top_k":2}' | jq -c '.[] | {chunk_id, section_path, page}'

echo "▶ study + conversation"
STUDY_ID=$(curl -sf "${H[@]}" -X POST "$GATEWAY/api/studies" -H 'content-type: application/json' \
  -d '{"name":"XYZ-201 UC induction","phase":"2","indication":"Ulcerative colitis"}' | jq -r .id)
CONV_ID=$(curl -sf "${H[@]}" -X POST "$GATEWAY/api/conversations" -H 'content-type: application/json' \
  -d "{\"title\":\"Design review\",\"study_id\":\"$STUDY_ID\"}" | jq -r .id)
echo "  study $STUDY_ID  conversation $CONV_ID"

echo "▶ send message"
RUN_ID=$(curl -sf "${H[@]}" -X POST "$GATEWAY/api/conversations/$CONV_ID/messages" -H 'content-type: application/json' \
  -d "$(jq -cn --arg t "$QUESTION" '{text:$t}')" | jq -r .run_id)
echo "  run $RUN_ID"

echo "▶ streaming (SSE)"; echo
curl -sN "${H[@]}" "$GATEWAY/api/runs/$RUN_ID/stream" | while IFS= read -r line; do
  case "$line" in
    event:*) EV=${line#event: } ;;
    data:*)
      DATA=${line#data: }
      case "$EV" in
        text.delta) printf '%s' "$(echo "$DATA" | jq -r .text)" ;;
        tool.call) printf '\n[tool: %s]\n' "$(echo "$DATA" | jq -r .name)" ;;
        citation) printf '\n[cite %s → %s p.%s]' "$(echo "$DATA" | jq -r .chunk_id)" "$(echo "$DATA" | jq -r .document_title)" "$(echo "$DATA" | jq -r .page)" ;;
        run.failed) printf '\n!! run failed: %s\n' "$(echo "$DATA" | jq -r .error)" ;;
        usage) printf '\n\n[usage] %s\n' "$(echo "$DATA" | jq -c .)" ;;
        done) printf '\n[done %s]\n' "$(echo "$DATA" | jq -r .status)" ;;
      esac ;;
  esac
done

echo "▶ usage summary"; curl -sf "${H[@]}" "$GATEWAY/api/usage/summary" | jq -c .
