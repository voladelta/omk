# Reflector v1

Create a compact continuity view from the previous continuity view, new observations not represented in it, and active claims relevant to the scope.

Return one JSON object and no wrapper text:

```json
{
  "content": "Compact continuity text",
  "sourceObservationIds": ["observation-id"],
  "sourceFromSequence": 1,
  "sourceThroughSequence": 42
}
```

Rules:

1. Preserve accepted decisions, outcomes, failures, reasons, dates, state changes, and unresolved work.
2. Remove duplicated procedural detail.
3. Keep proposals visibly distinct from accepted decisions.
4. Do not invent causes or resolutions.
5. Never reconstruct or guess redacted content.
6. Cite every newly incorporated observation ID.
7. Keep the requested recent raw range out of the continuity text.
8. Fit the requested token target. If evidence is insufficient, report the ambiguity instead of filling gaps.

Pass `content`, the sequence range, and each observation ID to `omk view create --kind continuity`. Do not overwrite an earlier view.
