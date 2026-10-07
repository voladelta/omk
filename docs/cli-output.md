# CLI output and idempotency

Every successful data command writes one compact JSON value to standard output. Help and version commands write plain text.

Read commands return data directly. `init` and each idempotent write return an operation envelope:

```json
{
  "data": { "id": "..." },
  "operation": { "replayed": false }
}
```

The operation fields tell you how to recover:

- `retryable` means you can retry the command without changing it
- `sameKeyReusable` means validation failed before OMK recorded the operation
- `nextAction` tells you what to do before you retry

## Retries and replays

Every write needs a stable, globally unique idempotency key. Reuse a key only when you retry an identical command.

An identical retry returns the original data with `replayed: true`. If another process holds the database past the five second busy timeout, OMK returns code `busy` with `retryable` and `sameKeyReusable` both true. Retry the identical request with the same key. OMK rejects a reused key if any input changes.

`do-not-store` is the exception. It replays requests when only the payload, metadata or token hint changes. OMK keeps no fingerprint derived from that data.

Saved results are kept for 30 days. After that, each write compacts a small batch of expired operations down to their key, operation name and request hash. A compacted key still rejects changed input with `idempotency_conflict`. An identical retry returns `operation_expired` instead of running the operation again: it already committed, so inspect its records instead of retrying. A purge still tombstones compacted operations that mention a purged record.

`observe plan` saves only its run ID. A replay rebuilds the plan: it keeps the same run and exact event range, but active claims and the previous continuation reflect the store at replay time. Storing whole plans made them most of the operation log, because each plan copied every active claim.

## Errors

Failures are JSON on standard error:

```json
{
  "error": {
    "code": "budget_exceeded",
    "message": "context budget too small: minimumRequiredTokens=12 for the context structure",
    "retryable": false,
    "sameKeyReusable": true,
    "nextAction": "increase the token budget and retry with the same key"
  }
}
```
