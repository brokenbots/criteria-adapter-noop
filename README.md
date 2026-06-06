# criteria-adapter-noop

A [Criteria](https://github.com/brokenbots/criteria) adapter that does nothing
and reports success, over the v2 adapter protocol. It is an out-of-process
plugin binary built on the [Go adapter SDK](https://github.com/brokenbots/criteria-go-adapter-sdk)
and the [wire contract](https://github.com/brokenbots/criteria-adapter-proto).

It runs no user code: it opens a session, optionally sleeps for `delay_ms`, and
emits a single `success` result. It exists for control-flow and pipeline
scaffolding (placeholder steps, joins, timing) and as a minimal reference
adapter for exercising the signing/lock path.

> Note: noop's control-flow role is transitional and will eventually be replaced
> by a native engine step.

## Usage

```hcl
adapter "noop" "gate" {}

step "join" {
  adapter = adapter.noop.gate
  input {
    delay_ms = "0"
  }
}
```

Inputs: `delay_ms` (optional; milliseconds to sleep before completing). Output:
a single `success` outcome.

## Build & test

```bash
go build -o bin/criteria-adapter-noop .
go test ./...
```

## Publish

Tagging `vX.Y.Z` builds the binary and publishes it as an OCI artifact to
`ghcr.io/brokenbots/criteria-adapter-noop:X.Y.Z` via the reusable
[`brokenbots/publish-adapter`](https://github.com/brokenbots/publish-adapter)
action.

## License

Apache-2.0. See [LICENSE](LICENSE).
