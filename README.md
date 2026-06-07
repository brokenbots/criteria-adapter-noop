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

## Install

Published as a signed, multi-platform OCI artifact
(`linux/amd64`, `linux/arm64`, `darwin/amd64`, `darwin/arm64`). Pin and lock it:

```bash
criteria adapter lock <workflow-dir>
```

## Setup (adapter configuration)

The noop adapter takes **no adapter-level `config {}` keys** — just declare it
and bind it where a control-flow placeholder is needed:

```hcl
adapter "noop" "gate" {
  source  = "ghcr.io/brokenbots/criteria-adapter-noop"
  version = "0.5.x"
}
```

## Step inputs

| Input | Required | Description |
| --- | --- | --- |
| `delay_ms` | no | Milliseconds to sleep before completing. Must be a non-negative integer. Default `0`. |

```hcl
step "join" {
  adapter = adapter.noop.gate
  input {
    delay_ms = "0"
  }
}
```

## Config overrides

None — the only knob is the `delay_ms` step input, set per step. There is no
adapter `config {}` block to override.

## Outputs

A single `success` outcome. No output keys are emitted.

## Build & test

```bash
make build
make test
```

## Security & dependencies

See [SECURITY.md](SECURITY.md) and [docs/dependency-policy.md](docs/dependency-policy.md).
Reproduce the CI security checks locally:

```bash
make vuln-scan      # osv-scanner — known-vulnerability gate (WS49)
make deps-outdated  # go-mod-outdated — freshness report (WS50)
make deps-majors    # gomajor — available major (/vN) upgrades
```

## Publish

Tagging `vX.Y.Z` runs [`.github/workflows/publish.yml`](.github/workflows/publish.yml),
which cross-builds all four platforms and publishes them as a single
multi-platform, signed OCI artifact to
`ghcr.io/brokenbots/criteria-adapter-noop:X.Y.Z` via the reusable
[`brokenbots/publish-adapter`](https://github.com/brokenbots/publish-adapter)
action.

## License

Apache-2.0. See [LICENSE](LICENSE).
