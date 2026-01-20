# sidereal-control

Deployment orchestration and control plane for Sidereal.

## Overview

The control plane is responsible for:

- Receiving build completion notifications
- Registering functions with the scheduler
- Provisioning and terminating worker VMs
- Tracking active deployments per project/environment

## State Machine

Deployments follow a strict state machine enforced at compile time:

```text
Pending ──▶ Registering ──▶ Active ──▶ Superseded
                │              │
                ▼              ▼
              Failed      Terminated
```

## Configuration

Configuration is loaded from `control.toml` and environment variables:

```toml
[server]
listen_addr = "0.0.0.0:8083"

[database]
url = "postgres://localhost/sidereal"

[scheduler]
url = "http://localhost:8082"

[deployment]
strategy = "simple"  # simple, intent_log, or event_sourced
```

Environment variables use the `SIDEREAL_CONTROL_` prefix with `__` as separator:

```bash
SIDEREAL_CONTROL_DATABASE__URL=postgres://localhost/sidereal
SIDEREAL_CONTROL_SERVER__LISTEN_ADDR=0.0.0.0:9000
```
