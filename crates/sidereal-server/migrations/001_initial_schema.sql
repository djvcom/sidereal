-- Sidereal: Initial schema
--
-- Creates all PostgreSQL tables required by the Sidereal platform.
-- Individual services also call ensure_schema() on startup, but this
-- migration provides versioned schema management for production deployments.

--------------------------------------------------------------------------------
-- Deployments (sidereal-control)
--------------------------------------------------------------------------------

-- Deployment records tracking all deployments and their states
CREATE TABLE IF NOT EXISTS deployments (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    environment TEXT NOT NULL,
    commit_sha TEXT NOT NULL,
    artifact_url TEXT NOT NULL,
    functions JSONB NOT NULL,
    state TEXT NOT NULL,
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Active deployment per project/environment combination
-- Only one deployment can be active per (project_id, environment) pair
CREATE TABLE IF NOT EXISTS active_deployments (
    project_id TEXT NOT NULL,
    environment TEXT NOT NULL,
    deployment_id TEXT NOT NULL REFERENCES deployments(id) ON DELETE CASCADE,
    PRIMARY KEY (project_id, environment)
);

-- Indices for common query patterns
CREATE INDEX IF NOT EXISTS idx_deployments_project_env
    ON deployments (project_id, environment);

CREATE INDEX IF NOT EXISTS idx_deployments_state
    ON deployments (state);

CREATE INDEX IF NOT EXISTS idx_deployments_created_at
    ON deployments (created_at DESC);

COMMENT ON TABLE deployments IS 'All deployment records with their current state';
COMMENT ON TABLE active_deployments IS 'Currently active deployment per project/environment';
COMMENT ON COLUMN deployments.functions IS 'JSON array of FunctionMetadata objects';
COMMENT ON COLUMN deployments.state IS 'One of: pending, registering, active, superseded, failed, terminated';

--------------------------------------------------------------------------------
-- Message Queue (sidereal-state)
--------------------------------------------------------------------------------

-- Default queue table for PostgreSQL queue backend
CREATE TABLE IF NOT EXISTS sidereal_queue (
    id BIGSERIAL PRIMARY KEY,
    queue_name TEXT NOT NULL,
    payload BYTEA NOT NULL,
    attempt INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    visible_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for efficient queue polling (SELECT FOR UPDATE SKIP LOCKED)
CREATE INDEX IF NOT EXISTS idx_sidereal_queue_receive
    ON sidereal_queue (queue_name, visible_at)
    WHERE visible_at <= NOW();

COMMENT ON TABLE sidereal_queue IS 'Message queue for async task processing';
COMMENT ON COLUMN sidereal_queue.visible_at IS 'Message becomes visible for processing after this time';
COMMENT ON COLUMN sidereal_queue.attempt IS 'Number of delivery attempts (for retry tracking)';
