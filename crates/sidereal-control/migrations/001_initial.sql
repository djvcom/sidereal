-- Sidereal Control: Initial schema
--
-- This migration creates the tables required for deployment tracking.
-- Note: The PostgresStore also creates these tables dynamically with ensure_schema()
-- but this file is provided for documentation and manual database setup.

-- Deployment records
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

-- Comments for documentation
COMMENT ON TABLE deployments IS 'All deployment records with their current state';
COMMENT ON TABLE active_deployments IS 'Currently active deployment per project/environment';
COMMENT ON COLUMN deployments.functions IS 'JSON array of FunctionMetadata objects';
COMMENT ON COLUMN deployments.state IS 'One of: pending, registering, active, superseded, failed, terminated';
