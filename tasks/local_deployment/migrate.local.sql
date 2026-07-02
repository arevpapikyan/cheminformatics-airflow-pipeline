-- Schema initialisation for local development (Docker Compose).
-- In production this is managed by Alembic — tasks NEVER touch DDL.

CREATE TYPE task_status AS ENUM ('created', 'running', 'success', 'failed');

CREATE TABLE IF NOT EXISTS users (
    id          SERIAL       PRIMARY KEY,
    email       VARCHAR      NOT NULL UNIQUE,
    name        VARCHAR      NOT NULL,
    surname     VARCHAR      NOT NULL,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS projects (
    id          SERIAL       PRIMARY KEY,
    name        VARCHAR      NOT NULL,
    description TEXT,
    owner_id    INTEGER      NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS experiments (
    id          SERIAL       PRIMARY KEY,
    name        VARCHAR      NOT NULL,
    project_id  INTEGER      NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    created_by  INTEGER      NOT NULL REFERENCES users(id),
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tasks (
    id             VARCHAR(36)  PRIMARY KEY,
    task_type      VARCHAR(64)  NOT NULL,
    status         task_status  NOT NULL DEFAULT 'created',
    params         JSONB        NOT NULL DEFAULT '{}',
    error          TEXT,
    experiment_id  INTEGER      NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
    created_by     INTEGER      NOT NULL REFERENCES users(id),
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS artifacts (
    id            VARCHAR(36)   PRIMARY KEY,
    task_id       VARCHAR(36)   NOT NULL REFERENCES tasks(id),
    s3_key        VARCHAR(512)  NOT NULL,
    filename      VARCHAR(256)  NOT NULL,
    content_type  VARCHAR(128),
    meta          JSONB         NOT NULL DEFAULT '{}',
    created_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

-- Seed: user, project, experiment
INSERT INTO users (id, email, name, surname)
VALUES (1, 'dev@example.com', 'Dev', 'User')
ON CONFLICT DO NOTHING;

INSERT INTO projects (id, name, description, owner_id)
VALUES (1, 'Local Dev Project', 'Seed project for local development', 1)
ON CONFLICT DO NOTHING;

INSERT INTO experiments (id, name, project_id, created_by)
VALUES (1, 'Test Experiments', 1, 1)
ON CONFLICT DO NOTHING;
