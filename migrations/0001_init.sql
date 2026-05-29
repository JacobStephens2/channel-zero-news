-- Channel 0 News — durable artifacts only. Live game state lives in memory
-- (the per-room actors); these tables hold the prompt catalog, each room's
-- final responses, and the append-only archive of past rounds.
--
-- Migrated from the original MySQL schema (tblPrompts / tblResponses /
-- tblResponseArchive). The seven prompt1..prompt7 columns are normalized into a
-- single TEXT[] array.

CREATE TABLE prompt_sets (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT,
    author      TEXT,
    prompts     TEXT[] NOT NULL DEFAULT '{}',
    archived_at TIMESTAMPTZ
);

-- A room's finalized responses for the current round (overwritten each round,
-- cleared on archive). Mirrors tblResponses.
CREATE TABLE responses (
    id            BIGSERIAL PRIMARY KEY,
    room_code     TEXT NOT NULL,
    name          TEXT NOT NULL,
    partner       TEXT,
    prompt_set_id BIGINT REFERENCES prompt_sets(id) ON DELETE SET NULL,
    prompts       TEXT[] NOT NULL DEFAULT '{}',
    responses     TEXT[] NOT NULL DEFAULT '{}',
    signoff       TEXT,
    submitted_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (room_code, name)
);

-- Append-only history of archived rounds. Mirrors tblResponseArchive.
CREATE TABLE response_archive (
    id               BIGSERIAL PRIMARY KEY,
    archive_batch_id TEXT NOT NULL,
    room_code        TEXT,
    archived_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    name             TEXT,
    partner          TEXT,
    prompt_set_id    BIGINT,
    prompts          TEXT[] NOT NULL DEFAULT '{}',
    responses        TEXT[] NOT NULL DEFAULT '{}',
    signoff          TEXT
);

CREATE INDEX idx_archive_batch ON response_archive (archive_batch_id);
CREATE INDEX idx_archive_room ON response_archive (room_code);
