JOBS_TABLE_SCHEMA = """
create table if not exists jobs (
    job_id                    text primary key,
    serialised_job            text not null,
    state                     text not null,
    priority                  integer default 0,
    created_at                text,
    started_at                text,
    recovery_started_at       text,
    completed_at              text,
    idempotency_hash          text
);
"""

JOB_OUTPUTS_TABLE_SCHEMA = """
create table if not exists job_outputs (
    job_id                    text primary key,
    output                    text not null,
    recovery                  text not null,
    foreign key (job_id)      references jobs(job_id)
);
"""

JOB_ERRORS_TABLE_SCHEMA = """
create table if not exists job_errors (
    job_id                    text,
    error_blob                text not null,
    error_text                text not null,
    recovery                  text not null,
    foreign key (job_id)      references jobs(job_id)
);
"""

CLAIMED_JOBS_TABLE_SCHEMA = """
create table if not exists claimed_jobs (
    job_id                    text primary key,
    claimed_at                text,
    claimed_by                text not null,
    foreign key (job_id)      references jobs(job_id)
);
"""

EVENTS_TABLE_SCHEMA = """
create table if not exists events (
    event_id                  integer primary key autoincrement,
    workflow_id               text,
    job_id                    text,
    event_type                text not null,
    event_blob                text not null,
    created_at                text
);
"""

JOBS_INDEX = """
CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs(state);
"""

JOBS_PRIORITY_INDEX = """
CREATE INDEX IF NOT EXISTS idx_jobs_priority ON jobs(state, priority DESC, created_at ASC);
"""

JOBS_IDEMPOTENCY_INDEX = """
CREATE INDEX IF NOT EXISTS idx_jobs_idempotency_hash ON jobs(idempotency_hash);
"""
