JOBS_TABLE_SCHEMA = """
create table if not exists jobs (
    job_id                    text primary key,
    serialised_job            text not null,
    state                     text not null,
    created_at                timestamp default current_timestamp,
    started_at                timestamp,
    completed_at              timestamp,
    duration_seconds          real,
    recovery_duration_seconds real
);
"""

JOB_OUTPUTS_TABLE_SCHEMA = """
create table if not exists job_outputs (
    job_id                    text primary key,
    output                    text not null,
    foreign key (job_id) references jobs(job_id)
);
"""

JOB_ERRORS_TABLE_SCHEMA = """
create table if not exists job_errors (
    job_id                    text,
    error_blob                text not null,
    foreign key (job_id) references jobs(job_id)
);
"""

CLAIMED_JOBS_TABLE_SCHEMA = """
create table if not exists claimed_jobs (
    job_id                    text primary key,
    claimed_at                timestamp default current_timestamp,
    claimed_by                text not null,
    foreign key (job_id) references jobs(job_id)
);
"""

JOBS_INDEX = """
CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs(state)
"""
