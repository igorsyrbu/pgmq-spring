-- One row per ingest request, written in the same transaction as its messages.
create table if not exists ingestions (
    id          text primary key,
    reading_ct  integer not null,
    received_at timestamptz not null default now()
);

-- Written by the batch handler, one multi-row insert per batch.
create table if not exists readings (
    id      text primary key,
    device  text             not null,
    value   double precision not null
);

-- Readings the handler refused. Recorded rather than thrown: see ReadingBatchHandler.
create table if not exists rejected_readings (
    id      text primary key,
    reason  text not null
);
