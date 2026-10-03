create table if not exists invoices (
    id           text primary key,
    customer     text   not null,
    total_cents  bigint not null
);

create table if not exists invoice_emails (
    invoice_id   text primary key,
    sent_at      timestamptz not null
);

create table if not exists audit_log (
    message_id   bigint primary key,
    subject      text not null,
    action       text not null
);
