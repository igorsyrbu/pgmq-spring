create table if not exists orders (
    id           text primary key,
    customer     text   not null,
    total_cents  bigint not null
);

create table if not exists order_confirmations (
    order_id     text primary key,
    confirmed_at timestamptz not null
);
