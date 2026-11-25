CREATE DATABASE catalogdb;

CREATE TABLE public.accounts (
    account_id bigint PRIMARY KEY,
    owner_name text NOT NULL,
    city text NOT NULL,
    status text NOT NULL CHECK (status IN ('ACTIVE', 'SUSPENDED', 'CANCELLED')),
    effective_at timestamp(6) NOT NULL
);

ALTER TABLE public.accounts REPLICA IDENTITY FULL;
CREATE PUBLICATION p2_accounts FOR TABLE public.accounts;

INSERT INTO public.accounts VALUES
    (101, 'Анна Романова', 'Казань', 'ACTIVE', TIMESTAMP '2026-02-20 09:00:00'),
    (102, 'Илья Орлов', 'Омск', 'ACTIVE', TIMESTAMP '2026-02-25 11:00:00');

CREATE ROLE p2_cdc LOGIN REPLICATION PASSWORD 'cdc-demo';
GRANT CONNECT ON DATABASE operations TO p2_cdc;
GRANT USAGE ON SCHEMA public TO p2_cdc;
GRANT SELECT ON public.accounts TO p2_cdc;
