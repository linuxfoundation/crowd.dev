CREATE TABLE stewards (
    user_id      TEXT        PRIMARY KEY,
    username     TEXT        NOT NULL,
    display_name TEXT        NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
