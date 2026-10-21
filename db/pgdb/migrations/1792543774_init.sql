-- +goose Up
CREATE TABLE IF NOT EXISTS user_presence(
    acct_id UUID PRIMARY KEY,
    last_seen TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- +goose Down
DROP TABLE IF EXISTS user_presence;