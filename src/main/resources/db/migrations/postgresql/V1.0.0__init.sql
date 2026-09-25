CREATE TABLE last_seen(
  acct_id UUID PRIMARY KEY,
    acct_timestamp TIMESTAMP WITH TIME ZONE
);

CREATE INDEX last_seen_acct_id_idx ON last_seen(acct_id);