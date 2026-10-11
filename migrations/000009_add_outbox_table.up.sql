CREATE TABLE outbox (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE DEFAULT gen_random_uuid(),
    event_type TEXT NOT NULL,
    partition_key TEXT NOT NULL, -- account id
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);

-- idx_outbox_unpublished for quickly search for unpublished events
CREATE INDEX idx_outbox_unpublished ON outbox(id) WHERE published_at IS NULL;