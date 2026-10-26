-- Audit log of every request this service sent to a third-party API and what came
-- back. Written asynchronously and in batches by the response recorder; pruned by the
-- retention job (DB_RETENTION).
CREATE TABLE upstream_responses (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    created_at     timestamptz      NOT NULL DEFAULT now(),
    request_id     text,
    upstream       text             NOT NULL,
    endpoint       text             NOT NULL,
    query          jsonb            NOT NULL DEFAULT '{}'::jsonb,
    attempt        smallint         NOT NULL CHECK (attempt >= 1),
    status_code    smallint         CHECK (status_code BETWEEN 100 AND 599),
    outcome        text             NOT NULL,
    duration_ms    double precision NOT NULL CHECK (duration_ms >= 0),
    response_bytes integer          NOT NULL DEFAULT 0 CHECK (response_bytes >= 0),
    response_body  jsonb,
    body_truncated boolean          NOT NULL DEFAULT false,
    error          text
);

COMMENT ON TABLE upstream_responses IS 'One row per upstream HTTP attempt (including retries and failures). Credentials are never stored.';
COMMENT ON COLUMN upstream_responses.request_id IS 'X-Request-ID of the API request that triggered the call; joins logs to rows.';
COMMENT ON COLUMN upstream_responses.outcome IS 'success, timeout, network, rate_limited, server_error, client_error or bad_response.';
COMMENT ON COLUMN upstream_responses.response_body IS 'Vendor JSON body; NULL when absent, not valid JSON, or larger than RECORDER_MAX_BODY_BYTES (see body_truncated).';

-- Time-range scans and retention deletes. BRIN is tiny and ideal for append-only,
-- time-ordered data.
CREATE INDEX upstream_responses_created_at_brin ON upstream_responses USING brin (created_at);

-- Find the vendor calls behind a given API request.
CREATE INDEX upstream_responses_request_id_idx ON upstream_responses (request_id) WHERE request_id IS NOT NULL;

-- Incident investigation: recent failures per upstream.
CREATE INDEX upstream_responses_failures_idx ON upstream_responses (upstream, created_at) WHERE outcome <> 'success';
