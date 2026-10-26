-- Write access for the least-privileged service role (created by
-- deploy/postgres/initdb). The recorder inserts rows and the retention job deletes
-- them; nothing updates. Guarded so the migration also applies where the role does not
-- exist (local development, integration tests).

-- +goose Up
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'airq_app') THEN
        GRANT SELECT, INSERT, DELETE ON upstream_responses TO airq_app;
    END IF;
END
$$;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'airq_app') THEN
        REVOKE INSERT, DELETE ON upstream_responses FROM airq_app;
    END IF;
END
$$;
-- +goose StatementEnd
