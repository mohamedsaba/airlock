export function generatePostgresMigration(): string {
  return `
CREATE TABLE airlock_messages (
  id              UUID PRIMARY KEY,
  aggregate_type  VARCHAR(64)  NOT NULL,
  aggregate_id    VARCHAR(64)  NOT NULL,
  event_type      VARCHAR(128) NOT NULL,
  partition_key   VARCHAR(64)  NOT NULL,
  payload         JSONB        NOT NULL,
  payload_size    INT          NOT NULL,
  idempotency_key VARCHAR(128) NULL,
  status          VARCHAR(16)  NOT NULL DEFAULT 'PENDING',
  retry_count     INT          NOT NULL DEFAULT 0,
  next_retry_at   TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP,
  locked_by       VARCHAR(64)  NULL,
  locked_until    TIMESTAMPTZ  NULL,
  error_reason    TEXT         NULL,
  created_at      TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP,
  first_attempt_at TIMESTAMPTZ NULL,
  last_attempt_at TIMESTAMPTZ  NULL,
  processed_at    TIMESTAMPTZ  NULL
);

CREATE INDEX idx_airlock_due ON airlock_messages (next_retry_at)
  WHERE status IN ('PENDING', 'IN_FLIGHT');

CREATE INDEX idx_airlock_failed ON airlock_messages (created_at)
  WHERE status = 'FAILED';

CREATE INDEX idx_airlock_processed ON airlock_messages (processed_at)
  WHERE status = 'PROCESSED';

CREATE UNIQUE INDEX uq_airlock_idem ON airlock_messages (idempotency_key)
  WHERE idempotency_key IS NOT NULL;

CREATE INDEX idx_airlock_partition ON airlock_messages (partition_key, created_at)
  WHERE status IN ('PENDING', 'IN_FLIGHT');

CREATE TABLE airlock_meta (
  key   VARCHAR(64) PRIMARY KEY,
  value VARCHAR(64) NOT NULL
);

INSERT INTO airlock_meta (key, value) VALUES ('schema_version', '0');
`;
}

// Simple CLI entry point
if (require.main === module) {
  const args = process.argv.slice(2);
  if (args.includes('migration:generate')) {
    console.log(generatePostgresMigration());
  }
}
