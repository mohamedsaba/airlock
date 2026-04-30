import { Entity, Column, PrimaryColumn, Index, CreateDateColumn } from 'typeorm';

@Entity('airlock_messages')
@Index('idx_airlock_due', ['nextRetryAt'], { where: "status IN ('PENDING', 'IN_FLIGHT')" })
@Index('idx_airlock_failed', ['createdAt'], { where: "status = 'FAILED'" })
@Index('idx_airlock_processed', ['processedAt'], { where: "status = 'PROCESSED'" })
@Index('idx_airlock_partition', ['partitionKey', 'createdAt'], {
  where: "status IN ('PENDING', 'IN_FLIGHT')",
})
export class AirLockMessageEntity {
  @PrimaryColumn('uuid')
  id!: string;

  @Column({ name: 'aggregate_type', length: 64 })
  aggregateType!: string;

  @Column({ name: 'aggregate_id', length: 64 })
  aggregateId!: string;

  @Column({ name: 'event_type', length: 128 })
  eventType!: string;

  @Column({ name: 'partition_key', length: 64 })
  partitionKey!: string;

  @Column('jsonb')
  payload!: any;

  @Column({ name: 'payload_size' })
  payloadSize!: number;

  @Column({ name: 'idempotency_key', length: 128, nullable: true })
  @Index('uq_airlock_idem', { unique: true, where: 'idempotency_key IS NOT NULL' })
  idempotencyKey?: string;

  @Column({ length: 16, default: 'PENDING' })
  status!: 'PENDING' | 'IN_FLIGHT' | 'PROCESSED' | 'FAILED';

  @Column({ name: 'retry_count', default: 0 })
  retryCount!: number;

  @Column('timestamptz', { name: 'next_retry_at', default: () => 'CURRENT_TIMESTAMP' })
  nextRetryAt!: Date;

  @Column({ name: 'locked_by', length: 64, nullable: true })
  lockedBy?: string;

  @Column('timestamptz', { name: 'locked_until', nullable: true })
  lockedUntil?: Date;

  @Column('text', { name: 'error_reason', nullable: true })
  errorReason?: string;

  @CreateDateColumn({ name: 'created_at', type: 'timestamptz' })
  createdAt!: Date;

  @Column('timestamptz', { name: 'first_attempt_at', nullable: true })
  firstAttemptAt?: Date;

  @Column('timestamptz', { name: 'last_attempt_at', nullable: true })
  lastAttemptAt?: Date;

  @Column('timestamptz', { name: 'processed_at', nullable: true })
  processedAt?: Date;
}

