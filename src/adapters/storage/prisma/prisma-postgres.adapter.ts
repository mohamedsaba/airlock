import * as crypto from 'crypto';
// @ts-expect-error - Prisma might not be fully generated in this environment
import { Prisma } from '@prisma/client';
import { IStorageAdapter } from '../../../core/interfaces/adapter.interfaces';
import { AirLockMessage } from '../../../core/interfaces/airlock-types.interface';

export class PrismaPostgresAdapter implements IStorageAdapter {
  constructor(
    private readonly prisma: any,
    private readonly workerId: string,
  ) {}

  async claimLeases(batchSize: number, leaseTtlMs: number): Promise<AirLockMessage[]> {
    return await this.prisma.$transaction(async (tx: any) => {
      // Step 1: Fetch IDs using SKIP LOCKED
      const rows = await tx.$queryRaw<any[]>`
        SELECT id, payload, "payloadSize" as "payloadSize"
        FROM airlock_messages
        WHERE status IN ('PENDING', 'IN_FLIGHT')
          AND "next_retry_at" <= CURRENT_TIMESTAMP
          AND ("locked_until" IS NULL OR "locked_until" < CURRENT_TIMESTAMP)
        ORDER BY "next_retry_at" ASC
        LIMIT ${batchSize}
        FOR UPDATE SKIP LOCKED
      `;

      if (rows.length === 0) return [];

      const ids = rows.map((r: any) => r.id);

      // Step 2: Update status
      await tx.$executeRaw`
        UPDATE airlock_messages
        SET status = 'IN_FLIGHT',
            locked_by = ${this.workerId},
            locked_until = CURRENT_TIMESTAMP + (${leaseTtlMs} * INTERVAL '1 millisecond'),
            first_attempt_at = COALESCE(first_attempt_at, CURRENT_TIMESTAMP),
            last_attempt_at = CURRENT_TIMESTAMP
        WHERE id IN (${Prisma.join(ids)})
      `;

      return rows as unknown as AirLockMessage[];
    });
  }

  async markProcessed(id: string, workerId: string): Promise<void> {
    await this.prisma.$executeRaw`
      UPDATE airlock_messages
      SET status = 'PROCESSED',
          processed_at = CURRENT_TIMESTAMP,
          locked_by = NULL,
          locked_until = NULL
      WHERE id = ${id} AND locked_by = ${workerId}
    `;
  }

  async scheduleRetry(
    id: string,
    workerId: string,
    nextRetryAt: Date,
    errorReason: string,
    maxRetries: number,
    retryDelayMs?: number,
  ): Promise<void> {
    const delay = retryDelayMs ?? 10000;
    await this.prisma.$executeRaw`
      UPDATE airlock_messages
      SET status = CASE WHEN retry_count + 1 >= ${maxRetries} THEN 'FAILED' ELSE 'PENDING' END,
          retry_count = retry_count + 1,
          next_retry_at = CURRENT_TIMESTAMP + (${delay} * INTERVAL '1 millisecond'),
          error_reason = ${errorReason},
          locked_by = NULL,
          locked_until = NULL
      WHERE id = ${id} AND locked_by = ${workerId}
    `;
  }

  async insertMessage(
    message: Partial<AirLockMessage>,
    transactionManager?: any,
  ): Promise<string> {
    const client = transactionManager || this.prisma;
    const id = message.id || crypto.randomUUID();
    
    await client.$executeRaw`
      INSERT INTO airlock_messages (
        id, aggregate_type, aggregate_id, event_type, partition_key, 
        payload, "payloadSize", status, next_retry_at
      ) VALUES (
        ${id}, ${message.aggregateType}, ${message.aggregateId}, 
        ${message.eventType}, ${message.partitionKey}, ${message.payload}, 
        ${message.payloadSize || 0}, 'PENDING', CURRENT_TIMESTAMP
      )
    `;

    return id;
  }

  async pruneProcessedMessages(retentionDays: number, chunkSize: number): Promise<number> {
    const result = await this.prisma.$executeRaw`
      WITH gc AS (
        SELECT id FROM airlock_messages
        WHERE status = 'PROCESSED' AND processed_at < CURRENT_TIMESTAMP - (${retentionDays} * INTERVAL '1 day')
        ORDER BY processed_at ASC
        LIMIT ${chunkSize}
        FOR UPDATE SKIP LOCKED
      )
      DELETE FROM airlock_messages
      WHERE id IN (SELECT id FROM gc)
    `;

    return result;
  }

  async verifySchema(): Promise<void> {
    // Placeholder for schema verification
    await this.prisma.$queryRaw`SELECT 1 FROM airlock_messages LIMIT 1`;
  }
}
