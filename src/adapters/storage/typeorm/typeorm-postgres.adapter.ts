import { DataSource, EntityManager, In } from 'typeorm';
import { IStorageAdapter } from '../../../core/interfaces/adapter.interfaces';
import { AirLockMessage } from '../../../core/interfaces/airlock-types.interface';
import { AirLockMessageEntity } from './airlock-message.entity';

export class TypeOrmPostgresAdapter implements IStorageAdapter {
  constructor(
    private readonly dataSource: DataSource,
    private readonly workerId: string,
  ) { }

  async claimLeases(batchSize: number, leaseTtlMs: number): Promise<AirLockMessage[]> {
    return await this.dataSource.transaction(async (manager) => {
      const messages = await manager
        .createQueryBuilder(AirLockMessageEntity, 'm')
        .select(['m.id', 'm.payload', 'm.payloadSize'])
        .where("m.status IN ('PENDING', 'IN_FLIGHT')")
        .andWhere('m.nextRetryAt <= CURRENT_TIMESTAMP')
        .andWhere('(m.lockedUntil IS NULL OR m.lockedUntil < CURRENT_TIMESTAMP)')
        .orderBy('m.nextRetryAt', 'ASC')
        .limit(batchSize)
        .setLock('pessimistic_write')
        .setOnLocked('skip_locked')
        .getMany();

      if (messages.length === 0) {
        return [];
      }

      const ids = messages.map((m) => m.id);

      await manager
        .createQueryBuilder()
        .update(AirLockMessageEntity)
        .set({
          status: 'IN_FLIGHT',
          lockedBy: this.workerId,
          lockedUntil: () => `CURRENT_TIMESTAMP + (${leaseTtlMs} * INTERVAL '1 millisecond')`,
          firstAttemptAt: () => `COALESCE(first_attempt_at, CURRENT_TIMESTAMP)`,
          lastAttemptAt: () => 'CURRENT_TIMESTAMP',
        })
        .where({ id: In(ids) })
        .execute();

      return messages as unknown as AirLockMessage[];
    });
  }

  async markProcessed(id: string, workerId: string): Promise<void> {
    await this.dataSource
      .createQueryBuilder()
      .update(AirLockMessageEntity)
      .set({
        status: 'PROCESSED',
        processedAt: () => 'CURRENT_TIMESTAMP',
        lockedBy: null,
        lockedUntil: null,
      })
      .where('id = :id AND lockedBy = :workerId', { id, workerId })
      .execute();
  }

  async scheduleRetry(
    id: string,
    workerId: string,
    nextRetryAt: Date, // Kept for interface compatibility but implementation uses SQL for Rule 5
    errorReason: string,
    maxRetries: number,
  ): Promise<void> {
    await this.dataSource
      .createQueryBuilder()
      .update(AirLockMessageEntity)
      .set({
        status: () => `CASE WHEN retry_count + 1 >= ${maxRetries} THEN 'FAILED' ELSE 'PENDING' END`,
        retryCount: () => 'retry_count + 1',
        nextRetryAt: () => `CURRENT_TIMESTAMP + INTERVAL '10 seconds'`, // Phase 0: fixed 10s backoff using DB time
        errorReason,
        lockedBy: null,
        lockedUntil: null,
      })
      .where('id = :id AND lockedBy = :workerId', { id, workerId })
      .execute();
  }

  async insertMessage(
    message: Partial<AirLockMessage>,
    transactionManager?: EntityManager,
  ): Promise<string> {
    const manager = transactionManager || this.dataSource.manager;

    try {
      const result = await manager.insert(AirLockMessageEntity, message);
      return result.identifiers[0].id;
    } catch (error: any) {
      // Postgres unique violation code
      if (error.code === '23505' && message.idempotencyKey) {
        const existing = await manager.findOne(AirLockMessageEntity, {
          where: { idempotencyKey: message.idempotencyKey },
          select: ['id'],
        });
        if (existing) {
          return existing.id;
        }
      }
      throw error;
    }
  }

  async verifySchema(): Promise<void> {
    try {
      const res = await this.dataSource.query(
        "SELECT value FROM airlock_meta WHERE key = 'schema_version'"
      );
      if (!res || res.length === 0 || res[0].value !== '0') {
        throw new Error(`Airlock schema version mismatch. Expected 0, found ${res?.[0]?.value || 'none'}. Please run migrations.`);
      }
    } catch (error: any) {
      if (error.message.includes('relation "airlock_meta" does not exist')) {
        throw new Error('Airlock tables not found. Please run migrations.');
      }
      throw error;
    }
  }
}
