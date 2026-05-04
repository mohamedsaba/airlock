import { IStorageAdapter } from '../../src/core/interfaces/adapter.interfaces';
import { AirLockMessage } from '../../src/core/interfaces/airlock-types.interface';

export class MockStorageAdapter implements IStorageAdapter {
  public messages: AirLockMessage[] = [];
  public errorRate = 0;
  public latencyMs = 0;

  private async simulate() {
    if (this.latencyMs > 0) {
      await new Promise(resolve => setTimeout(resolve, this.latencyMs));
    }
    if (this.errorRate > 0 && Math.random() < this.errorRate) {
      throw new Error('Simulated Storage Error');
    }
  }

  async claimLeases(batchSize: number, leaseTtlMs: number): Promise<AirLockMessage[]> {
    await this.simulate();
    const now = new Date();
    const claimable = this.messages
      .filter(m => (m.status === 'PENDING' || m.status === 'IN_FLIGHT') &&
        m.nextRetryAt <= now &&
        (!m.lockedUntil || m.lockedUntil < now))
      .slice(0, batchSize);

    for (const m of claimable) {
      m.status = 'IN_FLIGHT';
      m.lockedUntil = new Date(Date.now() + leaseTtlMs);
    }

    return claimable;
  }

  async markProcessed(id: string, workerId: string): Promise<void> {
    await this.simulate();
    const m = this.messages.find(msg => msg.id === id);
    if (m) {
      m.status = 'PROCESSED';
      m.lockedUntil = undefined;
    }
  }

  async scheduleRetry(id: string, workerId: string, nextRetryAt: Date, errorReason: string, maxRetries: number): Promise<void> {
    await this.simulate();
    const m = this.messages.find(msg => msg.id === id);
    if (m) {
      m.retryCount++;
      if (m.retryCount >= maxRetries) {
        m.status = 'FAILED';
      } else {
        m.status = 'PENDING';
      }
      m.nextRetryAt = nextRetryAt;
      m.lockedUntil = undefined;
      m.errorReason = errorReason;
    }
  }

  async insertMessage(message: Partial<AirLockMessage>, transactionManager: any): Promise<string> {
    const fullMsg = {
      ...message,
      id: message.id || Math.random().toString(36).substring(7),
      status: message.status || 'PENDING',
      retryCount: message.retryCount || 0,
      nextRetryAt: message.nextRetryAt || new Date(),
      createdAt: new Date(),
    } as AirLockMessage;
    this.messages.push(fullMsg);
    return fullMsg.id;
  }

  async verifySchema(): Promise<void> {
    await this.simulate();
  }
}


