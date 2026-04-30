import { Inject, Injectable } from '@nestjs/common';
import { AIRLOCK_STORAGE_ADAPTER, AIRLOCK_OPTIONS } from './constants';
import { IStorageAdapter } from '../core/interfaces/adapter.interfaces';
import { assertSerializable } from '../core/utils/assert-serializable';
import { buildCloudEvent } from '../core/utils/build-cloud-event';
import { AirLockMessage } from '../core/interfaces/airlock-types.interface';
import { PayloadTooLargeError } from '../core/utils/errors';

export interface PublishOptions {
  aggregateType: string;
  aggregateId: string;
  eventType: string;
  partitionKey?: string;
  idempotencyKey?: string;
  transaction?: any;
  subject?: string;
}

@Injectable()
export class AirlockService {
  constructor(
    @Inject(AIRLOCK_STORAGE_ADAPTER) private readonly storage: IStorageAdapter,
    @Inject(AIRLOCK_OPTIONS) private readonly options: any,
  ) {}

  async publish(data: Record<string, any>, options: PublishOptions): Promise<string> {
    // Rule 10
    assertSerializable(data);

    const event = buildCloudEvent(data, {
      type: options.eventType,
      source: this.options.cloudEvents?.source || 'airlock',
      subject: options.subject,
    });

    const payloadStr = JSON.stringify(event);
    const payloadSize = Buffer.byteLength(payloadStr, 'utf8');

    // Rule 3
    const maxPayloadBytes = this.options.worker?.maxPayloadBytes || 1_048_576;
    if (payloadSize > maxPayloadBytes) {
      throw new PayloadTooLargeError(payloadSize, maxPayloadBytes);
    }

    const message: Partial<AirLockMessage> = {
      id: event.id,
      aggregateType: options.aggregateType,
      aggregateId: options.aggregateId,
      eventType: options.eventType,
      partitionKey: options.partitionKey || options.aggregateId,
      payload: event,
      payloadSize,
      idempotencyKey: options.idempotencyKey,
      status: 'PENDING',
    };

    return await this.storage.insertMessage(message, options.transaction);
  }
}
