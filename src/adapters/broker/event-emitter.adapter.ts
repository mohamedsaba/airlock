import { EventEmitter } from 'events';
import { IBrokerAdapter } from '../../core/interfaces/adapter.interfaces';
import { AirLockEvent } from '../../core/interfaces/airlock-types.interface';

export class EventEmitterBrokerAdapter implements IBrokerAdapter {
  constructor(private readonly emitter: EventEmitter) {}

  async publish(event: AirLockEvent): Promise<void> {
    this.emitter.emit('airlock.event', event);
  }
}
