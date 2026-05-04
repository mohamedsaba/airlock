import * as amqp from 'amqplib';
import { IBrokerAdapter } from '../../core/interfaces/adapter.interfaces';
import { AirLockEvent } from '../../core/interfaces/airlock-types.interface';

export interface RabbitMQBrokerConfig {
  url: string | amqp.Options.Connect;
  exchange?: string;
  exchangeType?: string;
}

export class RabbitMQBrokerAdapter implements IBrokerAdapter {
  private connection?: any;
  private channel?: any;
  private readonly exchange: string;

  constructor(private readonly config: RabbitMQBrokerConfig) {
    this.exchange = config.exchange || 'airlock.events';
  }

  async publish(event: AirLockEvent): Promise<void> {
    const channel = await this.getChannel();

    const anyEvent = event as any;
    channel.publish(
      this.exchange,
      event.type,
      Buffer.from(JSON.stringify(event)),
      {
        messageId: anyEvent.id,
        timestamp: new Date(anyEvent.time).getTime(),
        contentType: 'application/json',
      }
    );

    await channel.waitForConfirms();
  }

  private async getChannel(): Promise<amqp.ConfirmChannel> {
    if (this.channel) return this.channel;

    if (!this.connection) {
      this.connection = await amqp.connect(this.config.url);
    }

    this.channel = await this.connection.createConfirmChannel();
    
    if (this.config.exchange) {
      await this.channel.assertExchange(this.exchange, this.config.exchangeType || 'topic', {
        durable: true,
      });
    }

    return this.channel;
  }

  async disconnect(): Promise<void> {
    if (this.channel) {
      await this.channel.close();
      this.channel = undefined;
    }
    if (this.connection) {
      await this.connection.close();
      this.connection = undefined;
    }
  }
}
