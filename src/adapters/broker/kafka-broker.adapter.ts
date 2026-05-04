import { Kafka, Producer, ProducerConfig } from 'kafkajs';
import { IBrokerAdapter } from '../../core/interfaces/adapter.interfaces';
import { AirLockEvent } from '../../core/interfaces/airlock-types.interface';
import { BrokerTimeoutError } from '../../core/utils/errors';

export interface KafkaBrokerConfig {
  kafkaConfig: any; // kafkajs.KafkaConfig
  producerConfig?: ProducerConfig;
  publishTimeoutMs?: number;
}

export class KafkaBrokerAdapter implements IBrokerAdapter {
  private readonly kafka: Kafka;
  private readonly producer: Producer;
  private readonly publishTimeoutMs: number;
  private isConnected = false;

  constructor(config: KafkaBrokerConfig) {
    this.kafka = new Kafka(config.kafkaConfig);
    this.producer = this.kafka.producer(config.producerConfig);
    this.publishTimeoutMs = config.publishTimeoutMs || 3000;
  }

  async publish(event: AirLockEvent): Promise<void> {
    if (!this.isConnected) {
      await this.producer.connect();
      this.isConnected = true;
    }

    const publishPromise = this.producer.send({
      topic: event.type,
      messages: [
        {
          key: event.id,
          value: JSON.stringify(event),
        },
      ],
    });

    let timeoutId: NodeJS.Timeout;
    const timeoutPromise = new Promise((_, reject) => {
      timeoutId = setTimeout(() => reject(new BrokerTimeoutError(`Kafka publish timed out after ${this.publishTimeoutMs}ms`)), this.publishTimeoutMs);
    });

    try {
      await Promise.race([publishPromise, timeoutPromise]);
    } finally {
      clearTimeout(timeoutId!);
    }
  }

  async disconnect(): Promise<void> {
    if (this.isConnected) {
      await this.producer.disconnect();
      this.isConnected = false;
    }
  }
}
