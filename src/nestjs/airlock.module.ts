import {
  DynamicModule,
  Module,
  Global,
  Provider,
  OnApplicationBootstrap,
  OnApplicationShutdown,
  Inject,
  Logger,
} from '@nestjs/common';
import { randomUUID } from 'crypto';
import { EventEmitter } from 'events';
import { DataSource } from 'typeorm';
import { AirlockService } from './airlock.service';
import {
  AIRLOCK_OPTIONS,
  AIRLOCK_STORAGE_ADAPTER,
  AIRLOCK_BROKER_ADAPTER,
} from './constants';
import { TypeOrmPostgresAdapter } from '../adapters/storage/typeorm/typeorm-postgres.adapter';
import { EventEmitterBrokerAdapter } from '../adapters/broker/event-emitter.adapter';
import { RelayWorker, RelayWorkerConfig } from '../core/worker/relay-worker';
import { IStorageAdapter } from '../core/interfaces/adapter.interfaces';

export interface AirlockModuleOptions {
  storage: {
    adapter: 'typeorm-postgres';
    dataSource?: DataSource;
  };
  broker: {
    adapter: 'event-emitter';
    emitter?: EventEmitter;
  };
  worker?: Partial<RelayWorkerConfig> & {
    enabled?: boolean;
    shutdownTimeoutMs?: number;
  };
  cloudEvents?: {
    source: string;
  };
}

export interface AirlockModuleAsyncOptions {
  imports?: any[];
  inject?: any[];
  useFactory: (...args: any[]) => Promise<AirlockModuleOptions> | AirlockModuleOptions;
}

@Global()
@Module({
  providers: [AirlockService],
  exports: [AirlockService],
})
export class AirlockModule implements OnApplicationBootstrap, OnApplicationShutdown {
  private readonly logger = new Logger(AirlockModule.name);

  constructor(
    @Inject(AIRLOCK_OPTIONS) private readonly options: AirlockModuleOptions,
    @Inject(AIRLOCK_STORAGE_ADAPTER) private readonly storage: IStorageAdapter,
    @Inject('RELAY_WORKER') private readonly worker: RelayWorker,
  ) {}

  static forRootAsync(asyncOptions: AirlockModuleAsyncOptions): DynamicModule {
    const optionsProvider: Provider = {
      provide: AIRLOCK_OPTIONS,
      useFactory: asyncOptions.useFactory,
      inject: asyncOptions.inject || [],
    };

    const workerId = randomUUID();

    const storageProvider: Provider = {
      provide: AIRLOCK_STORAGE_ADAPTER,
      useFactory: (options: AirlockModuleOptions, dataSource: DataSource) => {
        if (options.storage.adapter === 'typeorm-postgres') {
          return new TypeOrmPostgresAdapter(
            options.storage.dataSource || dataSource,
            workerId,
          );
        }
        throw new Error(`Unsupported storage adapter: ${options.storage.adapter}`);
      },
      inject: [AIRLOCK_OPTIONS, DataSource],
    };

    const brokerProvider: Provider = {
      provide: AIRLOCK_BROKER_ADAPTER,
      useFactory: (options: AirlockModuleOptions) => {
        if (options.broker.adapter === 'event-emitter') {
          return new EventEmitterBrokerAdapter(options.broker.emitter || new EventEmitter());
        }
        throw new Error(`Unsupported broker adapter: ${options.broker.adapter}`);
      },
      inject: [AIRLOCK_OPTIONS],
    };

    const workerProvider: Provider = {
      provide: 'RELAY_WORKER',
      useFactory: (
        storage: TypeOrmPostgresAdapter,
        broker: EventEmitterBrokerAdapter,
        options: AirlockModuleOptions,
      ) => {
        const config: RelayWorkerConfig = {
          pollIntervalMs: options.worker?.pollIntervalMs || 1000,
          batchSize: options.worker?.batchSize || 100,
          concurrency: options.worker?.concurrency || 4,
          leaseTtlMs: options.worker?.leaseTtlMs || 30000,
          maxBatchBytes: options.worker?.maxBatchBytes || 10 * 1024 * 1024,
          maxRetries: options.worker?.maxRetries || 8,
        };
        const logger = new Logger('RelayWorker');
        return new RelayWorker(storage, broker, config, logger, workerId);
      },
      inject: [AIRLOCK_STORAGE_ADAPTER, AIRLOCK_BROKER_ADAPTER, AIRLOCK_OPTIONS],
    };

    return {
      module: AirlockModule,
      imports: asyncOptions.imports || [],
      providers: [optionsProvider, storageProvider, brokerProvider, workerProvider],
      exports: [AirlockService],
    };
  }

  async onApplicationBootstrap() {
    try {
      await this.storage.verifySchema();
    } catch (error: any) {
      this.logger.error(`Airlock initialization failed: ${error.message}`);
      process.exit(1);
    }

    if (this.options.worker?.enabled !== false) {
      await this.worker.start();
    }
  }

  async onApplicationShutdown() {
    if (this.options.worker?.enabled !== false) {
      await this.worker.stop(this.options.worker?.shutdownTimeoutMs || 10000);
    }
  }
}
