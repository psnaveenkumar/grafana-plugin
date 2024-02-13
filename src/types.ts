import { DataQuery, DataSourceJsonData } from '@grafana/data';

export interface KafkaDataSourceOptions extends DataSourceJsonData {
  bootstrapServers: string;
  securityProtocol: string;
  saslMechanisms: string;
  saslUsername: string;
  debug: string;
  healthcheckTimeout: number;
  dataType: string;
  consumerName: string;
}

export interface KafkaSecureJsonData {
  saslPassword?: string;
}

export interface KafkaQuery extends DataQuery {
  topicName: string;
  partition: number;
  withStreaming: boolean;
}

export const defaultQuery: Partial<KafkaQuery> = {
  partition: 0,
  withStreaming: true,
};
