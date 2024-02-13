import { defaults } from 'lodash';
import React, { ChangeEvent, PureComponent } from 'react';
import { InlineFormLabel, InlineFieldRow } from '@grafana/ui';
import { QueryEditorProps } from '@grafana/data';
import { DataSource } from './datasource';
import { defaultQuery, KafkaDataSourceOptions, KafkaQuery } from './types';

type Props = QueryEditorProps<DataSource, KafkaQuery, KafkaDataSourceOptions>;

export class QueryEditor extends PureComponent<Props> {
  onTopicNameChange = (event: ChangeEvent<HTMLInputElement>) => {
    const { onChange, query, onRunQuery } = this.props;
    onChange({ ...query, topicName: event.target.value });
    onRunQuery();
  };

  render() {
    const query = defaults(this.props.query, defaultQuery);
    const { topicName } = query;
    return (
        <>
          <div className="gf-form">
            <InlineFieldRow>
              <InlineFormLabel width={10}>Topic</InlineFormLabel>
              <input
                  className="gf-form-input width-14"
                  value={topicName || ''}
                  onChange={this.onTopicNameChange}
                  type="text"
              />
            </InlineFieldRow>
          </div>
        </>
    );
  }
}
