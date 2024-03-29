package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/live"
	"strings"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/hoptical/grafana-kafka-datasource/pkg/kafka_client"
)

var (
	_ backend.QueryDataHandler      = (*KafkaDatasource)(nil)
	_ backend.CheckHealthHandler    = (*KafkaDatasource)(nil)
	_ backend.StreamHandler         = (*KafkaDatasource)(nil)
	_ instancemgmt.InstanceDisposer = (*KafkaDatasource)(nil)
)

func NewKafkaInstance(s backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
	settings, err := getDatasourceSettings(s)

	if err != nil {
		return nil, err
	}

	kafka_client := kafka_client.NewKafkaClient(*settings)
	dataSource := &KafkaDatasource{kafka_client}
	return dataSource, nil
}

func getDatasourceSettings(s backend.DataSourceInstanceSettings) (*kafka_client.Options, error) {
	settings := &kafka_client.Options{}

	if err := json.Unmarshal(s.JSONData, settings); err != nil {
		return nil, err
	}

	if sasl_password, exists := s.DecryptedSecureJSONData["saslPassword"]; exists {
		log.DefaultLogger.Info("saslPassword exits", "pasword", sasl_password)
		if sasl_password != "" {
			settings.SaslPassword = sasl_password
		}
	}

	return settings, nil
}

type KafkaDatasource struct {
	client kafka_client.KafkaClient
}

func (d *KafkaDatasource) Dispose() {
	// Clean up datasource instance resources.
	log.DefaultLogger.Info("dispose called")
}

func (d *KafkaDatasource) QueryData(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	log.DefaultLogger.Info("QueryData called", "request", req)

	response := backend.NewQueryDataResponse()

	for _, q := range req.Queries {
		res := d.query(ctx, req.PluginContext, q)

		response.Responses[q.RefID] = res
	}

	return response, nil
}

type queryModel struct {
	Topic         string `json:"topicName"`
	Partition     int32  `json:"partition"`
	WithStreaming bool   `json:"withStreaming"`
}

func (d *KafkaDatasource) query(_ context.Context, pCtx backend.PluginContext, query backend.DataQuery) backend.DataResponse {
	response := backend.DataResponse{}
	var qm queryModel
	response.Error = json.Unmarshal(query.JSON, &qm)

	if response.Error != nil {
		return response
	}

	frame := data.NewFrame("response")

	frame.Fields = append(frame.Fields,
		data.NewField("time", nil, []time.Time{query.TimeRange.From, query.TimeRange.To}),
		data.NewField("values", nil, []int64{0, 0}),
	)

	topic := qm.Topic
	partition := qm.Partition
	if qm.WithStreaming {
		channel := live.Channel{
			Scope:     live.ScopeDatasource,
			Namespace: pCtx.DataSourceInstanceSettings.UID,
			Path:      fmt.Sprintf("%v_%d_%v", topic, partition, query.RefID),
		}
		frame.SetMeta(&data.FrameMeta{Channel: channel.String()})
	}

	response.Frames = append(response.Frames, frame)

	return response
}

func (d *KafkaDatasource) CheckHealth(_ context.Context, req *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
	log.DefaultLogger.Info("CheckHealth called", "request", req)

	var status = backend.HealthStatusOk
	var message = "Data source is working"

	err := d.client.HealthCheck()

	if err != nil {
		status = backend.HealthStatusError
		message = "Cannot connect to the brokers!"
	}

	return &backend.CheckHealthResult{
		Status:  status,
		Message: message,
	}, nil
}

func (d *KafkaDatasource) SubscribeStream(_ context.Context, req *backend.SubscribeStreamRequest) (*backend.SubscribeStreamResponse, error) {
	log.DefaultLogger.Info("SubscribeStream called", "request", req, "path", req.Path)
	// Extract the query parameters
	var path []string = strings.Split(req.Path, "_")
	topic := path[0]
	//partition, _ := strconv.Atoi(path[1])
	//autoOffsetReset := path[2]
	//timestampMode := path[3]
	// Initialize Consumer and Assign the topic
	log.DefaultLogger.Info("SubscribeStream called for topic", "topic_name", topic)
	// TODO need to accept list
	err := d.client.TopicAssign(topic)
	if err != nil {
		log.DefaultLogger.Error("topicAssign error", "topic", topic)
		return &backend.SubscribeStreamResponse{
			Status: backend.SubscribeStreamStatusNotFound,
		}, nil
	}
	//status := backend.SubscribeStreamStatusPermissionDenied
	status := backend.SubscribeStreamStatusOK

	return &backend.SubscribeStreamResponse{
		Status: status,
	}, nil
}

func (d *KafkaDatasource) RunStream(ctx context.Context, req *backend.RunStreamRequest, sender *backend.StreamSender) error {
	log.DefaultLogger.Info("RunStream called for path", "request", req, "path", req.Path)

	for {
		select {
		case <-ctx.Done():
			log.DefaultLogger.Info("Context done, finish streaming", "path", req.Path)
			return nil
		default:
			msg, event := d.client.ConsumerPull()
			if event == nil {
				log.DefaultLogger.Info("event empty")
				continue
			}
			if msg.Topic == "" {
				log.DefaultLogger.Info("topic empty")
				continue
			}
			frame := data.NewFrame("response")
			frame.Fields = append(frame.Fields,
				data.NewField("time", nil, make([]time.Time, 1)),
			)
			var frame_time time.Time
			//if d.client.TimestampMode == "now" {
			//	frame_time = time.Now()
			//} else {
			//	frame_time = msg.Timestamp
			//}
			frame_time = time.Now()
			log.DefaultLogger.Info("Offset", msg.Offset)
			log.DefaultLogger.Info("timestamp", frame_time)
			frame.Fields[0].Set(0, frame_time)

			layout := "2006-01-02T15:04:05.000000Z"
			date, _ := time.Parse(layout, msg.Value.ValueTimestamp)
			log.DefaultLogger.Info(fmt.Sprintf("kafka topic: %v", msg.Topic))
			log.DefaultLogger.Info(fmt.Sprintf("kafka msg: %v", msg.Value))
			log.DefaultLogger.Info(fmt.Sprintf("name: %v", msg.Value.Name))
			log.DefaultLogger.Info(fmt.Sprintf("value: %v", msg.Value.Value))
			log.DefaultLogger.Info(fmt.Sprintf("quality: %v", msg.Value.Quality))
			log.DefaultLogger.Info(fmt.Sprintf("valuetimestame date: %v", date))
			log.DefaultLogger.Info("---------")
			frame.Fields = append(frame.Fields,
				data.NewField("name", nil, []string{msg.Value.Name}),
				data.NewField("value", nil, []float64{msg.Value.Value}),
				data.NewField("quality", nil, []string{msg.Value.Quality}),
				data.NewField("timestamp", nil, []time.Time{date}),
			)
			//change this and see if retyping topic works
			channel := live.Channel{
				Scope:     live.ScopeDatasource,
				Namespace: req.PluginContext.DataSourceInstanceSettings.UID,
				Path:      msg.Topic,
			}
			frame.SetMeta(&data.FrameMeta{Path: msg.Topic, Channel: channel.String()})
			// add dummy value
			//frame.Fields = append(frame.Fields,
			//	data.NewField("testValue", nil, make([]float64, 1)))
			//frame.Fields[cnt].Set(0, msg.Value.TestValue)
			//cnt++
			//for key, value := range msg.Value {
			//	frame.Fields = append(frame.Fields,
			//		data.NewField(key, nil, make([]float64, 1)))
			//	frame.Fields[cnt].Set(0, value)
			//	cnt++
			//}

			err := sender.SendFrame(frame, data.IncludeAll)

			if err != nil {
				log.DefaultLogger.Error("Error sending frame", "error", err)
				continue
			}
		}
	}
}

func (d *KafkaDatasource) PublishStream(_ context.Context, req *backend.PublishStreamRequest) (*backend.PublishStreamResponse, error) {
	log.DefaultLogger.Info("PublishStream called", "request", req)

	return &backend.PublishStreamResponse{
		Status: backend.PublishStreamStatusPermissionDenied,
	}, nil
}
