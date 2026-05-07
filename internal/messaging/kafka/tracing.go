package kafka

import (
	"github.com/IBM/sarama"
	"go.opentelemetry.io/otel/propagation"
)

type producerHeadersCarrier struct {
	headers *[]sarama.RecordHeader
}

func (c *producerHeadersCarrier) Get(key string) string {
	for _, h := range *c.headers {
		if string(h.Key) == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c *producerHeadersCarrier) Set(key, value string) {
	for i, h := range *c.headers {
		if string(h.Key) == key {
			(*c.headers)[i].Value = []byte(value)
			return
		}
	}
	*c.headers = append(*c.headers, sarama.RecordHeader{Key: []byte(key), Value: []byte(value)})
}

func (c *producerHeadersCarrier) Keys() []string {
	keys := make([]string, len(*c.headers))
	for i, h := range *c.headers {
		keys[i] = string(h.Key)
	}
	return keys
}

var _ propagation.TextMapCarrier = (*producerHeadersCarrier)(nil)

type consumerHeadersCarrier struct {
	headers []*sarama.RecordHeader
}

func (c *consumerHeadersCarrier) Get(key string) string {
	for _, h := range c.headers {
		if h == nil {
			continue
		}
		if string(h.Key) == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c *consumerHeadersCarrier) Set(key, value string) {
	panic("consumer carrier is read-only")
}

func (c *consumerHeadersCarrier) Keys() []string {
	keys := make([]string, 0, len(c.headers))
	for _, h := range c.headers {
		if h == nil {
			continue
		}
		keys = append(keys, string(h.Key))
	}
	return keys
}

var _ propagation.TextMapCarrier = (*consumerHeadersCarrier)(nil)
