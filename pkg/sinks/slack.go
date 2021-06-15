package sinks

import (
	"context"

	"github.com/opsgenie/kubernetes-event-exporter/pkg/kube"
	"github.com/rs/zerolog/log"
	"github.com/slack-go/slack"
)

type SlackConfig struct {
	Token   string            `yaml:"token"`
	Channel string            `yaml:"channel"`
	Message string            `yaml:"message"`
	Fields  map[string]string `yaml:"fields"`
}

type SlackSink struct {
	cfg    *SlackConfig
	client *slack.Client
}

func NewSlackSink(cfg *SlackConfig) (Sink, error) {
	return &SlackSink{
		cfg:    cfg,
		client: slack.New(cfg.Token),
	}, nil
}

func (s *SlackSink) Send(ctx context.Context, ev *kube.EnhancedEvent) error {
	channel, err := GetString(ev, s.cfg.Channel)
	if err != nil {
		return err
	}

	message, err := GetString(ev, s.cfg.Message)
	if err != nil {
		return err
	}

	options := []slack.MsgOption{slack.MsgOptionText(message, true)}
	if s.cfg.Fields != nil {
		fields := make([]slack.AttachmentField, 0)
		for k, v := range s.cfg.Fields {
			fieldText, err := GetString(ev, v)
			if err != nil {
				return err
			}

			fields = append(fields, slack.AttachmentField{
				Title: k,
				Value: fieldText,
				Short: false,
			})
		}
		options = append(options, slack.MsgOptionAttachments(slack.Attachment{Fields: fields}))
	}

	_ch, _ts, _text, err := s.client.SendMessageContext(ctx, channel, options...)
	log.Debug().Str("ch", _ch).Str("ts", _ts).Str("text", _text).Err(err).Msg("Slack Response")
	return err
}

func (s *SlackSink) Close() {
	// No-op
}
