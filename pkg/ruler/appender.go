package ruler

import (
	"context"
	"errors"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/prometheus/pkg/exemplar"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/storage"

	"github.com/grafana/loki/pkg/util"
)

type RemoteWriteAppendable struct {
	groupAppender map[string]*RemoteWriteAppender

	userID    string
	cfg       Config
	overrides RulesLimits
	logger    log.Logger
}

type RemoteWriteAppender struct {
	logger       log.Logger
	ctx          context.Context
	remoteWriter remoteWriter
	userID       string
	groupKey     string

	queue *util.EvictingQueue
}

func (a *RemoteWriteAppendable) Appender(ctx context.Context) storage.Appender {
	var appender *RemoteWriteAppender

	if a.groupAppender == nil {
		a.groupAppender = make(map[string]*RemoteWriteAppender)
	}

	groupKey := retrieveGroupKeyFromContext(ctx)

	// create or retrieve an appender associated with this groupKey (unique ID for rule group)
	appender, found := a.groupAppender[groupKey]
	if !found {
		client, err := newRemoteWriter(a.cfg, a.userID)
		if err != nil {
			level.Error(a.logger).Log("msg", "error creating remote-write client; setting appender as noop", "err", err, "tenant", a.userID)
			return &NoopAppender{}
		}

		capacity := a.queueCapacityForTenant()
		appender = &RemoteWriteAppender{
			ctx:          ctx,
			logger:       a.logger,
			remoteWriter: client,
			groupKey:     groupKey,
			userID:       a.userID,

			queue: util.NewEvictingQueue(capacity, onEvict(a.userID, groupKey)),
		}

		samplesQueueCapacity.WithLabelValues(a.userID, groupKey).Set(float64(capacity))

		// only track reference if groupKey was retrieved
		if groupKey == "" {
			level.Warn(a.logger).Log("msg", "blank group key passed via context; creating new appender")
			return appender
		}

		a.groupAppender[groupKey] = appender
	}

	return appender
}

func (a *RemoteWriteAppendable) queueCapacityForTenant() int {
	capacity := a.cfg.RemoteWrite.QueueCapacity
	if tenantCapacity := a.overrides.RulerRemoteWriteQueueCapacity(a.userID); tenantCapacity > 0 {
		capacity = tenantCapacity
	}

	return capacity
}

func onEvict(userID, groupKey string) func() {
	return func() {
		samplesEvicted.WithLabelValues(userID, groupKey).Inc()
	}
}

func (a *RemoteWriteAppender) Append(_ uint64, l labels.Labels, t int64, v float64) (uint64, error) {
	a.queue.Append(queueEntry{
		labels: l,
		sample: cortexpb.Sample{
			Value:       v,
			TimestampMs: t,
		},
	})

	samplesQueued.WithLabelValues(a.userID, a.groupKey).Set(float64(a.queue.Length()))
	samplesQueuedTotal.WithLabelValues(a.userID, a.groupKey).Inc()

	return 0, nil
}

func (a *RemoteWriteAppender) AppendExemplar(_ uint64, _ labels.Labels, _ exemplar.Exemplar) (uint64, error) {
	return 0, errors.New("exemplars are unsupported")
}

func (a *RemoteWriteAppender) Commit() error {
	if a.queue.Length() <= 0 {
		return nil
	}

	if a.remoteWriter == nil {
		level.Warn(a.logger).Log("msg", "no remote_write client defined, skipping commit")
		return nil
	}

	level.Debug(a.logger).Log("msg", "writing samples to remote_write target", "target", a.remoteWriter.Endpoint(), "count", a.queue.Length())

	req, err := a.remoteWriter.PrepareRequest(a.queue)
	if err != nil {
		level.Error(a.logger).Log("msg", "could not prepare remote-write request", "err", err)
		remoteWriteErrors.WithLabelValues(a.userID, a.groupKey).Inc()
		return err
	}

	err = a.remoteWriter.Store(a.ctx, req)
	if err != nil {
		level.Error(a.logger).Log("msg", "could not store recording rule samples", "err", err)
		remoteWriteErrors.WithLabelValues(a.userID, a.groupKey).Inc()
		return err
	}

	// Clear the queue on a successful response
	a.queue.Clear()

	samplesQueued.WithLabelValues(a.userID, a.groupKey).Set(0)

	return nil
}

func (a *RemoteWriteAppender) Rollback() error {
	a.queue.Clear()

	return nil
}

func retrieveGroupKeyFromContext(ctx context.Context) string {
	data, found := ctx.Value(promql.QueryOrigin{}).(map[string]interface{})
	if !found {
		return ""
	}

	ruleGroup, found := data["ruleGroup"].(map[string]string)
	if !found {
		return ""
	}

	file, found := ruleGroup["file"]
	if !found {
		return ""
	}

	name, found := ruleGroup["name"]
	if !found {
		return ""
	}

	return rules.GroupKey(file, name)
}
