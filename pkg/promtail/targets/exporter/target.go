// +build linux,cgo

package exporter

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"time"

	promConfig "github.com/prometheus/common/config"

	"github.com/grafana/loki/pkg/logproto"

	"github.com/pkg/errors"

	"github.com/prometheus/prometheus/pkg/labels"

	"github.com/prometheus/prometheus/pkg/textparse"

	"github.com/go-kit/kit/log/level"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/relabel"

	"github.com/grafana/loki/pkg/promtail/api"
	"github.com/grafana/loki/pkg/promtail/scrapeconfig"
	"github.com/grafana/loki/pkg/promtail/targets/target"
)

// ExporterTarget scrapes Prometheus exporters.
// nolint(golint)
type ExporterTarget struct {
	name          string
	client        *http.Client
	logger        log.Logger
	handler       api.EntryHandler
	relabelConfig []*relabel.Config
	config        *scrapeconfig.ExporterTargetConfig
	labels        model.LabelSet
	ticker        *time.Ticker
}

// NewExporterTarget configures a new ExporterTarget.
func NewExporterTarget(
	logger log.Logger,
	handler api.EntryHandler,
	jobName string,
	relabelConfig []*relabel.Config,
	targetConfig *scrapeconfig.ExporterTargetConfig,
) (*ExporterTarget, error) {

	client, err := promConfig.NewClientFromConfig(promConfig.HTTPClientConfig{}, jobName, false, false)
	if err != nil {
		return nil, errors.Wrap(err, "error creating HTTP client")
	}

	t := &ExporterTarget{
		name:          jobName,
		client:        client,
		logger:        logger,
		handler:       handler,
		relabelConfig: relabelConfig,
		config:        targetConfig,
		labels:        model.LabelSet{},
	}

	errChan := make(chan error)
	go t.tickAndRun(errChan)

	go func() {
		for {
			err := <-errChan
			if err != nil {
				level.Error(logger).Log("msg", err)
			}
		}
	}()

	return t, nil
}

func (t *ExporterTarget) tickAndRun(errChan chan error) {
	t.ticker = time.NewTicker(t.config.ScrapeInterval)
	done := make(chan bool)

	for {
		select {
		case <-done:
			return
		case <-t.ticker.C:
			err := t.run()
			errChan <- err
		}
	}
}

func (t *ExporterTarget) run() error {
	ctx, cancel := context.WithTimeout(context.Background(), t.scrapeTimeout())
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, t.metricsPath(), nil) // TODO scheme configurable
	if err != nil {
		return fmt.Errorf("Cannot create request: %s\n", err)
	}

	rsp, err := t.client.Do(req)
	if rsp != nil {
		defer rsp.Body.Close()
	}

	if e, ok := err.(net.Error); ok && e.Timeout() {
		return fmt.Errorf("scrape request timeout: %s", err)
	} else if err != nil {
		return fmt.Errorf("scrape request failure: %s", err)
	}

	body, err := ioutil.ReadAll(rsp.Body)
	if err != nil {
		return fmt.Errorf("cannot read scrape response body: %s", err)
	}

	p := textparse.NewPromParser(body)
	for {
		e, err := p.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			return errors.Wrap(err, "parse")
		}

		if e != textparse.EntrySeries {
			continue
		}

		_, _, val := p.Series()

		l := labels.Labels{
			labels.Label{
				Name:  t.valueName(),
				Value: fmt.Sprintf("%v", val),
			},
		}
		p.Metric(&l)

		out, err := l.MarshalJSON()
		if err != nil {
			level.Warn(t.logger).Log("msg", "cannot marshal labels to JSON")
			continue
		}

		t.handler.Chan() <- api.Entry{
			Labels: model.LabelSet{
				model.LabelName(t.metricName()): model.LabelValue(l.Get(model.MetricNameLabel)),
				model.JobLabel:                  model.LabelValue(t.name),
			},
			Entry: logproto.Entry{
				Timestamp: time.Now(),
				Line:      string(out),
			},
		}
	}

	return nil
}

func (t *ExporterTarget) metricName() string {
	if t.config.MetricName == "" {
		return "metric"
	}

	return t.config.MetricName
}

func (t *ExporterTarget) valueName() string {
	if t.config.ValueName == "" {
		return "val"
	}

	return t.config.ValueName
}

func (t *ExporterTarget) scrapeTimeout() time.Duration {
	if t.config.ScrapeTimeout <= 0 {
		return time.Second * 5
	}

	return t.config.ScrapeTimeout
}

func (t *ExporterTarget) metricsPath() string {
	metricsPath := t.config.MetricsPath
	if t.config.MetricsPath == "" {
		metricsPath = "/metrics"
	}

	return fmt.Sprintf("http://%s/%s", net.JoinHostPort(t.config.Host, t.config.Port), metricsPath)
}

// Type returns ExporterTargetType.
func (t *ExporterTarget) Type() target.TargetType {
	return target.ExporterTargetType
}

// Ready indicates whether or not the exporter is ready to be
// read from.
func (t *ExporterTarget) Ready() bool {
	return true
}

// DiscoveredLabels returns the set of labels discovered by
// the ExporterTarget, which is always nil. Implements
// Target.
func (t *ExporterTarget) DiscoveredLabels() model.LabelSet {
	return nil
}

// Labels returns the set of labels that statically apply to
// all log entries produced by the ExporterTarget.
func (t *ExporterTarget) Labels() model.LabelSet {
	return t.labels
}

// Details returns target-specific details.
func (t *ExporterTarget) Details() interface{} {
	return nil
}

// Stop shuts down the ExporterTarget.
func (t *ExporterTarget) Stop() error {
	t.handler.Stop()

	if t.ticker != nil {
		t.ticker.Stop()
	}

	return nil
}
