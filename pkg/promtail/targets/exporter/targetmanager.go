// +build cgo

package exporter

import (
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/loki/pkg/logentry/stages"
	"github.com/grafana/loki/pkg/promtail/api"
	"github.com/grafana/loki/pkg/promtail/scrapeconfig"
	"github.com/grafana/loki/pkg/promtail/targets/target"
)

// ExporterTargetManager manages a series of ExporterTargets.
// nolint(golint)
type ExporterTargetManager struct {
	logger  log.Logger
	targets map[string]*ExporterTarget
}

// NewExporterTargetManager creates a new ExporterTargetManager.
func NewExporterTargetManager(
	reg prometheus.Registerer,
	logger log.Logger,
	client api.EntryHandler,
	scrapeConfigs []scrapeconfig.Config,
) (*ExporterTargetManager, error) {
	tm := &ExporterTargetManager{
		logger:  logger,
		targets: make(map[string]*ExporterTarget),
	}

	for _, cfg := range scrapeConfigs {
		if cfg.ExporterConfig == nil {
			continue
		}

		pipeline, err := stages.NewPipeline(log.With(logger, "component", "exporter_pipeline"), cfg.PipelineStages, &cfg.JobName, reg)
		if err != nil {
			return nil, err
		}

		t, err := NewExporterTarget(
			log.With(logger, "target", "exporter", "job", cfg.JobName),
			pipeline.Wrap(client),
			cfg.JobName,
			cfg.RelabelConfigs,
			cfg.ExporterConfig,
		)
		if err != nil {
			return nil, err
		}

		tm.targets[cfg.JobName] = t
	}

	return tm, nil
}

// Ready returns true if at least one ExporterTarget is also ready.
func (tm *ExporterTargetManager) Ready() bool {
	for _, t := range tm.targets {
		if t.Ready() {
			return true
		}
	}
	return false
}

// Stop stops the ExporterTargetManager and all of its ExporterTargets.
func (tm *ExporterTargetManager) Stop() {
	for _, t := range tm.targets {
		if err := t.Stop(); err != nil {
			level.Error(t.logger).Log("msg", "error stopping ExporterTarget", "err", err.Error())
		}
	}
}

// ActiveTargets returns the list of ExporterTargets where exporter data
// is being read. ActiveTargets is an alias to AllTargets as
// ExporterTargets cannot be deactivated, only stopped.
func (tm *ExporterTargetManager) ActiveTargets() map[string][]target.Target {
	return tm.AllTargets()
}

// AllTargets returns the list of all targets where exporter data
// is currently being read.
func (tm *ExporterTargetManager) AllTargets() map[string][]target.Target {
	result := make(map[string][]target.Target, len(tm.targets))
	for k, v := range tm.targets {
		result[k] = []target.Target{v}
	}
	return result
}
