// +build linux,cgo

package exporter

import (
	"io"
	"os"
	"testing"
	"time"

	"github.com/coreos/go-systemd/sdexporter"
	"github.com/go-kit/kit/log"
	"github.com/prometheus/prometheus/pkg/relabel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"

	"github.com/grafana/loki/pkg/promtail/client/fake"
	"github.com/grafana/loki/pkg/promtail/positions"
	"github.com/grafana/loki/pkg/promtail/scrapeconfig"
	"github.com/grafana/loki/pkg/promtail/targets/testutils"
)

type mockExporterReader struct {
	config sdexporter.ExporterReaderConfig
	t      *testing.T
}

func newMockExporterReader(c sdexporter.ExporterReaderConfig) (exporterReader, error) {
	return &mockExporterReader{config: c}, nil
}

func (r *mockExporterReader) Close() error {
	return nil
}

func (r *mockExporterReader) Follow(until <-chan time.Time, writer io.Writer) error {
	<-until
	return nil
}

func newMockExporterEntry(entry *sdexporter.ExporterEntry) exporterEntryFunc {
	return func(c sdexporter.ExporterReaderConfig, cursor string) (*sdexporter.ExporterEntry, error) {
		return entry, nil
	}
}

func (r *mockExporterReader) Write(msg string, fields map[string]string) {
	allFields := make(map[string]string, len(fields))
	for k, v := range fields {
		allFields[k] = v
	}
	allFields["MESSAGE"] = msg

	ts := uint64(time.Now().UnixNano())

	_, err := r.config.Formatter(&sdexporter.ExporterEntry{
		Fields:             allFields,
		MonotonicTimestamp: ts,
		RealtimeTimestamp:  ts,
	})
	assert.NoError(r.t, err)
}

func TestExporterTarget(t *testing.T) {
	w := log.NewSyncWriter(os.Stderr)
	logger := log.NewLogfmtLogger(w)

	testutils.InitRandom()
	dirName := "/tmp/" + testutils.RandName()
	positionsFileName := dirName + "/positions.yml"

	// Set the sync period to a really long value, to guarantee the sync timer
	// never runs, this way we know everything saved was done through channel
	// notifications when target.stop() was called.
	ps, err := positions.New(logger, positions.Config{
		SyncPeriod:    10 * time.Second,
		PositionsFile: positionsFileName,
	})
	if err != nil {
		t.Fatal(err)
	}

	client := fake.New(func() {})

	relabelCfg := `
- source_labels: ['__exporter_code_file']
  regex: 'exportertarget_test\.go'
  action: 'keep'
- source_labels: ['__exporter_code_file']
  target_label: 'code_file'`

	var relabels []*relabel.Config
	err = yaml.Unmarshal([]byte(relabelCfg), &relabels)
	require.NoError(t, err)

	jt, err := exporterTargetWithReader(logger, client, ps, "test", relabels,
		&scrapeconfig.ExporterTargetConfig{}, newMockExporterReader, newMockExporterEntry(nil))
	require.NoError(t, err)

	r := jt.r.(*mockExporterReader)
	r.t = t

	for i := 0; i < 10; i++ {
		r.Write("ping", map[string]string{
			"CODE_FILE": "exportertarget_test.go",
		})
		assert.NoError(t, err)
	}
	require.NoError(t, jt.Stop())
	client.Stop()
	assert.Len(t, client.Received(), 10)
}

func TestExporterTarget_JSON(t *testing.T) {
	w := log.NewSyncWriter(os.Stderr)
	logger := log.NewLogfmtLogger(w)

	testutils.InitRandom()
	dirName := "/tmp/" + testutils.RandName()
	positionsFileName := dirName + "/positions.yml"

	// Set the sync period to a really long value, to guarantee the sync timer
	// never runs, this way we know everything saved was done through channel
	// notifications when target.stop() was called.
	ps, err := positions.New(logger, positions.Config{
		SyncPeriod:    10 * time.Second,
		PositionsFile: positionsFileName,
	})
	if err != nil {
		t.Fatal(err)
	}

	client := fake.New(func() {})

	relabelCfg := `
- source_labels: ['__exporter_code_file']
  regex: 'exportertarget_test\.go'
  action: 'keep'
- source_labels: ['__exporter_code_file']
  target_label: 'code_file'`

	var relabels []*relabel.Config
	err = yaml.Unmarshal([]byte(relabelCfg), &relabels)
	require.NoError(t, err)

	cfg := &scrapeconfig.ExporterTargetConfig{JSON: true}

	jt, err := exporterTargetWithReader(logger, client, ps, "test", relabels,
		cfg, newMockExporterReader, newMockExporterEntry(nil))
	require.NoError(t, err)

	r := jt.r.(*mockExporterReader)
	r.t = t

	for i := 0; i < 10; i++ {
		r.Write("ping", map[string]string{
			"CODE_FILE":   "exportertarget_test.go",
			"OTHER_FIELD": "foobar",
		})
		assert.NoError(t, err)

	}
	expectMsg := `{"CODE_FILE":"exportertarget_test.go","MESSAGE":"ping","OTHER_FIELD":"foobar"}`
	require.NoError(t, jt.Stop())
	client.Stop()

	assert.Len(t, client.Received(), 10)
	for i := 0; i < 10; i++ {
		require.Equal(t, expectMsg, client.Received()[i].Line)
	}

}

func TestExporterTarget_Since(t *testing.T) {
	w := log.NewSyncWriter(os.Stderr)
	logger := log.NewLogfmtLogger(w)

	testutils.InitRandom()
	dirName := "/tmp/" + testutils.RandName()
	positionsFileName := dirName + "/positions.yml"

	// Set the sync period to a really long value, to guarantee the sync timer
	// never runs, this way we know everything saved was done through channel
	// notifications when target.stop() was called.
	ps, err := positions.New(logger, positions.Config{
		SyncPeriod:    10 * time.Second,
		PositionsFile: positionsFileName,
	})
	if err != nil {
		t.Fatal(err)
	}

	client := fake.New(func() {})

	cfg := scrapeconfig.ExporterTargetConfig{
		MaxAge: "4h",
	}

	jt, err := exporterTargetWithReader(logger, client, ps, "test", nil,
		&cfg, newMockExporterReader, newMockExporterEntry(nil))
	require.NoError(t, err)

	r := jt.r.(*mockExporterReader)
	require.Equal(t, r.config.Since, -1*time.Hour*4)
	client.Stop()
}

func TestExporterTarget_Cursor_TooOld(t *testing.T) {
	w := log.NewSyncWriter(os.Stderr)
	logger := log.NewLogfmtLogger(w)

	testutils.InitRandom()
	dirName := "/tmp/" + testutils.RandName()
	positionsFileName := dirName + "/positions.yml"

	// Set the sync period to a really long value, to guarantee the sync timer
	// never runs, this way we know everything saved was done through channel
	// notifications when target.stop() was called.
	ps, err := positions.New(logger, positions.Config{
		SyncPeriod:    10 * time.Second,
		PositionsFile: positionsFileName,
	})
	if err != nil {
		t.Fatal(err)
	}
	ps.PutString("exporter-test", "foobar")

	client := fake.New(func() {})

	cfg := scrapeconfig.ExporterTargetConfig{}

	entryTs := time.Date(1980, time.July, 3, 12, 0, 0, 0, time.UTC)
	exporterEntry := newMockExporterEntry(&sdexporter.ExporterEntry{
		Cursor:            "foobar",
		Fields:            nil,
		RealtimeTimestamp: uint64(entryTs.UnixNano()),
	})

	jt, err := exporterTargetWithReader(logger, client, ps, "test", nil,
		&cfg, newMockExporterReader, exporterEntry)
	require.NoError(t, err)

	r := jt.r.(*mockExporterReader)
	require.Equal(t, r.config.Since, -1*time.Hour*7)
	client.Stop()
}

func TestExporterTarget_Cursor_NotTooOld(t *testing.T) {
	w := log.NewSyncWriter(os.Stderr)
	logger := log.NewLogfmtLogger(w)

	testutils.InitRandom()
	dirName := "/tmp/" + testutils.RandName()
	positionsFileName := dirName + "/positions.yml"

	// Set the sync period to a really long value, to guarantee the sync timer
	// never runs, this way we know everything saved was done through channel
	// notifications when target.stop() was called.
	ps, err := positions.New(logger, positions.Config{
		SyncPeriod:    10 * time.Second,
		PositionsFile: positionsFileName,
	})
	if err != nil {
		t.Fatal(err)
	}
	ps.PutString("exporter-test", "foobar")

	client := fake.New(func() {})

	cfg := scrapeconfig.ExporterTargetConfig{}

	entryTs := time.Now().Add(-time.Hour)
	exporterEntry := newMockExporterEntry(&sdexporter.ExporterEntry{
		Cursor:            "foobar",
		Fields:            nil,
		RealtimeTimestamp: uint64(entryTs.UnixNano() / int64(time.Microsecond)),
	})

	jt, err := exporterTargetWithReader(logger, client, ps, "test", nil,
		&cfg, newMockExporterReader, exporterEntry)
	require.NoError(t, err)

	r := jt.r.(*mockExporterReader)
	require.Equal(t, r.config.Since, time.Duration(0))
	require.Equal(t, r.config.Cursor, "foobar")
	client.Stop()
}

func Test_MakeExporterFields(t *testing.T) {
	entryFields := map[string]string{
		"CODE_FILE":   "exportertarget_test.go",
		"OTHER_FIELD": "foobar",
		"PRIORITY":    "6",
	}
	receivedFields := makeExporterFields(entryFields)
	expectedFields := map[string]string{
		"__exporter_code_file":        "exportertarget_test.go",
		"__exporter_other_field":      "foobar",
		"__exporter_priority":         "6",
		"__exporter_priority_keyword": "info",
	}
	assert.Equal(t, expectedFields, receivedFields)
}
