package compactor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cortexproject/cortex/pkg/chunk/purger"
	"github.com/cortexproject/cortex/pkg/storage/bucket"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	cortex_testutil "github.com/cortexproject/cortex/pkg/storage/tsdb/testutil"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore"
)

var (
	compactionRanges = []time.Duration{2 * time.Hour, 6 * time.Hour, 24 * time.Hour}
)

type testSeriesCleanerConfig struct {
	concurrency         int
	seriesDeletionDelay time.Duration
}

func (o testSeriesCleanerConfig) String() string {
	return fmt.Sprintf("concurrency=%d, deletion delay=%v",
		o.concurrency, o.seriesDeletionDelay)
}

func TestDeletedSeriesCleaner(t *testing.T) {
	for _, options := range []testSeriesCleanerConfig{
		{concurrency: 1, seriesDeletionDelay: 0},
		{concurrency: 1, seriesDeletionDelay: 2 * time.Hour},
		{concurrency: 2, seriesDeletionDelay: 1 * time.Hour},
		{concurrency: 6, seriesDeletionDelay: 2 * time.Hour},
	} {
		options := options

		t.Run(options.String(), func(t *testing.T) {
			testDeletedSeriesCleanerWithOptions(t, options)
		})
	}
}

func testDeletedSeriesCleanerWithOptions(t *testing.T, options testSeriesCleanerConfig) {
	bucketClient, _ := cortex_testutil.PrepareFilesystemBucket(t)
	ctx := context.Background()

	// create the tombstones
	createTombstone(t, bucketClient, "user-1", 100, 100, 0, 15, []string{"{series_id=~\".+\"}"}, "request1", cortex_tsdb.StatePending)
	createTombstone(t, bucketClient, "user-1", 100, 100, 0, 9, []string{"{series_id=~\".+\"}"}, "request2", cortex_tsdb.StatePending)
	createTombstone(t, bucketClient, "user-1", 100, 300, 31, 35, []string{"{series_id=~\".+\"}"}, "request3", cortex_tsdb.StateProcessed)
	createTombstone(t, bucketClient, "user-2", 100, 100, 0, 9, []string{"{series_id=~\".+\"}"}, "request1", cortex_tsdb.StatePending)
	createTombstone(t, bucketClient, "user-2", 100, 100, 15, 30, []string{"{series_id=~\".+\"}"}, "request2", cortex_tsdb.StatePending)

	block1 := createTSDBBlock(t, bucketClient, "user-1", 10, 20, nil)

	createMetaJSONFile(t, bucketClient, "user-1", block1, 10, 20, len(compactionRanges))

	block2 := createTSDBBlock(t, bucketClient, "user-1", 20, 30, nil)
	createMetaJSONFile(t, bucketClient, "user-1", block2, 20, 30, len(compactionRanges))

	block3 := createTSDBBlock(t, bucketClient, "user-1", 30, 40, nil)
	createMetaJSONFile(t, bucketClient, "user-1", block3, 30, 40, len(compactionRanges))

	block4 := createTSDBBlock(t, bucketClient, "user-2", 10, 40, nil)
	createMetaJSONFile(t, bucketClient, "user-2", block4, 10, 40, len(compactionRanges))

	purgerCfg := purger.Config{
		Enable:                    true,
		DeleteRequestCancelPeriod: options.seriesDeletionDelay,
	}

	cfg := DeletedSeriesCleanerConfig{
		CleanupInterval:       time.Minute,
		CleanupConcurrency:    options.concurrency,
		CompactionBlockRanges: compactionRanges,
	}

	reg := prometheus.NewPedanticRegistry()
	logger := log.NewNopLogger()
	scanner := cortex_tsdb.NewUsersScanner(bucketClient, cortex_tsdb.AllUsers, logger)
	cfgProvider := newMockConfigProvider()

	cleaner := NewDeletedSeriesCleaner(cfg, bucketClient, scanner, cfgProvider, purgerCfg, logger, reg)
	require.NoError(t, services.StartAndAwaitRunning(ctx, cleaner))
	defer services.StopAndAwaitTerminated(ctx, cleaner) //nolint:errcheck

	for _, tc := range []struct {
		block     ulid.ULID
		userID    string
		rewritten bool
	}{
		// block 1 should be rewritten because tombstone 1 applies to it
		{
			block:     block1,
			userID:    "user-1",
			rewritten: true,
		},
		// Block 2 should not be rewritten since no tombstone applies to the interval time for user-1
		{
			block:     block2,
			userID:    "user-1",
			rewritten: false,
		},
		// Tombstone 3 fits the interval but processed state tombstones should not rewrite any blocks
		{
			block:     block3,
			userID:    "user-1",
			rewritten: false,
		},
		// Tombstone 5 should rewrite the block
		{
			block:     block4,
			userID:    "user-2",
			rewritten: true,
		},
	} {

		// if the block was rewritten then it should create a new block and mark this one as deleted
		// To see if a new block was correctly created, a different test will handle this.
		deletionMarkPath := path.Join(tc.userID, tc.block.String(), metadata.DeletionMarkFilename)
		exists, err := bucketClient.Exists(ctx, deletionMarkPath)
		require.NoError(t, err)
		require.Equal(t, tc.rewritten, exists)

	}

	assert.Equal(t, float64(1), testutil.ToFloat64(cleaner.runsStarted))
	assert.Equal(t, float64(1), testutil.ToFloat64(cleaner.runsCompleted))
	assert.Equal(t, float64(0), testutil.ToFloat64(cleaner.runsFailed))
	assert.Equal(t, float64(0), testutil.ToFloat64(cleaner.blocksFailedTotal))
	assert.Equal(t, float64(1), testutil.ToFloat64(cleaner.tenentBlocksProcessedTotal.WithLabelValues("user-1")))
	assert.Equal(t, float64(1), testutil.ToFloat64(cleaner.tenentBlocksProcessedTotal.WithLabelValues("user-2")))

	assert.NoError(t, prom_testutil.GatherAndCompare(reg, strings.NewReader(`
	
		# HELP cortex_compactor_blocks_processed_for_series_deletion_total Total number of blocks processed for series deletion.
		# TYPE cortex_compactor_blocks_processed_for_series_deletion_total counter
		cortex_compactor_blocks_processed_for_series_deletion_total{user="user-1"} 1
		cortex_compactor_blocks_processed_for_series_deletion_total{user="user-2"} 1
		# HELP cortex_compactor_deletion_requests_processed_total Total number of deletion requests that have been fully processed.
		# TYPE cortex_compactor_deletion_requests_processed_total counter
		cortex_compactor_deletion_requests_processed_total{user="user-1"} 2
		cortex_compactor_deletion_requests_processed_total{user="user-2"} 2
		# HELP cortex_compactor_pending_deletion_requests Total number of deletion requests that require series deletion.
		# TYPE cortex_compactor_pending_deletion_requests gauge
		cortex_compactor_pending_deletion_requests{user="user-1"} 0
		cortex_compactor_pending_deletion_requests{user="user-2"} 0
	`),
		"cortex_compactor_blocks_processed_for_series_deletion_total",
		"cortex_compactor_deletion_requests_processed_total",
		"cortex_compactor_pending_deletion_requests",
	))
}

func TestSeriesDeletion_ShouldRemoveMetricsForTenantsNotBelongingAnymoreToTheShard(t *testing.T) {
	bucketClient, _ := cortex_testutil.PrepareFilesystemBucket(t)

	// // Create blocks.
	// Write some blocks to the bucket
	series1 := []seriesSamples{
		{lset: labels.Labels{{Name: "name", Value: "series1"}},
			chunks: [][]sample{{{0, 0}, {20, 20}, {30, 30}}}},
	}
	createBlockSeries(t, bucketClient, "user-1", series1, len(compactionRanges))

	series2 := []seriesSamples{
		{lset: labels.Labels{{Name: "name", Value: "series2"}},
			chunks: [][]sample{{{40, 40}, {60, 60}, {90, 90}}}},
	}
	createBlockSeries(t, bucketClient, "user-1", series2, len(compactionRanges))

	series3 := []seriesSamples{
		{lset: labels.Labels{{Name: "name", Value: "series1"}},
			chunks: [][]sample{{{91, 91}, {92, 20}, {120, 30}}}},
	}
	createBlockSeries(t, bucketClient, "user-2", series3, len(compactionRanges))

	// create some tombstones before cancellation period
	createTime := time.Now().Add(-3*time.Minute).Unix() * 1000
	createTombstone(t, bucketClient, "user-1", createTime, createTime, 0, 50, []string{"{name=\"series1\"}"}, "request1", cortex_tsdb.StatePending)
	createTombstone(t, bucketClient, "user-2", createTime, createTime, 90, 95, []string{"{name=\"series1\"}"}, "request2", cortex_tsdb.StatePending)

	purgerCfg := purger.Config{
		Enable:                    true,
		DeleteRequestCancelPeriod: time.Minute * 5,
	}

	cfg := DeletedSeriesCleanerConfig{
		CleanupInterval:       time.Minute,
		CleanupConcurrency:    1,
		CompactionBlockRanges: compactionRanges,
	}

	ctx := context.Background()
	logger := log.NewNopLogger()
	reg := prometheus.NewRegistry()

	scanner := cortex_tsdb.NewUsersScanner(bucketClient, cortex_tsdb.AllUsers, logger)
	cfgProvider := newMockConfigProvider()

	cleaner := NewDeletedSeriesCleaner(cfg, bucketClient, scanner, cfgProvider, purgerCfg, logger, reg)

	require.NoError(t, services.StartAndAwaitRunning(ctx, cleaner))
	defer services.StopAndAwaitTerminated(ctx, cleaner) //nolint:errcheck

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_compactor_blocks_pending_series_deletion Total number of blocks that need to be rewritten to processes the series deletion request.
		# TYPE cortex_compactor_blocks_pending_series_deletion gauge
		cortex_compactor_blocks_pending_series_deletion{user="user-1"} 2
		cortex_compactor_blocks_pending_series_deletion{user="user-2"} 1
		# HELP cortex_compactor_pending_deletion_requests Total number of deletion requests that require series deletion.
		# TYPE cortex_compactor_pending_deletion_requests gauge
		cortex_compactor_pending_deletion_requests{user="user-1"} 1
		cortex_compactor_pending_deletion_requests{user="user-2"} 1
`),
		"cortex_compactor_blocks_pending_series_deletion",
		"cortex_compactor_pending_deletion_requests",
	))

	// Override the users scanner to reconfigure it to only return a subset of users.
	cleaner.usersScanner = cortex_tsdb.NewUsersScanner(bucketClient, func(userID string) (bool, error) { return userID == "user-1", nil }, logger)

	// override the previous files to simulate that the cancellation period has passed
	createTime = time.Now().Add(-6*time.Minute).Unix() * 1000
	createTombstone(t, bucketClient, "user-1", createTime, createTime, 0, 50, []string{"{name=\"series1\"}"}, "request1", cortex_tsdb.StatePending)
	createTombstone(t, bucketClient, "user-2", createTime, createTime, 90, 95, []string{"{name=\"series1\"}"}, "request2", cortex_tsdb.StatePending)

	require.NoError(t, cleaner.ticker(ctx))

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_compactor_blocks_pending_series_deletion Total number of blocks that need to be rewritten to processes the series deletion request.
		# TYPE cortex_compactor_blocks_pending_series_deletion gauge
		cortex_compactor_blocks_pending_series_deletion{user="user-1"} 0
		# HELP cortex_compactor_pending_deletion_requests Total number of deletion requests that require series deletion.
		# TYPE cortex_compactor_pending_deletion_requests gauge
		cortex_compactor_pending_deletion_requests{user="user-1"} 0
`),
		"cortex_compactor_blocks_pending_series_deletion",
		"cortex_compactor_pending_deletion_requests",
	))
}

func TestSeriesDeletion_ShouldNotDeleteWhenTombstoneCancellationPeriodNotOver(t *testing.T) {

	userID := "test-user"
	cancelPeriod := time.Minute * 5

	for name, tc := range map[string]struct {
		tombstones          []*cortex_tsdb.Tombstone
		cancellationPeriod  time.Duration
		numNewBlocks        int
		pendingTombstones   int
		processedTombstones int
	}{
		"Single pending tombstone past cancel period should rewrite block": {
			tombstones: []*cortex_tsdb.Tombstone{
				{RequestID: "r1", State: cortex_tsdb.StatePending,
					RequestCreatedAt: time.Now().Add(-7*time.Minute).Unix() * 1000,
					StateCreatedAt:   time.Now().Add(-7*time.Minute).Unix() * 1000,
					Selectors:        []string{"{name=\"series1\"}"},
					StartTime:        0, EndTime: 29,
				},
			},
			numNewBlocks:        1,
			pendingTombstones:   0,
			processedTombstones: 1,
		},
		"Single pending tombstone before cancel period should not rewrite block": {
			tombstones: []*cortex_tsdb.Tombstone{
				{RequestID: "r1", State: cortex_tsdb.StatePending,
					RequestCreatedAt: time.Now().Add(-4*time.Minute).Unix() * 1000,
					StateCreatedAt:   time.Now().Add(-4*time.Minute).Unix() * 1000,
					Selectors:        []string{"{name=\"series1\"}"},
					StartTime:        0, EndTime: 30,
				},
			},
			numNewBlocks:        0,
			pendingTombstones:   1,
			processedTombstones: 0,
		},
		"Multiple tombstone before cancel period should not rewrite block": {
			tombstones: []*cortex_tsdb.Tombstone{
				{RequestID: "r1", State: cortex_tsdb.StatePending,
					RequestCreatedAt: time.Now().Add(-4*time.Minute).Unix() * 1000,
					StateCreatedAt:   time.Now().Add(-4*time.Minute).Unix() * 1000,
					Selectors:        []string{"{name=\"series1\"}"},
					StartTime:        0, EndTime: 30,
				},
				{RequestID: "r2", State: cortex_tsdb.StatePending,
					RequestCreatedAt: time.Now().Add(-3*time.Minute).Unix() * 1000,
					StateCreatedAt:   time.Now().Add(-3*time.Minute).Unix() * 1000,
					Selectors:        []string{"{name=\"series1\"}"},
					StartTime:        0, EndTime: 90,
				},
				{RequestID: "r3", State: cortex_tsdb.StatePending,
					RequestCreatedAt: time.Now().Add(-1*time.Minute).Unix() * 1000,
					StateCreatedAt:   time.Now().Add(-1*time.Minute).Unix() * 1000,
					Selectors:        []string{"{name=\"series1\"}"},
					StartTime:        0, EndTime: 120,
				},
			},
			numNewBlocks:        0,
			pendingTombstones:   3,
			processedTombstones: 0,
		},
		"Multiple tombstone, some before and some after the cancel period ": {
			tombstones: []*cortex_tsdb.Tombstone{
				{RequestID: "r1", State: cortex_tsdb.StatePending,
					RequestCreatedAt: time.Now().Add(-4*time.Minute).Unix() * 1000,
					StateCreatedAt:   time.Now().Add(-4*time.Minute).Unix() * 1000,
					Selectors:        []string{"{name=\"series1\"}"},
					StartTime:        0, EndTime: 30,
				},
				{RequestID: "r2", State: cortex_tsdb.StatePending,
					RequestCreatedAt: time.Now().Add(-3*time.Minute).Unix() * 1000,
					StateCreatedAt:   time.Now().Add(-3*time.Minute).Unix() * 1000,
					Selectors:        []string{"{name=\"series1\"}"},
					StartTime:        30, EndTime: 60,
				},
				{RequestID: "r3", State: cortex_tsdb.StatePending,
					RequestCreatedAt: time.Now().Add(-6*time.Minute).Unix() * 1000,
					StateCreatedAt:   time.Now().Add(-6*time.Minute).Unix() * 1000,
					Selectors:        []string{"{name=\"series1\"}"},
					StartTime:        100, EndTime: 115,
				},
				{RequestID: "r4", State: cortex_tsdb.StatePending,
					RequestCreatedAt: time.Now().Add(-7*time.Minute).Unix() * 1000,
					StateCreatedAt:   time.Now().Add(-7*time.Minute).Unix() * 1000,
					Selectors:        []string{"{name=\"series1\"}"},
					StartTime:        45, EndTime: 110,
				},
			},
			numNewBlocks:        2,
			pendingTombstones:   2,
			processedTombstones: 2,
		},
	} {
		t.Run(name, func(t *testing.T) {
			bucketClient, _ := cortex_testutil.PrepareFilesystemBucket(t)
			ctx := context.Background()

			// Write some blocks to the bucket
			series1 := []seriesSamples{
				{lset: labels.Labels{{Name: "name", Value: "series1"}},
					chunks: [][]sample{{{0, 0}, {20, 20}, {30, 30}}}},
			}
			createBlockSeries(t, bucketClient, userID, series1, len(compactionRanges))

			series2 := []seriesSamples{
				{lset: labels.Labels{{Name: "name", Value: "series1"}},
					chunks: [][]sample{{{40, 40}, {60, 60}, {90, 90}}}},
			}
			createBlockSeries(t, bucketClient, userID, series2, len(compactionRanges))

			series3 := []seriesSamples{
				{lset: labels.Labels{{Name: "name", Value: "series1"}},
					chunks: [][]sample{{{91, 91}, {92, 20}, {120, 30}}}},
			}
			createBlockSeries(t, bucketClient, userID, series3, len(compactionRanges))

			for _, ts := range tc.tombstones {
				ts.UserID = userID
				uploadTombstone(t, bucketClient, userID, ts)
			}

			purgerCfg := purger.Config{
				Enable:                    true,
				DeleteRequestCancelPeriod: cancelPeriod,
			}

			cfg := DeletedSeriesCleanerConfig{
				CleanupInterval:       time.Minute,
				CleanupConcurrency:    1,
				CompactionBlockRanges: compactionRanges,
			}

			reg := prometheus.NewPedanticRegistry()
			logger := log.NewNopLogger()
			scanner := cortex_tsdb.NewUsersScanner(bucketClient, cortex_tsdb.AllUsers, logger)
			cfgProvider := newMockConfigProvider()

			cleaner := NewDeletedSeriesCleaner(cfg, bucketClient, scanner, cfgProvider, purgerCfg, logger, reg)

			newBlocks, err := cleaner.deleteSeriesForUser(ctx, userID)
			require.NoError(t, err)
			require.Equal(t, tc.numNewBlocks, len(newBlocks))
			require.Equal(t, float64(tc.numNewBlocks), testutil.ToFloat64(cleaner.tenentBlocksProcessedTotal.WithLabelValues(userID)))
			require.Equal(t, float64(tc.pendingTombstones), testutil.ToFloat64(cleaner.tenantPendingRequests.WithLabelValues(userID)))
			require.Equal(t, float64(tc.processedTombstones), testutil.ToFloat64(cleaner.tenentTombstonesProcessedTotal.WithLabelValues(userID)))
		})

	}
}

func TestSeriesDeletion_DeletingNoneOrAllShouldNotCreateNewBlock(t *testing.T) {

	userID := "test-user"

	for name, tc := range map[string]struct {
		tombstones      []*cortex_tsdb.Tombstone
		oldBlockDeleted bool
	}{
		"Single tombstone deleting entire block": {
			tombstones: []*cortex_tsdb.Tombstone{
				{RequestID: "r1", State: cortex_tsdb.StatePending,
					RequestCreatedAt: 0, StateCreatedAt: 0,
					Selectors: []string{"{name=\"series1\"}"},
					StartTime: 0, EndTime: 120,
				},
			},
			oldBlockDeleted: true,
		},
		"Multiple tombstone delete entire block": {
			tombstones: []*cortex_tsdb.Tombstone{
				{RequestID: "r1", State: cortex_tsdb.StatePending,
					RequestCreatedAt: 0, StateCreatedAt: 0,
					Selectors: []string{"{name=\"series1\"}"},
					StartTime: 0, EndTime: 30,
				},
				{RequestID: "r2", State: cortex_tsdb.StatePending,
					RequestCreatedAt: 0, StateCreatedAt: 0,
					Selectors: []string{"{name=\"series1\"}"},
					StartTime: 30, EndTime: 60,
				},
				{RequestID: "r3", State: cortex_tsdb.StatePending,
					RequestCreatedAt: 0, StateCreatedAt: 0,
					Selectors: []string{"{name=\"series1\"}"},
					StartTime: 60, EndTime: 120,
				},
			},
			oldBlockDeleted: true,
		},
		"Single tombstone but selectors don't match, old block should remain": {
			tombstones: []*cortex_tsdb.Tombstone{
				{RequestID: "r1", State: cortex_tsdb.StatePending,
					RequestCreatedAt: 0, StateCreatedAt: 0,
					Selectors: []string{"{name=\"series2\"}"},
					StartTime: 0, EndTime: 120,
				},
			},
			oldBlockDeleted: false,
		},
		"Multiple tombstone but selectors or time don't match, old block should remain": {
			tombstones: []*cortex_tsdb.Tombstone{
				{RequestID: "r1", State: cortex_tsdb.StatePending,
					RequestCreatedAt: 0, StateCreatedAt: 0,
					Selectors: []string{"{name=\"series2\"}"},
					StartTime: 0, EndTime: 30,
				},
				{RequestID: "r2", State: cortex_tsdb.StatePending,
					RequestCreatedAt: 0, StateCreatedAt: 0,
					Selectors: []string{"{name=\"series3\"}"},
					StartTime: 30, EndTime: 60,
				},
				{RequestID: "r3", State: cortex_tsdb.StatePending,
					RequestCreatedAt: 0, StateCreatedAt: 0,
					Selectors: []string{"{name=\"series1\"}"},
					StartTime: 140, EndTime: 180,
				},
			},
			oldBlockDeleted: false,
		},
	} {
		t.Run(name, func(t *testing.T) {
			bucketClient, _ := cortex_testutil.PrepareFilesystemBucket(t)
			ctx := context.Background()

			// Write some blocks to the bucket
			series1 := []seriesSamples{
				{lset: labels.Labels{{Name: "name", Value: "series1"}},
					chunks: [][]sample{{{0, 0}, {20, 20}, {30, 30}}, {{50, 50}, {60, 60}, {120, 1}}}},
			}
			block1 := createBlockSeries(t, bucketClient, userID, series1, len(compactionRanges))

			for _, ts := range tc.tombstones {
				ts.UserID = userID
				uploadTombstone(t, bucketClient, userID, ts)
			}

			purgerCfg := purger.Config{
				Enable:                    true,
				DeleteRequestCancelPeriod: time.Second,
			}

			cfg := DeletedSeriesCleanerConfig{
				CleanupInterval:       time.Minute,
				CleanupConcurrency:    1,
				CompactionBlockRanges: compactionRanges,
			}

			reg := prometheus.NewPedanticRegistry()
			logger := log.NewNopLogger()
			scanner := cortex_tsdb.NewUsersScanner(bucketClient, cortex_tsdb.AllUsers, logger)
			cfgProvider := newMockConfigProvider()

			cleaner := NewDeletedSeriesCleaner(cfg, bucketClient, scanner, cfgProvider, purgerCfg, logger, reg)

			newBlocks, err := cleaner.deleteSeriesForUser(ctx, userID)
			require.NoError(t, err)
			require.Equal(t, 0, len(newBlocks))
			require.Equal(t, float64(1), testutil.ToFloat64(cleaner.tenentBlocksProcessedTotal.WithLabelValues(userID)))
			require.Equal(t, float64(0), testutil.ToFloat64(cleaner.tenantPendingRequests.WithLabelValues(userID)))
			require.Equal(t, float64(len(tc.tombstones)), testutil.ToFloat64(cleaner.tenentTombstonesProcessedTotal.WithLabelValues(userID)))

			//check that the old block was marked for deletion
			deletionMarkPath := path.Join(userID, block1.String(), metadata.DeletionMarkFilename)
			exists, err := bucketClient.Exists(ctx, deletionMarkPath)
			require.NoError(t, err)
			require.Equal(t, tc.oldBlockDeleted, exists)
		})

	}
}

func TestSeriesDeletion_ShouldNotDeleteThatIsntFullyCompacted(t *testing.T) {

	for name, tc := range map[string]struct {
		compactionLevel int
		rewritten       bool
	}{
		"Initially compacted block should not be rewritten": {
			compactionLevel: 1,
			rewritten:       false,
		},

		"Block is compacted beyond 2 hours but is not fully": {
			compactionLevel: 2,
			rewritten:       false,
		},

		"Block is fully compacted and can be rewritten": {
			compactionLevel: len(compactionRanges),
			rewritten:       true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			bucketClient, _ := cortex_testutil.PrepareFilesystemBucket(t)
			ctx := context.Background()

			createTombstone(t, bucketClient, "user-1", 0, 0, 0, 10, []string{"{a=\"1\"}"}, "request1", cortex_tsdb.StatePending)

			// create block
			seriesToWrite := []seriesSamples{
				{lset: labels.Labels{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}},
					chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}, {{10, 11}, {11, 11}, {20, 20}}}}}

			block1 := createBlockSeries(t, bucketClient, "user-1", seriesToWrite, tc.compactionLevel)

			purgerCfg := purger.Config{
				Enable:                    true,
				DeleteRequestCancelPeriod: 0,
			}

			cfg := DeletedSeriesCleanerConfig{
				CleanupInterval:       time.Minute,
				CleanupConcurrency:    1,
				CompactionBlockRanges: compactionRanges,
			}

			reg := prometheus.NewPedanticRegistry()
			logger := log.NewNopLogger()
			scanner := cortex_tsdb.NewUsersScanner(bucketClient, cortex_tsdb.AllUsers, logger)
			cfgProvider := newMockConfigProvider()

			cleaner := NewDeletedSeriesCleaner(cfg, bucketClient, scanner, cfgProvider, purgerCfg, logger, reg)
			require.NoError(t, services.StartAndAwaitRunning(ctx, cleaner))
			defer services.StopAndAwaitTerminated(ctx, cleaner) //nolint:errcheck

			deletionMarkPath := path.Join("user-1", block1.String(), metadata.DeletionMarkFilename)
			exists, err := bucketClient.Exists(ctx, deletionMarkPath)
			require.NoError(t, err)
			require.Equal(t, tc.rewritten, exists)

			assert.Equal(t, float64(1), testutil.ToFloat64(cleaner.runsStarted))
			assert.Equal(t, float64(1), testutil.ToFloat64(cleaner.runsCompleted))
			assert.Equal(t, float64(0), testutil.ToFloat64(cleaner.runsFailed))
			assert.Equal(t, float64(0), testutil.ToFloat64(cleaner.blocksFailedTotal))

			left := 1
			done := 0
			if tc.rewritten {
				left = 0
				done = 1
			}
			assert.Equal(t, float64(left), testutil.ToFloat64(cleaner.tenantPendingRequests.WithLabelValues("user-1")))
			assert.Equal(t, float64(left), testutil.ToFloat64(cleaner.tenantPendingBlocks.WithLabelValues("user-1")))
			assert.Equal(t, float64(done), testutil.ToFloat64(cleaner.tenentBlocksProcessedTotal.WithLabelValues("user-1")))

		})
	}

}

// TODO make this take care of multiple scenarios
func TestDeletedSeriesCleaner_multipleTombstonesActOnBlock(t *testing.T) {
	bucketClient, _ := cortex_testutil.PrepareFilesystemBucket(t)
	username := "user-a"
	ctx := context.Background()

	// create the tombstones
	createTombstone(t, bucketClient, username, 100, 100, 0, 10, []string{"{a=\"series1\"}", "{b=\"series1\"}"}, "request1", cortex_tsdb.StatePending)
	createTombstone(t, bucketClient, username, 100, 100, 5, 15, []string{"{b=\"series2\"}"}, "request2", cortex_tsdb.StatePending)
	//this tombstone is in processed state so should not affect rewrite
	createTombstone(t, bucketClient, username, 100, 100, 0, 30, []string{"{a=\"series3\"}"}, "request3", cortex_tsdb.StateProcessed)

	seriesToWrite := []seriesSamples{
		{lset: labels.Labels{{Name: "a", Value: "series1"}},
			chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {3, 12}, {4, 11}, {20, 20}}}},
		{lset: labels.Labels{{Name: "a", Value: "series1"}, {Name: "b", Value: "series1"}},
			chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}, {{10, 11}, {11, 11}, {20, 20}}}},
		{lset: labels.Labels{{Name: "a", Value: "series1"}, {Name: "b", Value: "series2"}},
			chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}, {{10, 11}, {11, 11}, {20, 20}}}},
		{lset: labels.Labels{{Name: "a", Value: "series3"}},
			chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {3, 12}, {4, 11}, {20, 20}}}},
		{lset: labels.Labels{{Name: "b", Value: "series2"}},
			chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {10, 12}, {11, 11}, {20, 20}}}},
	}

	expectedSeriesAfterDel := []seriesSamples{
		{lset: labels.Labels{{Name: "a", Value: "series1"}},
			chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {3, 12}, {4, 11}, {20, 20}}}},
		{lset: labels.Labels{{Name: "a", Value: "series1"}, {Name: "b", Value: "series1"}},
			chunks: [][]sample{{{11, 11}, {20, 20}}}},
		{lset: labels.Labels{{Name: "a", Value: "series1"}, {Name: "b", Value: "series2"}},
			chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}, {{20, 20}}}},
		{lset: labels.Labels{{Name: "a", Value: "series3"}},
			chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {3, 12}, {4, 11}, {20, 20}}}},
		{lset: labels.Labels{{Name: "b", Value: "series2"}},
			chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {20, 20}}}},
	}

	expectedSamples := 22
	expectedSeries := 5

	// write the block
	createBlockSeries(t, bucketClient, username, seriesToWrite, len(compactionRanges))

	purgerCfg := purger.Config{
		Enable:                    true,
		DeleteRequestCancelPeriod: 0,
	}

	cfg := DeletedSeriesCleanerConfig{
		CleanupInterval:       time.Minute,
		CleanupConcurrency:    1,
		CompactionBlockRanges: compactionRanges,
	}

	reg := prometheus.NewPedanticRegistry()
	logger := log.NewNopLogger()
	scanner := cortex_tsdb.NewUsersScanner(bucketClient, cortex_tsdb.AllUsers, logger)
	cfgProvider := newMockConfigProvider()

	cleaner := NewDeletedSeriesCleaner(cfg, bucketClient, scanner, cfgProvider, purgerCfg, logger, reg)
	newBlocks, err := cleaner.deleteSeriesForUser(ctx, username)

	require.NoError(t, err)
	require.Equal(t, 1, len(newBlocks))

	meta, series := readFromBlock(t, bucketClient, cfgProvider, username, newBlocks[0])

	// Check that the tombstone information was saved in the new meta.json file
	deleteRequestsApplied := getDeletionsAppliedFromMeta(t, meta)

	require.ElementsMatch(t, []string{"request1", "request2"}, deleteRequestsApplied)
	require.Equal(t, expectedSamples, int(meta.Stats.NumSamples))
	require.Equal(t, expectedSeries, int(meta.Stats.NumSeries))

	// Check that the series were deleted
	require.Equal(t, expectedSeriesAfterDel, series)

	assert.Equal(t, float64(0), testutil.ToFloat64(cleaner.blocksFailedTotal))
	assert.Equal(t, float64(2), testutil.ToFloat64(cleaner.tenentTombstonesProcessedTotal.WithLabelValues(username)))
	assert.Equal(t, float64(1), testutil.ToFloat64(cleaner.tenentBlocksProcessedTotal.WithLabelValues(username)))
	assert.Equal(t, float64(30), testutil.ToFloat64(cleaner.tenantSamplesProcessedTotal.WithLabelValues(username)))
	assert.Equal(t, float64(8), testutil.ToFloat64(cleaner.tenantSamplesDeletedTotal.WithLabelValues(username)))
	assert.Equal(t, float64(0), testutil.ToFloat64(cleaner.tenantPendingBlocks.WithLabelValues(username)))
	assert.Equal(t, float64(0), testutil.ToFloat64(cleaner.tenantPendingRequests.WithLabelValues(username)))
}

func TestDeletedSeriesCleaner_RewritingABlockTwice(t *testing.T) {
	userID := "test-user"
	// When rewriting a block twice, all the previous rewrites info inside the meta.json file should remain
	bucketClient, _ := cortex_testutil.PrepareFilesystemBucket(t)
	ctx := context.Background()

	// Write a block to the bucket
	inputSeries := []seriesSamples{
		{lset: labels.Labels{{Name: "name", Value: "series1"}},
			chunks: [][]sample{{{1, 1}, {20, 20}, {30, 30}}}},
	}
	createBlockSeries(t, bucketClient, userID, inputSeries, len(compactionRanges))
	createTombstone(t, bucketClient, userID, 100, 100, 0, 10, []string{"{name=\"series1\"}"}, "request1", cortex_tsdb.StatePending)

	purgerCfg := purger.Config{
		Enable:                    true,
		DeleteRequestCancelPeriod: time.Millisecond,
	}

	cfg := DeletedSeriesCleanerConfig{
		CleanupInterval:       time.Minute,
		CleanupConcurrency:    1,
		CompactionBlockRanges: compactionRanges,
	}

	reg := prometheus.NewPedanticRegistry()
	logger := log.NewNopLogger()
	scanner := cortex_tsdb.NewUsersScanner(bucketClient, cortex_tsdb.AllUsers, logger)
	cfgProvider := newMockConfigProvider()

	cleaner := NewDeletedSeriesCleaner(cfg, bucketClient, scanner, cfgProvider, purgerCfg, logger, reg)

	newBlocks, err := cleaner.deleteSeriesForUser(ctx, userID)

	require.NoError(t, err)
	require.Equal(t, 1, len(newBlocks))

	// read the new block
	meta, outputSeries := readFromBlock(t, bucketClient, cfgProvider, userID, newBlocks[0])

	expectedSeries := []seriesSamples{
		{lset: labels.Labels{{Name: "name", Value: "series1"}},
			chunks: [][]sample{{{20, 20}, {30, 30}}}},
	}

	require.Equal(t, expectedSeries, outputSeries)

	//verify that the new meta.json contains the delete request id
	deleteRequestsApplied := getDeletionsAppliedFromMeta(t, meta)
	require.Equal(t, []string{"request1"}, deleteRequestsApplied)

	assert.Equal(t, float64(0), testutil.ToFloat64(cleaner.blocksFailedTotal))
	assert.Equal(t, float64(1), testutil.ToFloat64(cleaner.tenentTombstonesProcessedTotal.WithLabelValues(userID)))
	assert.Equal(t, float64(1), testutil.ToFloat64(cleaner.tenentBlocksProcessedTotal.WithLabelValues(userID)))
	assert.Equal(t, float64(3), testutil.ToFloat64(cleaner.tenantSamplesProcessedTotal.WithLabelValues(userID)))
	assert.Equal(t, float64(1), testutil.ToFloat64(cleaner.tenantSamplesDeletedTotal.WithLabelValues(userID)))

	// upload new tombstone
	createTombstone(t, bucketClient, userID, 100, 100, 30, 100, []string{"{name=\"series1\"}"}, "request2", cortex_tsdb.StatePending)

	newBlocks, err = cleaner.deleteSeriesForUser(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, 1, len(newBlocks))

	// read the new block
	meta, outputSeries = readFromBlock(t, bucketClient, cfgProvider, userID, newBlocks[0])

	expectedSeries = []seriesSamples{
		{lset: labels.Labels{{Name: "name", Value: "series1"}},
			chunks: [][]sample{{{20, 20}}}},
	}

	require.Equal(t, expectedSeries, outputSeries)

	// veerify that the meta.json file has both of the requests
	deleteRequestsApplied = getDeletionsAppliedFromMeta(t, meta)
	require.Equal(t, []string{"request1", "request2"}, deleteRequestsApplied)

	assert.Equal(t, float64(0), testutil.ToFloat64(cleaner.blocksFailedTotal))
	assert.Equal(t, float64(2), testutil.ToFloat64(cleaner.tenentTombstonesProcessedTotal.WithLabelValues(userID)))
	assert.Equal(t, float64(2), testutil.ToFloat64(cleaner.tenentBlocksProcessedTotal.WithLabelValues(userID)))
	assert.Equal(t, float64(5), testutil.ToFloat64(cleaner.tenantSamplesProcessedTotal.WithLabelValues(userID)))
	assert.Equal(t, float64(2), testutil.ToFloat64(cleaner.tenantSamplesDeletedTotal.WithLabelValues(userID)))

}

type sample struct {
	t int64
	v float64
}

type seriesSamples struct {
	lset   labels.Labels
	chunks [][]sample
}

func createBlockSeries(t *testing.T, bkt objstore.Bucket, userID string, inputSeries []seriesSamples, mockCompactionLevel int) ulid.ULID {
	// The code below was taken from https://github.com/thanos-io/thanos/blob/main/pkg/compactv2/compactor_test.go
	// Create a temporary dir for TSDB.
	tempDir, err := ioutil.TempDir(os.TempDir(), "tsdb")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir) //nolint:errcheck

	blockID := ulid.MustNew(ulid.Now(), nil)
	bDir := path.Join(tempDir, blockID.String())

	require.NoError(t, os.MkdirAll(bDir, os.ModePerm))

	d, err := block.NewDiskWriter(context.Background(), log.NewNopLogger(), bDir)
	require.NoError(t, err)

	defer func() {
		if err != nil {
			_, _ = d.Flush()
			_ = os.RemoveAll(bDir)
		}
	}()

	sort.Slice(inputSeries, func(i, j int) bool {
		return labels.Compare(inputSeries[i].lset, inputSeries[j].lset) < 0
	})

	// Gather symbols.
	symbols := map[string]struct{}{}
	for _, input := range inputSeries {
		for _, l := range input.lset {
			symbols[l.Name] = struct{}{}
			symbols[l.Value] = struct{}{}
		}
	}

	symbolsSlice := make([]string, 0, len(symbols))
	for s := range symbols {
		symbolsSlice = append(symbolsSlice, s)
	}
	sort.Strings(symbolsSlice)
	for _, s := range symbolsSlice {
		require.NoError(t, d.AddSymbol(s))

	}
	var ref uint64
	var blockMinTime int64 = math.MaxInt64
	var blockMaxTime int64 = math.MinInt64
	numSamples := 0
	numSeries := 0
	numChunks := 0

	for _, input := range inputSeries {
		var chks []chunks.Meta
		for _, chk := range input.chunks {
			x := chunkenc.NewXORChunk()
			a, err := x.Appender()
			require.NoError(t, err)

			for _, sa := range chk {
				numSamples++
				a.Append(sa.t, sa.v)
			}
			numChunks++
			chks = append(chks, chunks.Meta{Chunk: x, MinTime: chk[0].t, MaxTime: chk[len(chk)-1].t})
			if chk[0].t < blockMinTime {
				blockMinTime = chk[0].t
			}
			if chk[len(chk)-1].t > blockMaxTime {
				blockMaxTime = chk[len(chk)-1].t
			}
		}
		numSeries++
		err = d.WriteChunks(chks...)
		require.NoError(t, err)
		err = d.AddSeries(ref, input.lset, chks...)
		require.NoError(t, err)

		ref++
	}

	_, err = d.Flush()
	require.NoError(t, err)

	// Copy the block files to the bucket.
	require.NoError(t, filepath.Walk(bDir, func(file string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		// Read the file content in memory.
		content, err := ioutil.ReadFile(file)
		if err != nil {
			return err
		}

		// Upload it to the bucket.
		relPath, err := filepath.Rel(bDir, file)
		if err != nil {
			return err
		}

		return bkt.Upload(context.Background(), path.Join(userID, blockID.String(), relPath), bytes.NewReader(content))
	}))

	blockMeta := tsdb.BlockMeta{
		Version: 1,
		ULID:    blockID,
		MinTime: blockMinTime,
		MaxTime: blockMaxTime,
		Compaction: tsdb.BlockMetaCompaction{
			Level:   mockCompactionLevel,
			Sources: []ulid.ULID{blockID},
		},
		Stats: tsdb.BlockStats{
			NumChunks:  uint64(numChunks),
			NumSamples: uint64(numSamples),
			NumSeries:  uint64(numSeries),
		},
	}

	meta := metadata.Meta{
		BlockMeta: blockMeta,
		Thanos: metadata.Thanos{
			Source: "test",
			Labels: map[string]string{
				"label": "test",
			},
		},
	}

	content, err := json.Marshal(meta)
	require.NoError(t, err)

	require.NoError(t, bkt.Upload(context.Background(), path.Join(userID, blockID.String(), "meta.json"), bytes.NewReader(content)))

	return blockID
}

func readFromBlock(t *testing.T, bkt objstore.Bucket, cfg ConfigProvider, userID string, id ulid.ULID) (*metadata.Meta, []seriesSamples) {
	dir, err := ioutil.TempDir("", "tsdb")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = os.RemoveAll(dir)
	})

	userBucketClient := bucket.NewUserBucketClient(userID, bkt, cfg)

	err = block.Download(context.Background(), util_log.Logger, userBucketClient, id, filepath.Join(dir, id.String()))
	require.NoError(t, err)

	meta, err := metadata.ReadFromDir(filepath.Join(dir, id.String()))
	require.NoError(t, err)
	bDir := path.Join(dir, id.String())

	// The code below was taken from https://github.com/thanos-io/thanos/blob/main/pkg/compactv2/compactor_test.go
	indexr, err := index.NewFileReader(filepath.Join(bDir, block.IndexFilename))
	require.NoError(t, err)
	defer indexr.Close()

	chunkr, err := chunks.NewDirReader(filepath.Join(bDir, block.ChunksDirname), nil)
	require.NoError(t, err)
	defer chunkr.Close()

	all, err := indexr.Postings(index.AllPostingsKey())
	require.NoError(t, err)
	all = indexr.SortedPostings(all)

	var series []seriesSamples
	var chks []chunks.Meta
	for all.Next() {
		s := seriesSamples{}
		require.NoError(t, indexr.Series(all.At(), &s.lset, &chks))

		for _, c := range chks {
			c.Chunk, err = chunkr.Chunk(c.Ref)
			require.NoError(t, err)

			var chk []sample
			iter := c.Chunk.Iterator(nil)
			for iter.Next() {
				sa := sample{}
				sa.t, sa.v = iter.At()
				chk = append(chk, sa)
			}
			require.NoError(t, iter.Err())
			s.chunks = append(s.chunks, chk)
		}
		series = append(series, s)
	}
	require.NoError(t, all.Err())
	return meta, series

}

func uploadTombstone(t testing.TB, bucket objstore.Bucket, userID string, ts *cortex_tsdb.Tombstone) {

	tombstoneFilename := ts.GetFilename()
	path := path.Join(userID, cortex_tsdb.TombstonePath, tombstoneFilename)
	data, err := json.Marshal(ts)

	require.NoError(t, err)
	require.NoError(t, bucket.Upload(context.Background(), path, bytes.NewReader(data)))

	ts.Matchers, err = cortex_tsdb.ParseMatchers(ts.Selectors)
	require.NoError(t, err)
}

func getDeletionsAppliedFromMeta(t testing.TB, meta *metadata.Meta) []string {
	require.NotNil(t, meta.Thanos.Rewrites)
	deleteRequestsApplied := []string{}
	for _, r := range meta.Thanos.Rewrites {
		for _, d := range r.DeletionsApplied {
			deleteRequestsApplied = append(deleteRequestsApplied, d.RequestID)
		}
	}

	return deleteRequestsApplied
}
