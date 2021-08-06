package compactor

import (
	"context"
	"crypto/rand"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/cortexproject/cortex/pkg/chunk/purger"
	"github.com/cortexproject/cortex/pkg/storage/bucket"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/concurrency"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/tombstones"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compactv2"
	"github.com/thanos-io/thanos/pkg/objstore"
)

type DeletedSeriesCleanerConfig struct {
	CleanupInterval       time.Duration
	CleanupConcurrency    int
	CompactionBlockRanges cortex_tsdb.DurationList
}

type DeletedSeriesCleaner struct {
	services.Service

	cfg          DeletedSeriesCleanerConfig
	cfgProvider  ConfigProvider
	logger       log.Logger
	bucketClient objstore.Bucket
	usersScanner *cortex_tsdb.UsersScanner
	purgerCfg    purger.Config

	// Keep track of the last owned users.
	lastOwnedUsers []string

	// Metrics.
	runsStarted                    prometheus.Counter
	runsCompleted                  prometheus.Counter
	runsFailed                     prometheus.Counter
	runsLastSuccess                prometheus.Gauge
	blocksRewrittenTotal           prometheus.Counter
	blocksFailedTotal              prometheus.Counter
	tenantPendingBlocks            *prometheus.GaugeVec
	tenantPendingRequests          *prometheus.GaugeVec
	tenentBlocksProcessedTotal     *prometheus.CounterVec
	tenentTombstonesProcessedTotal *prometheus.CounterVec
	tenantSamplesProcessedTotal    *prometheus.CounterVec
	tenantSamplesDeletedTotal      *prometheus.CounterVec
}

func NewDeletedSeriesCleaner(cfg DeletedSeriesCleanerConfig, bucketClient objstore.Bucket, usersScanner *cortex_tsdb.UsersScanner, cfgProvider ConfigProvider, purgerCfg purger.Config, logger log.Logger, reg prometheus.Registerer) *DeletedSeriesCleaner {
	c := &DeletedSeriesCleaner{
		cfg:          cfg,
		bucketClient: bucketClient,
		usersScanner: usersScanner,
		cfgProvider:  cfgProvider,
		purgerCfg:    purgerCfg,
		logger:       log.With(logger, "component", "series-cleaner"),
		runsStarted: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_compactor_series_deletion_runs_started_total",
			Help: "Total number of series deletion cleaner runs started.",
		}),
		runsCompleted: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_compactor_series_deletion_runs_completed_total",
			Help: "Total number of series deletion cleaner successfully completed.",
		}),
		runsFailed: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_compactor_series_deletion_on_blocks_failed_total",
			Help: "Total number of blocks cleanup runs failed.",
		}),
		runsLastSuccess: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_compactor_series_deletion_last_successful_run_timestamp_seconds",
			Help: "Unix timestamp of the last successful series deletion run.",
		}),
		blocksFailedTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_compactor_series_deletion_block_rewrite_failed_total",
			Help: "Total number of blocks failed to be rewritten during series deletion.",
		}),
		tenantPendingRequests: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_compactor_pending_deletion_requests",
			Help: "Total number of deletion requests that require series deletion.",
		}, []string{"user"}),
		tenentTombstonesProcessedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_compactor_deletion_requests_processed_total",
			Help: "Total number of deletion requests that have been fully processed.",
		}, []string{"user"}),
		tenantPendingBlocks: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_compactor_blocks_pending_series_deletion",
			Help: "Total number of blocks that need to be rewritten to processes the series deletion request.",
		}, []string{"user"}),
		tenentBlocksProcessedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_compactor_blocks_processed_for_series_deletion_total",
			Help: "Total number of blocks processed for series deletion.",
		}, []string{"user"}),
		tenantSamplesProcessedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_compactor_samples_processed_during_series_deletion",
			Help: "Total number of samples processed during series deletion, includes both samples reamining and deleted.",
		}, []string{"user"}),
		tenantSamplesDeletedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_compactor_samples_deleted",
			Help: "Total number of samples deleted during series deletion.",
		}, []string{"user"}),
	}
	c.Service = services.NewTimerService(cfg.CleanupInterval, c.ticker, c.ticker, nil)

	return c
}

func (c *DeletedSeriesCleaner) ticker(ctx context.Context) error {
	level.Info(c.logger).Log("msg", "started series deletion cleaner")
	c.runsStarted.Inc()

	err := c.cleanDeletedSeries(ctx)
	if err != nil {
		level.Error(c.logger).Log("msg", "failed to run the series deletion cleaner", "err", err.Error())
		c.runsFailed.Inc()
	} else {
		level.Info(c.logger).Log("msg", "successfully ran the series deletion cleaner")
		c.runsCompleted.Inc()
		c.runsLastSuccess.SetToCurrentTime()
	}

	return nil
}

func (c *DeletedSeriesCleaner) cleanDeletedSeries(ctx context.Context) error {
	users, deleted, err := c.usersScanner.ScanUsers(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to discover users from bucket")
	}

	isActive := util.StringsMap(users)
	isDeleted := util.StringsMap(deleted)
	allUsers := append(users, deleted...)

	// Delete per-tenant metrics for all tenants not belonging anymore to this shard.
	// Such tenants have been moved to a different shard, so their updated metrics will
	// be exported by the new shard.
	for _, userID := range c.lastOwnedUsers {
		if !isActive[userID] && !isDeleted[userID] {
			c.tenantPendingBlocks.DeleteLabelValues(userID)
			c.tenantPendingRequests.DeleteLabelValues(userID)
			c.tenantSamplesProcessedTotal.DeleteLabelValues(userID)
			c.tenantSamplesDeletedTotal.DeleteLabelValues(userID)
			c.tenentBlocksProcessedTotal.DeleteLabelValues(userID)
			c.tenentTombstonesProcessedTotal.DeleteLabelValues(userID)
		}
	}
	c.lastOwnedUsers = allUsers

	return concurrency.ForEachUser(ctx, users, c.cfg.CleanupConcurrency, func(ctx context.Context, userID string) error {
		_, err = c.deleteSeriesForUser(ctx, userID)
		return errors.Wrapf(err, "failed to run the series deletion cleaner for user: %s", userID)
	})
}

func (c *DeletedSeriesCleaner) deleteSeriesForUser(ctx context.Context, userID string) ([]ulid.ULID, error) {
	userLogger := util_log.WithUserID(userID, c.logger)
	userBucket := bucket.NewUserBucketClient(userID, c.bucketClient, c.cfgProvider)

	emptyULID := ulid.ULID{}
	newBlockIDs := []ulid.ULID{}

	level.Info(userLogger).Log("msg", "Running the series deletion cleaner")

	allTombstones, err := cortex_tsdb.GetAllDeleteRequestsForUser(ctx, c.bucketClient, c.cfgProvider, userID)
	if err != nil {
		return newBlockIDs, err
	}

	// Make a slice containing tombstones that can be processed for deletion
	pendingDeletion := []*cortex_tsdb.Tombstone{}
	for _, t := range allTombstones {
		if t.State == cortex_tsdb.StatePending {
			pendingDeletion = append(pendingDeletion, t)
		}
	}

	if len(pendingDeletion) == 0 {
		c.tenantPendingRequests.WithLabelValues(userID).Set(0)
		c.tenantPendingBlocks.WithLabelValues(userID).Set(0)
		level.Info(userLogger).Log("msg", "No pending tombstones exist that are ready for deletion")
		return newBlockIDs, nil
	}

	// Map to see how many blocks need to be processed for each request
	// If after iterating through all blocks, the value is 0
	// Then the deletion request can be updated to processed state
	blocksLeftPerRequest := make(map[string]int, len(pendingDeletion))

	var pendingBlocks int = 0

	err = userBucket.Iter(ctx, "", func(name string) error {
		if err := ctx.Err(); err != nil {
			return err
		}

		id, ok := block.IsBlockDir(name)
		if !ok {
			return nil
		}

		// If the block was marked for deletion, then skip it
		deletionMarkFile := path.Join(id.String(), metadata.DeletionMarkFilename)
		deletionMarkExists, err := userBucket.Exists(ctx, deletionMarkFile)
		if err != nil {
			c.blocksFailedTotal.Inc()
			level.Error(userLogger).Log("msg", "failed to check if deletion mark exists for block", "block", id, "err", err)
			// Since we don't know if this block has been deleted or not, we should not proceed with series deletion.
			// To be safe, increase the number of blocks per request so that the tombstones aren't moved to processed
			// state while there still remains a block for deletion.
			for _, t := range pendingDeletion {
				blocksLeftPerRequest[t.RequestID] += 1
			}
			return nil //continue with the other blocks
		}
		if deletionMarkExists {
			return nil
		}

		meta, err := block.DownloadMeta(ctx, userLogger, userBucket, id)
		if err != nil {
			c.blocksFailedTotal.Inc()
			level.Error(userLogger).Log("msg", "failed to download block meta.json file", "block", id, "err", err)
			// don't know if this block would have required deletion, so to be safe, need to increase the
			// blocks left per request. Otherwise, the cleaner might think that all blocks have been re-written and
			// the request can be moved to processed when there is still a block that requires deletion.
			for _, t := range pendingDeletion {
				blocksLeftPerRequest[t.RequestID] += 1
			}
			return nil //continue with the other blocks
		}

		// Keep track of which deletions have already been applied to this block
		deletionRequestsApplied := make(map[string]struct{})
		if meta.Thanos.Rewrites != nil {
			for _, r := range meta.Thanos.Rewrites {
				for _, d := range r.DeletionsApplied {
					deletionRequestsApplied[d.RequestID] = struct{}{}
				}
			}
		}

		blockForDeletion := false
		var deletions []metadata.DeletionRequest
		for _, t := range pendingDeletion {
			if meta.MinTime > t.EndTime || t.StartTime > meta.MaxTime {
				// tombstone time doesn't overlap with the block timestamps
				continue
			}

			if _, processed := deletionRequestsApplied[t.RequestID]; processed {
				// Request has already been processed for this block
				continue
			}

			blockForDeletion = true
			blocksLeftPerRequest[t.RequestID] += 1

			if time.Since(t.GetCreateTime()) < c.purgerCfg.DeleteRequestCancelPeriod {
				// the cancellation period is not over yet
				continue
			}

			if meta.Compaction.Level < len(c.cfg.CompactionBlockRanges) {
				// Unable to process for deletion until the block is no longer going to be compacted
				continue
			}

			deletion := metadata.DeletionRequest{
				RequestID: t.RequestID,
				Matchers:  t.Matchers,
				Intervals: tombstones.Intervals{{Mint: t.StartTime, Maxt: t.EndTime}}}

			deletions = append(deletions, deletion)

		}

		if blockForDeletion {
			pendingBlocks++
		}

		// no tombstones apply to this block
		if len(deletions) == 0 {
			return nil
		}

		newID, err := c.rewriteBlockWithoutDeletedSeries(ctx, userID, id, deletions)
		if err != nil {
			level.Error(userLogger).Log("msg", "failed to rewrite the block without the deleted series", "block", id, "deletions", deletions, "err", err)
			c.blocksFailedTotal.Inc()
			return nil // continue with the other blocks
		}
		if newID != emptyULID {
			newBlockIDs = append(newBlockIDs, newID)
		}

		for _, del := range deletions {
			blocksLeftPerRequest[del.RequestID]--
		}
		pendingBlocks--
		return nil
	})

	if err != nil {
		return newBlockIDs, err
	}

	c.tenantPendingBlocks.WithLabelValues(userID).Set(float64(pendingBlocks))

	pendingRequestsRemaning := 0
	for _, t := range pendingDeletion {
		blocksRemaining := blocksLeftPerRequest[t.RequestID]
		if blocksRemaining == 0 {
			level.Info(userLogger).Log("msg", "Finished processing deletions for tombstone", "requestID", t.RequestID)
			_, err := cortex_tsdb.UpdateTombstoneState(ctx, c.bucketClient, c.cfgProvider, t, cortex_tsdb.StateProcessed)
			if err != nil {
				level.Error(userLogger).Log("msg", "failed to update tombstone state", "requestID", t.RequestID, "err", err)
				// Will try to update again next time the cleaner is ran.
				// No more blocks will be re-written since they all should have the requestID inside the meta.json
				continue // continue updating the rest of the tombstones

			}
			c.tenentTombstonesProcessedTotal.WithLabelValues(userID).Inc()
		} else {
			pendingRequestsRemaning++
		}
	}

	c.tenantPendingRequests.WithLabelValues(userID).Set(float64(pendingRequestsRemaning))
	return newBlockIDs, nil
}

func (c *DeletedSeriesCleaner) rewriteBlockWithoutDeletedSeries(ctx context.Context, userID string, id ulid.ULID, del []metadata.DeletionRequest) (ulid.ULID, error) {
	// The logic for the block rewrite is based off the Thanos bucket rewrite tool
	// https://github.com/thanos-io/thanos/blob/main/cmd/thanos/tools_bucket.go

	userLogger := util_log.WithUserID(userID, c.logger)
	userBucketClient := bucket.NewUserBucketClient(userID, c.bucketClient, c.cfgProvider)
	chunkPool := chunkenc.NewPool()
	changeLog := compactv2.NewChangeLog(ioutil.Discard)

	var emptyULID ulid.ULID = ulid.ULID{}

	level.Info(userLogger).Log("msg", "downloading block for rewrite", "source", id)
	tmpDir := filepath.Join(os.TempDir(), "series-deletion-rewrite")
	if err := block.Download(ctx, userLogger, userBucketClient, id, filepath.Join(tmpDir, id.String())); err != nil {
		return emptyULID, errors.Wrapf(err, "download %v", id)
	}

	meta, err := metadata.ReadFromDir(filepath.Join(tmpDir, id.String()))
	if err != nil {
		return emptyULID, errors.Wrapf(err, "read meta of %v", id)
	}
	b, err := tsdb.OpenBlock(userLogger, filepath.Join(tmpDir, id.String()), chunkPool)
	if err != nil {
		return emptyULID, errors.Wrapf(err, "open block %v", id)
	}

	p := compactv2.NewProgressLogger(c.logger, int(b.Meta().Stats.NumSeries))
	newID := ulid.MustNew(ulid.Now(), rand.Reader)
	meta.ULID = newID
	meta.Thanos.Rewrites = append(meta.Thanos.Rewrites, metadata.Rewrite{
		Sources:          meta.Compaction.Sources,
		DeletionsApplied: del,
	})

	// add the block to previous block to the sources and parents
	// This will prevent the compactor from merging the old block
	// and new block together due to overlapping time periods
	meta.Compaction.Sources = append(meta.Compaction.Sources, id)
	meta.Compaction.Parents = append(meta.Compaction.Parents, tsdb.BlockDesc{
		ULID:    id,
		MinTime: meta.MinTime,
		MaxTime: meta.MaxTime,
	})
	meta.Thanos.Source = metadata.BucketRewriteSource

	if err := os.MkdirAll(filepath.Join(tmpDir, newID.String()), os.ModePerm); err != nil {
		return emptyULID, err
	}

	d, err := block.NewDiskWriter(ctx, userLogger, filepath.Join(tmpDir, newID.String()))
	if err != nil {
		return emptyULID, err
	}

	numSamplesBeforeDeletion := meta.Stats.NumSamples

	comp := compactv2.New(tmpDir, userLogger, changeLog, chunkPool)
	level.Info(userLogger).Log("msg", "starting series deletion rewrite for block", "block", id)
	if err := comp.WriteSeries(ctx, []block.Reader{b}, d, p, compactv2.WithDeletionModifier(del...)); err != nil {
		return emptyULID, errors.Wrapf(err, "writing series from %v to %v", id, newID)
	}
	meta.Stats, err = d.Flush()
	if err != nil {
		return emptyULID, errors.Wrap(err, "flush")
	}

	if numSamplesBeforeDeletion == meta.Stats.NumSamples {
		// nothing has been deleted, no need to upload the new block
		c.tenentBlocksProcessedTotal.WithLabelValues(userID).Inc()
		return emptyULID, nil
	} else if meta.Stats.NumSamples == 0 {
		level.Info(userLogger).Log("msg", "No samples from the block were deleted", "block", id)
		// everything has been deleted
		// the new block would be empty, so no need to upload it, still need to mark the old one for deletion
		if err := block.MarkForDeletion(
			ctx, userLogger, userBucketClient, id,
			"block has been rewritten without deleted data. All samples inside of block were deleted.",
			c.tenentBlocksProcessedTotal.WithLabelValues(userID)); err != nil {
			level.Warn(userLogger).Log("msg", "failed to mark block for deletion", "block", id, "err", err)
			return emptyULID, err
		}
		level.Info(userLogger).Log("msg", "Block was completely deleted", "block", id)
		return emptyULID, nil
	}

	if err := meta.WriteToDir(userLogger, filepath.Join(tmpDir, newID.String())); err != nil {
		return emptyULID, err
	}

	level.Info(userLogger).Log("msg", "uploading new block after series deletion", "source", id, "new", newID)
	if err := block.Upload(ctx, userLogger, userBucketClient, filepath.Join(tmpDir, newID.String()), metadata.NoneFunc); err != nil {
		return newID, errors.Wrap(err, "upload")
	}

	level.Info(userLogger).Log("msg", "marking block for deletion after it has been rewritten", "block", id)
	// the old block is no longer required as it has been rewritten, can delete it now
	if err := block.MarkForDeletion(
		ctx, userLogger, userBucketClient, id,
		fmt.Sprintf("block has been rewritten without deleted data, new block=%v", newID.String()),
		c.tenentBlocksProcessedTotal.WithLabelValues(userID)); err != nil {

		level.Warn(userLogger).Log("msg", "failed to mark block for deletion", "block", id, "err", err)
	}

	level.Info(userLogger).Log("msg", "Rewrite completed, new block uploaded", "source", id, "new", newID)

	c.tenantSamplesProcessedTotal.WithLabelValues(userID).Add(float64(numSamplesBeforeDeletion))
	c.tenantSamplesDeletedTotal.WithLabelValues(userID).Add(float64(numSamplesBeforeDeletion - meta.Stats.NumSamples))

	return newID, nil
}
