package querier

import (
	"context"
	"math"
	"time"

	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/cortexproject/cortex/pkg/chunk/purger"
	"github.com/cortexproject/cortex/pkg/storage/bucket"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/bucketindex"
	"github.com/cortexproject/cortex/pkg/util/services"
)

var (
	errBucketIndexBlocksFinderNotRunning = errors.New("bucket index blocks finder is not running")
	errBucketIndexTooOld                 = errors.New("bucket index is too old and the last time it was updated exceeds the allowed max staleness")
)

type BucketIndexBlocksFinderConfig struct {
	IndexLoader              bucketindex.LoaderConfig
	MaxStalePeriod           time.Duration
	IgnoreDeletionMarksDelay time.Duration
}

// BucketIndexBlocksFinder implements BlocksFinder interface and find blocks in the bucket
// looking up the bucket index.
type BucketIndexBlocksFinder struct {
	services.Service

	cfg    BucketIndexBlocksFinderConfig
	loader *bucketindex.Loader
}

func NewBucketIndexBlocksFinder(cfg BucketIndexBlocksFinderConfig, bkt objstore.Bucket, cfgProvider bucket.TenantConfigProvider, logger log.Logger, reg prometheus.Registerer) *BucketIndexBlocksFinder {
	loader := bucketindex.NewLoader(cfg.IndexLoader, bkt, cfgProvider, logger, reg)

	return &BucketIndexBlocksFinder{
		cfg:     cfg,
		loader:  loader,
		Service: loader,
	}
}

// GetBlocks implements BlocksFinder.
func (f *BucketIndexBlocksFinder) GetBlocks(ctx context.Context, userID string, minT, maxT int64) (bucketindex.Blocks, map[ulid.ULID]*bucketindex.BlockDeletionMark, error) {
	if f.State() != services.Running {
		return nil, nil, errBucketIndexBlocksFinderNotRunning
	}
	if maxT < minT {
		return nil, nil, errInvalidBlocksRange
	}

	// Get the bucket index for this user.
	idx, err := f.loader.GetIndex(ctx, userID)
	if errors.Is(err, bucketindex.ErrIndexNotFound) {
		// This is a legit edge case, happening when a new tenant has not shipped blocks to the storage yet
		// so the bucket index hasn't been created yet.
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}

	// Ensure the bucket index is not too old.
	if time.Since(idx.GetUpdatedAt()) > f.cfg.MaxStalePeriod {
		return nil, nil, errBucketIndexTooOld
	}

	var (
		matchingBlocks        = map[ulid.ULID]*bucketindex.Block{}
		matchingDeletionMarks = map[ulid.ULID]*bucketindex.BlockDeletionMark{}
	)

	// Filter blocks containing samples within the range.
	for _, block := range idx.Blocks {
		if !block.Within(minT, maxT) {
			continue
		}

		matchingBlocks[block.ID] = block
	}

	for _, mark := range idx.BlockDeletionMarks {
		// Filter deletion marks by matching blocks only.
		if _, ok := matchingBlocks[mark.ID]; !ok {
			continue
		}

		// Exclude blocks marked for deletion. This is the same logic as Thanos IgnoreDeletionMarkFilter.
		if time.Since(time.Unix(mark.DeletionTime, 0)).Seconds() > f.cfg.IgnoreDeletionMarksDelay.Seconds() {
			delete(matchingBlocks, mark.ID)
			continue
		}

		matchingDeletionMarks[mark.ID] = mark
	}

	// Convert matching blocks into a list.
	blocks := make(bucketindex.Blocks, 0, len(matchingBlocks))
	for _, b := range matchingBlocks {
		blocks = append(blocks, b)
	}

	return blocks, matchingDeletionMarks, nil
}

func (f *BucketIndexBlocksFinder) GetTombstones(ctx context.Context, userID string, minT int64, maxT int64) (*purger.TombstonesSet, error) {
	if f.State() != services.Running {
		return nil, errBucketIndexBlocksFinderNotRunning
	}
	if maxT < minT {
		return nil, errInvalidBlocksRange
	}

	// Get the bucket index for this user.
	idx, err := f.loader.GetIndex(ctx, userID)
	if errors.Is(err, bucketindex.ErrIndexNotFound) {
		// This is a legit edge case, happening when a new tenant has not shipped blocks to the storage yet
		// so the bucket index hasn't been created yet.
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	// Ensure the bucket index is not too old.
	if time.Since(idx.GetUpdatedAt()) > f.cfg.MaxStalePeriod {
		return nil, errBucketIndexTooOld
	}

	tConverted := []purger.DeleteRequest{}
	var tMinTime int64 = math.MaxInt64
	var tMaxTime int64 = math.MinInt64
	for _, t := range idx.Tombstones {
		if !t.IsOverlappingInterval(minT, maxT) {
			continue
		}

		// Convert the tombstone into a deletion request which was implemented for chunk store deletion
		// This will allow many of the query filtering code to be shared among block/chunk store
		matchers, err := cortex_tsdb.ParseMatchers(t.Selectors)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse tombstone selectors for: %s", t.RequestID)
		}

		request := purger.DeleteRequest{
			StartTime: model.Time(t.StartTime),
			EndTime:   model.Time(t.EndTime),
			Matchers:  [][]*labels.Matcher{matchers},
		}
		tConverted = append(tConverted, request)

		if t.StartTime < tMinTime {
			tMinTime = t.StartTime
		}
		if t.EndTime > tMaxTime {
			tMaxTime = t.EndTime
		}
	}

	// Reduce the interval that tombstone will be applied if possible
	if minT > tMinTime {
		tMinTime = minT
	}
	if maxT < tMaxTime {
		tMaxTime = maxT
	}
	tombstoneSet := purger.NewTombstoneSet(tConverted, model.Time(tMinTime), model.Time(tMaxTime))

	return tombstoneSet, nil

}
