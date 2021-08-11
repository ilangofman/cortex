package querier

import (
	"context"
	"strconv"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/bucketindex"
	"github.com/cortexproject/cortex/pkg/util/services"
)

var (
	errBucketIndexCacheNumLoaderNotRunning = errors.New("bucket index blocks finder is not running")
)

type BucketIndexCacheNumLoaderConfig struct {
	IndexLoader              bucketindex.LoaderConfig
	MaxStalePeriod           time.Duration
	IgnoreDeletionMarksDelay time.Duration
}

// BucketIndexCacheNumLoader implements BlocksFinder interface and find blocks in the bucket
// looking up the bucket index.
type BucketIndexCacheNumLoader struct {
	services.Service

	cfg    BucketIndexCacheNumLoaderConfig
	loader *bucketindex.Loader
	logger log.Logger
}

func NewBucketIndexCacheNumLoader(cfg BucketIndexCacheNumLoaderConfig, bkt objstore.Bucket, cfgProvider bucket.TenantConfigProvider, logger log.Logger, reg prometheus.Registerer) *BucketIndexCacheNumLoader {
	loader := bucketindex.NewLoader(cfg.IndexLoader, bkt, cfgProvider, logger, reg)

	return &BucketIndexCacheNumLoader{
		cfg:     cfg,
		logger:  logger,
		loader:  loader,
		Service: loader,
	}
}

func (f *BucketIndexCacheNumLoader) GetResultsCacheGenNumber(ctx context.Context, tenantIDs []string) string {
	var result string

	if f.State() != services.Running {
		level.Error(f.logger).Log("msg", "error getting resultsCacheGenNumber", "err", errBucketIndexCacheNumLoaderNotRunning)
		return result
	}

	if len(tenantIDs) == 0 {
		return result
	}

	// keep the maximum value that's currently in result
	var maxResults int64

	for pos, tenantID := range tenantIDs {
		var genNumberStr string
		genNumber, err := f.getCacheGenNumber(ctx, tenantID)
		if err != nil {
			level.Error(f.logger).Log("msg", "error getting cache generation number from bucket index", "user", tenantID, "err", err)
		}
		if genNumber > 0 {
			genNumberStr = strconv.FormatInt(genNumber, 10)
		}

		// handle first tenant in the list
		if pos == 0 {
			// short cut if there is only one tenant
			if len(tenantIDs) == 1 {
				return genNumberStr
			}
			// set first tenant string whatever happens next
			result = genNumberStr
		}

		// set results number string if it's higher than the ones before
		if maxResults < genNumber {
			maxResults = genNumber
			result = genNumberStr
		}

	}

	return result

}

func (f *BucketIndexCacheNumLoader) getCacheGenNumber(ctx context.Context, userID string) (int64, error) {
	var cacheGenNum int64 = 0
	// Get the bucket index for this user.
	idx, err := f.loader.GetIndex(ctx, userID)
	if errors.Is(err, bucketindex.ErrIndexNotFound) {
		// This is a legit edge case, happening when a new tenant has not shipped blocks to the storage yet
		// so the bucket index hasn't been created yet.
		return cacheGenNum, nil
	}
	if err != nil {
		return cacheGenNum, err
	}

	// Ensure the bucket index is not too old.
	if time.Since(idx.GetUpdatedAt()) > f.cfg.MaxStalePeriod {
		return cacheGenNum, errBucketIndexTooOld
	}

	return idx.ResultsCacheGenNumber, nil

}
