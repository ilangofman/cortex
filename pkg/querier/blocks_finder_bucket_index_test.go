package querier

import (
	"context"
	"math"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/cortexproject/cortex/pkg/chunk/purger"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/bucketindex"
	cortex_testutil "github.com/cortexproject/cortex/pkg/storage/tsdb/testutil"
	"github.com/cortexproject/cortex/pkg/util/services"
)

func TestBucketIndexBlocksFinder_GetBlocks(t *testing.T) {
	const userID = "user-1"

	ctx := context.Background()
	bkt, _ := cortex_testutil.PrepareFilesystemBucket(t)

	// Mock a bucket index.
	block1 := &bucketindex.Block{ID: ulid.MustNew(1, nil), MinTime: 10, MaxTime: 15}
	block2 := &bucketindex.Block{ID: ulid.MustNew(2, nil), MinTime: 12, MaxTime: 20}
	block3 := &bucketindex.Block{ID: ulid.MustNew(3, nil), MinTime: 20, MaxTime: 30}
	block4 := &bucketindex.Block{ID: ulid.MustNew(4, nil), MinTime: 30, MaxTime: 40}
	block5 := &bucketindex.Block{ID: ulid.MustNew(5, nil), MinTime: 30, MaxTime: 40} // Time range overlaps with block4, but this block deletion mark is above the threshold.
	mark3 := &bucketindex.BlockDeletionMark{ID: block3.ID, DeletionTime: time.Now().Unix()}
	mark5 := &bucketindex.BlockDeletionMark{ID: block5.ID, DeletionTime: time.Now().Add(-2 * time.Hour).Unix()}

	require.NoError(t, bucketindex.WriteIndex(ctx, bkt, userID, nil, &bucketindex.Index{
		Version:            bucketindex.IndexVersion1,
		Blocks:             bucketindex.Blocks{block1, block2, block3, block4, block5},
		BlockDeletionMarks: bucketindex.BlockDeletionMarks{mark3, mark5},
		UpdatedAt:          time.Now().Unix(),
	}))

	finder := prepareBucketIndexBlocksFinder(t, bkt)

	tests := map[string]struct {
		minT           int64
		maxT           int64
		expectedBlocks bucketindex.Blocks
		expectedMarks  map[ulid.ULID]*bucketindex.BlockDeletionMark
	}{
		"no matching block because the range is too low": {
			minT:          0,
			maxT:          5,
			expectedMarks: map[ulid.ULID]*bucketindex.BlockDeletionMark{},
		},
		"no matching block because the range is too high": {
			minT:          50,
			maxT:          60,
			expectedMarks: map[ulid.ULID]*bucketindex.BlockDeletionMark{},
		},
		"matching all blocks": {
			minT:           0,
			maxT:           60,
			expectedBlocks: bucketindex.Blocks{block4, block3, block2, block1},
			expectedMarks: map[ulid.ULID]*bucketindex.BlockDeletionMark{
				block3.ID: mark3,
			},
		},
		"query range starting at a block maxT": {
			minT:           block3.MaxTime,
			maxT:           60,
			expectedBlocks: bucketindex.Blocks{block4},
			expectedMarks:  map[ulid.ULID]*bucketindex.BlockDeletionMark{},
		},
		"query range ending at a block minT": {
			minT:           block3.MinTime,
			maxT:           block4.MinTime,
			expectedBlocks: bucketindex.Blocks{block4, block3},
			expectedMarks: map[ulid.ULID]*bucketindex.BlockDeletionMark{
				block3.ID: mark3,
			},
		},
		"query range within a single block": {
			minT:           block3.MinTime + 2,
			maxT:           block3.MaxTime - 2,
			expectedBlocks: bucketindex.Blocks{block3},
			expectedMarks: map[ulid.ULID]*bucketindex.BlockDeletionMark{
				block3.ID: mark3,
			},
		},
		"query range within multiple blocks": {
			minT:           13,
			maxT:           16,
			expectedBlocks: bucketindex.Blocks{block2, block1},
			expectedMarks:  map[ulid.ULID]*bucketindex.BlockDeletionMark{},
		},
		"query range matching exactly a single block": {
			minT:           block3.MinTime,
			maxT:           block3.MaxTime - 1,
			expectedBlocks: bucketindex.Blocks{block3},
			expectedMarks: map[ulid.ULID]*bucketindex.BlockDeletionMark{
				block3.ID: mark3,
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			blocks, deletionMarks, err := finder.GetBlocks(ctx, userID, testData.minT, testData.maxT)
			require.NoError(t, err)
			require.ElementsMatch(t, testData.expectedBlocks, blocks)
			require.Equal(t, testData.expectedMarks, deletionMarks)
		})
	}
}

func BenchmarkBucketIndexBlocksFinder_GetBlocks(b *testing.B) {
	const (
		numBlocks        = 1000
		numDeletionMarks = 100
		userID           = "user-1"
	)

	ctx := context.Background()
	bkt, _ := cortex_testutil.PrepareFilesystemBucket(b)

	// Mock a bucket index.
	idx := &bucketindex.Index{
		Version:   bucketindex.IndexVersion1,
		UpdatedAt: time.Now().Unix(),
	}

	for i := 1; i <= numBlocks; i++ {
		id := ulid.MustNew(uint64(i), nil)
		minT := int64(i * 10)
		maxT := int64((i + 1) * 10)
		idx.Blocks = append(idx.Blocks, &bucketindex.Block{ID: id, MinTime: minT, MaxTime: maxT})
	}
	for i := 1; i <= numDeletionMarks; i++ {
		id := ulid.MustNew(uint64(i), nil)
		idx.BlockDeletionMarks = append(idx.BlockDeletionMarks, &bucketindex.BlockDeletionMark{ID: id, DeletionTime: time.Now().Unix()})
	}
	require.NoError(b, bucketindex.WriteIndex(ctx, bkt, userID, nil, idx))
	finder := prepareBucketIndexBlocksFinder(b, bkt)

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		blocks, marks, err := finder.GetBlocks(ctx, userID, 100, 200)
		if err != nil || len(blocks) != 11 || len(marks) != 11 {
			b.Fail()
		}
	}
}

func TestBucketIndexBlocksFinder_GetTombstones(t *testing.T) {
	const userID = "user-1"

	ctx := context.Background()
	bkt, _ := cortex_testutil.PrepareFilesystemBucket(t)

	// Mock a bucket index.
	t1 := cortex_tsdb.NewTombstone(userID, 0, 0, 10, 15, []string{"series1"}, "request1", cortex_tsdb.StatePending)
	t1.Matchers, _ = cortex_tsdb.ParseMatchers(t1.Selectors)
	t2 := cortex_tsdb.NewTombstone(userID, 0, 0, 12, 20, []string{"series2"}, "request2", cortex_tsdb.StatePending)
	t2.Matchers, _ = cortex_tsdb.ParseMatchers(t2.Selectors)
	t3 := cortex_tsdb.NewTombstone(userID, 0, 0, 20, 30, []string{"series3"}, "request3", cortex_tsdb.StatePending)
	t3.Matchers, _ = cortex_tsdb.ParseMatchers(t3.Selectors)
	t4 := cortex_tsdb.NewTombstone(userID, 0, 0, 30, 40, []string{"series4"}, "request4", cortex_tsdb.StatePending)
	t4.Matchers, _ = cortex_tsdb.ParseMatchers(t4.Selectors)

	require.NoError(t, bucketindex.WriteIndex(ctx, bkt, userID, nil, &bucketindex.Index{
		Version:    bucketindex.IndexVersion1,
		Tombstones: bucketindex.SeriesDeletionTombstones{t1, t2, t3, t4},
		UpdatedAt:  time.Now().Unix(),
	}))

	finder := prepareBucketIndexBlocksFinder(t, bkt)

	tests := map[string]struct {
		minT                 int64
		maxT                 int64
		expectedTombstoneSet *purger.TombstonesSet
	}{
		"no matching tombstones because the range is too low": {
			minT:                 0,
			maxT:                 5,
			expectedTombstoneSet: purger.NewTombstoneSet([]purger.DeleteRequest{}, math.MaxInt64, math.MinInt64),
		},
		"no matching tombstones because the range is too high": {
			minT:                 50,
			maxT:                 60,
			expectedTombstoneSet: purger.NewTombstoneSet([]purger.DeleteRequest{}, math.MaxInt64, math.MinInt64),
		},
		"matching all tombstones": {
			minT: 0,
			maxT: 60,
			expectedTombstoneSet: purger.NewTombstoneSet([]purger.DeleteRequest{
				{StartTime: model.Time(t1.StartTime), EndTime: model.Time(t1.EndTime), Matchers: [][]*labels.Matcher{t1.Matchers}},
				{StartTime: model.Time(t2.StartTime), EndTime: model.Time(t2.EndTime), Matchers: [][]*labels.Matcher{t2.Matchers}},
				{StartTime: model.Time(t3.StartTime), EndTime: model.Time(t3.EndTime), Matchers: [][]*labels.Matcher{t3.Matchers}},
				{StartTime: model.Time(t4.StartTime), EndTime: model.Time(t4.EndTime), Matchers: [][]*labels.Matcher{t4.Matchers}},
			}, model.Time(t1.StartTime), model.Time(t4.EndTime)),
		},
		"query range starting at a tombstone end time": {
			minT: t3.EndTime,
			maxT: 60,
			expectedTombstoneSet: purger.NewTombstoneSet([]purger.DeleteRequest{
				{StartTime: model.Time(t4.StartTime), EndTime: model.Time(t4.EndTime), Matchers: [][]*labels.Matcher{t4.Matchers}},
			}, model.Time(t4.StartTime), model.Time(t4.EndTime)),
		},
		"query range ending at a tombstone start time": {
			minT: t3.StartTime,
			maxT: t4.EndTime,
			expectedTombstoneSet: purger.NewTombstoneSet([]purger.DeleteRequest{
				{StartTime: model.Time(t3.StartTime), EndTime: model.Time(t3.EndTime), Matchers: [][]*labels.Matcher{t3.Matchers}},
				{StartTime: model.Time(t4.StartTime), EndTime: model.Time(t4.EndTime), Matchers: [][]*labels.Matcher{t4.Matchers}},
			}, model.Time(t3.StartTime), model.Time(t4.EndTime)),
		},
		"query range within a single tombstone": {
			minT: t3.StartTime + 2,
			maxT: t3.EndTime - 2,
			expectedTombstoneSet: purger.NewTombstoneSet([]purger.DeleteRequest{
				{StartTime: model.Time(t3.StartTime), EndTime: model.Time(t3.EndTime), Matchers: [][]*labels.Matcher{t3.Matchers}},
			}, model.Time(t3.StartTime+2), model.Time(t3.EndTime-2)),
		},
		"query range within multiple tombstones": {
			minT: 13,
			maxT: 16,
			expectedTombstoneSet: purger.NewTombstoneSet([]purger.DeleteRequest{
				{StartTime: model.Time(t1.StartTime), EndTime: model.Time(t1.EndTime), Matchers: [][]*labels.Matcher{t1.Matchers}},
				{StartTime: model.Time(t2.StartTime), EndTime: model.Time(t2.EndTime), Matchers: [][]*labels.Matcher{t2.Matchers}},
			}, 13, 16),
		},
		"query range matching exactly a single block": {
			minT: t3.StartTime,
			maxT: t3.EndTime - 1,
			expectedTombstoneSet: purger.NewTombstoneSet([]purger.DeleteRequest{
				{StartTime: model.Time(t3.StartTime), EndTime: model.Time(t3.EndTime), Matchers: [][]*labels.Matcher{t3.Matchers}},
			}, model.Time(t3.StartTime), model.Time(t3.EndTime-1)),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			tombstoneSet, err := finder.GetTombstones(ctx, userID, testData.minT, testData.maxT)
			require.NoError(t, err)
			require.Equal(t, testData.expectedTombstoneSet, tombstoneSet)
		})
	}
}

func TestBucketIndexBlocksFinder_BucketIndexDoesNotExist(t *testing.T) {
	const userID = "user-1"

	ctx := context.Background()
	bkt, _ := cortex_testutil.PrepareFilesystemBucket(t)
	finder := prepareBucketIndexBlocksFinder(t, bkt)

	blocks, deletionMarks, err := finder.GetBlocks(ctx, userID, 10, 20)
	require.NoError(t, err)
	assert.Empty(t, blocks)
	assert.Empty(t, deletionMarks)

	tombstoneSet, err := finder.GetTombstones(ctx, userID, 10, 20)
	require.NoError(t, err)
	assert.Empty(t, tombstoneSet)
}

func TestBucketIndexBlocksFinder_BucketIndexIsCorrupted(t *testing.T) {
	const userID = "user-1"

	ctx := context.Background()
	bkt, _ := cortex_testutil.PrepareFilesystemBucket(t)
	finder := prepareBucketIndexBlocksFinder(t, bkt)

	// Upload a corrupted bucket index.
	require.NoError(t, bkt.Upload(ctx, path.Join(userID, bucketindex.IndexCompressedFilename), strings.NewReader("invalid}!")))

	_, _, err := finder.GetBlocks(ctx, userID, 10, 20)
	require.Equal(t, bucketindex.ErrIndexCorrupted, err)

	_, err = finder.GetTombstones(ctx, userID, 10, 20)
	require.Equal(t, bucketindex.ErrIndexCorrupted, err)
}

func TestBucketIndexBlocksFinder_BucketIndexIsTooOld(t *testing.T) {
	const userID = "user-1"

	ctx := context.Background()
	bkt, _ := cortex_testutil.PrepareFilesystemBucket(t)
	finder := prepareBucketIndexBlocksFinder(t, bkt)

	require.NoError(t, bucketindex.WriteIndex(ctx, bkt, userID, nil, &bucketindex.Index{
		Version:            bucketindex.IndexVersion1,
		Blocks:             bucketindex.Blocks{},
		BlockDeletionMarks: bucketindex.BlockDeletionMarks{},
		UpdatedAt:          time.Now().Add(-2 * time.Hour).Unix(),
	}))

	_, _, err := finder.GetBlocks(ctx, userID, 10, 20)
	require.Equal(t, errBucketIndexTooOld, err)

	_, err = finder.GetTombstones(ctx, userID, 10, 20)
	require.Equal(t, errBucketIndexTooOld, err)
}

func prepareBucketIndexBlocksFinder(t testing.TB, bkt objstore.Bucket) *BucketIndexBlocksFinder {
	ctx := context.Background()
	cfg := BucketIndexBlocksFinderConfig{
		IndexLoader: bucketindex.LoaderConfig{
			CheckInterval:         time.Minute,
			UpdateOnStaleInterval: time.Minute,
			UpdateOnErrorInterval: time.Minute,
			IdleTimeout:           time.Minute,
		},
		MaxStalePeriod:           time.Hour,
		IgnoreDeletionMarksDelay: time.Hour,
	}

	finder := NewBucketIndexBlocksFinder(cfg, bkt, nil, log.NewNopLogger(), nil)
	require.NoError(t, services.StartAndAwaitRunning(ctx, finder))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, finder))
	})

	return finder
}
