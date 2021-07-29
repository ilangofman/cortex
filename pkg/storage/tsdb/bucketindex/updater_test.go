package bucketindex

import (
	"bytes"
	"context"
	"path"
	"testing"
	"time"

	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/testutil"
)

const (
	blockDeletionDelay    = time.Minute * 5
	blocksCleanupInterval = time.Minute * 10
)

func TestUpdater_UpdateIndex(t *testing.T) {
	const userID = "user-1"

	bkt, _ := testutil.PrepareFilesystemBucket(t)

	ctx := context.Background()
	logger := log.NewNopLogger()

	// Generate the initial index.
	bkt = BucketWithGlobalMarkers(bkt)
	block1 := testutil.MockStorageBlock(t, bkt, userID, 10, 20)
	block2 := testutil.MockStorageBlock(t, bkt, userID, 20, 30)
	block2Mark := testutil.MockStorageDeletionMark(t, bkt, userID, block2)
	tombstone1 := testutil.MockTombstone(t, bkt, userID, 0, 0, 0, 0, []string{"series"}, "request1", cortex_tsdb.StatePending)
	tombstone2 := testutil.MockTombstone(t, bkt, userID, 0, 0, 0, 0, []string{"series"}, "request2", cortex_tsdb.StatePending)

	w := NewUpdater(bkt, userID, nil, blockDeletionDelay, blocksCleanupInterval, newMockBucketStoreCfg(), logger)
	returnedIdx, _, err := w.UpdateIndex(ctx, nil)
	require.NoError(t, err)
	assertBucketIndexEqual(t, returnedIdx, bkt, userID,
		[]tsdb.BlockMeta{block1, block2},
		[]*metadata.DeletionMark{block2Mark},
		[]*cortex_tsdb.Tombstone{tombstone1, tombstone2})

	// Create new blocks, and update the index.
	block3 := testutil.MockStorageBlock(t, bkt, userID, 30, 40)
	block4 := testutil.MockStorageBlock(t, bkt, userID, 40, 50)
	block4Mark := testutil.MockStorageDeletionMark(t, bkt, userID, block4)
	tombstone3 := testutil.MockTombstone(t, bkt, userID, 0, 0, 0, 0, []string{"series"}, "request3", cortex_tsdb.StatePending)

	returnedIdx, _, err = w.UpdateIndex(ctx, returnedIdx)
	require.NoError(t, err)
	assertBucketIndexEqual(t, returnedIdx, bkt, userID,
		[]tsdb.BlockMeta{block1, block2, block3, block4},
		[]*metadata.DeletionMark{block2Mark, block4Mark},
		[]*cortex_tsdb.Tombstone{tombstone1, tombstone2, tombstone3})

	// Hard delete a block and tombstone and update the index.
	require.NoError(t, block.Delete(ctx, log.NewNopLogger(), bucket.NewUserBucketClient(userID, bkt, nil), block2.ULID))
	require.NoError(t, cortex_tsdb.DeleteTombstoneFile(ctx, bkt, nil, userID, tombstone1.RequestID, tombstone1.State))

	returnedIdx, _, err = w.UpdateIndex(ctx, returnedIdx)
	require.NoError(t, err)
	assertBucketIndexEqual(t, returnedIdx, bkt, userID,
		[]tsdb.BlockMeta{block1, block3, block4},
		[]*metadata.DeletionMark{block4Mark},
		[]*cortex_tsdb.Tombstone{tombstone2, tombstone3})
}

func TestUpdater_UpdateIndex_ShouldSkipPartialBlocks(t *testing.T) {
	const userID = "user-1"

	bkt, _ := testutil.PrepareFilesystemBucket(t)

	ctx := context.Background()
	logger := log.NewNopLogger()

	// Mock some blocks in the storage.
	bkt = BucketWithGlobalMarkers(bkt)
	block1 := testutil.MockStorageBlock(t, bkt, userID, 10, 20)
	block2 := testutil.MockStorageBlock(t, bkt, userID, 20, 30)
	block3 := testutil.MockStorageBlock(t, bkt, userID, 30, 40)
	block2Mark := testutil.MockStorageDeletionMark(t, bkt, userID, block2)
	tombstone1 := testutil.MockTombstone(t, bkt, userID, 0, 0, 0, 0, []string{"series"}, "request1", cortex_tsdb.StatePending)

	// Delete a block's meta.json to simulate a partial block.
	require.NoError(t, bkt.Delete(ctx, path.Join(userID, block3.ULID.String(), metadata.MetaFilename)))

	w := NewUpdater(bkt, userID, nil, blockDeletionDelay, blocksCleanupInterval, newMockBucketStoreCfg(), logger)
	idx, partials, err := w.UpdateIndex(ctx, nil)
	require.NoError(t, err)
	assertBucketIndexEqual(t, idx, bkt, userID,
		[]tsdb.BlockMeta{block1, block2},
		[]*metadata.DeletionMark{block2Mark},
		[]*cortex_tsdb.Tombstone{tombstone1})

	assert.Len(t, partials, 1)
	assert.True(t, errors.Is(partials[block3.ULID], ErrBlockMetaNotFound))
}

func TestUpdater_UpdateIndex_ShouldSkipBlocksWithCorruptedMeta(t *testing.T) {
	const userID = "user-1"

	bkt, _ := testutil.PrepareFilesystemBucket(t)

	ctx := context.Background()
	logger := log.NewNopLogger()

	// Mock some blocks in the storage.
	bkt = BucketWithGlobalMarkers(bkt)
	block1 := testutil.MockStorageBlock(t, bkt, userID, 10, 20)
	block2 := testutil.MockStorageBlock(t, bkt, userID, 20, 30)
	block3 := testutil.MockStorageBlock(t, bkt, userID, 30, 40)
	block2Mark := testutil.MockStorageDeletionMark(t, bkt, userID, block2)
	tombstone1 := testutil.MockTombstone(t, bkt, userID, 0, 0, 0, 0, []string{"series"}, "request1", cortex_tsdb.StatePending)

	// Overwrite a block's meta.json with invalid data.
	require.NoError(t, bkt.Upload(ctx, path.Join(userID, block3.ULID.String(), metadata.MetaFilename), bytes.NewReader([]byte("invalid!}"))))

	w := NewUpdater(bkt, userID, nil, blockDeletionDelay, blocksCleanupInterval, newMockBucketStoreCfg(), logger)
	idx, partials, err := w.UpdateIndex(ctx, nil)
	require.NoError(t, err)
	assertBucketIndexEqual(t, idx, bkt, userID,
		[]tsdb.BlockMeta{block1, block2},
		[]*metadata.DeletionMark{block2Mark},
		[]*cortex_tsdb.Tombstone{tombstone1})

	assert.Len(t, partials, 1)
	assert.True(t, errors.Is(partials[block3.ULID], ErrBlockMetaCorrupted))
}

func TestUpdater_UpdateIndex_ShouldSkipCorruptedDeletionMarks(t *testing.T) {
	const userID = "user-1"

	bkt, _ := testutil.PrepareFilesystemBucket(t)

	ctx := context.Background()
	logger := log.NewNopLogger()

	// Mock some blocks in the storage.
	bkt = BucketWithGlobalMarkers(bkt)
	block1 := testutil.MockStorageBlock(t, bkt, userID, 10, 20)
	block2 := testutil.MockStorageBlock(t, bkt, userID, 20, 30)
	block3 := testutil.MockStorageBlock(t, bkt, userID, 30, 40)
	block2Mark := testutil.MockStorageDeletionMark(t, bkt, userID, block2)

	// Overwrite a block's deletion-mark.json with invalid data.
	require.NoError(t, bkt.Upload(ctx, path.Join(userID, block2Mark.ID.String(), metadata.DeletionMarkFilename), bytes.NewReader([]byte("invalid!}"))))

	w := NewUpdater(bkt, userID, nil, blockDeletionDelay, blocksCleanupInterval, newMockBucketStoreCfg(), logger)
	idx, partials, err := w.UpdateIndex(ctx, nil)
	require.NoError(t, err)
	assertBucketIndexEqual(t, idx, bkt, userID,
		[]tsdb.BlockMeta{block1, block2, block3},
		[]*metadata.DeletionMark{},
		[]*cortex_tsdb.Tombstone{})
	assert.Empty(t, partials)
}

func TestUpdater_UpdateIndex_ShouldSkipCorruptedTombstones(t *testing.T) {
	const userID = "user-1"

	bkt, _ := testutil.PrepareFilesystemBucket(t)

	ctx := context.Background()
	logger := log.NewNopLogger()

	// Mock some blocks  and tombstones in the storage.
	bkt = BucketWithGlobalMarkers(bkt)
	block1 := testutil.MockStorageBlock(t, bkt, userID, 10, 20)
	tombstone1 := testutil.MockTombstone(t, bkt, userID, 0, 0, 0, 0, []string{"series"}, "request1", cortex_tsdb.StatePending)
	tombstone2 := testutil.MockTombstone(t, bkt, userID, 0, 0, 0, 0, []string{"series"}, "request2", cortex_tsdb.StatePending)

	// Overwrite a tombstone with invalid data.
	require.NoError(t, bkt.Upload(ctx, path.Join(userID, cortex_tsdb.TombstonePath, tombstone1.GetFilename()), bytes.NewReader([]byte("invalid!}"))))

	w := NewUpdater(bkt, userID, nil, blockDeletionDelay, blocksCleanupInterval, newMockBucketStoreCfg(), logger)
	idx, partials, err := w.UpdateIndex(ctx, nil)
	require.NoError(t, err)
	assertBucketIndexEqual(t, idx, bkt, userID,
		[]tsdb.BlockMeta{block1},
		[]*metadata.DeletionMark{},
		[]*cortex_tsdb.Tombstone{tombstone2})
	assert.Empty(t, partials)
}

func TestUpdater_UpdateIndex_ShouldNotUploadProcessedTombstonesPassedFilteringPeriod(t *testing.T) {
	const userID = "user-1"

	bkt, _ := testutil.PrepareFilesystemBucket(t)

	ctx := context.Background()
	logger := log.NewNopLogger()

	// The bucket index stores all the tombstones that need to be used for query filtering
	// All pending state tombstones should be uploaded to the index
	// Processed state tombstones should be uploaded if the following time period hasn't passed since they updated to processed
	// -compactor.deletion-delay + -compactor.cleanup-interval + -blocks-storage.bucket-store.sync-interval

	bktStoreCfg := cortex_tsdb.BucketStoreConfig{
		SyncInterval: time.Minute,
	}

	blockDeletionDelay := time.Minute * 2
	blocksCleanupInterval := time.Minute * 3

	// Mock some blocks and tombstones in the storage.
	bkt = BucketWithGlobalMarkers(bkt)
	block1 := testutil.MockStorageBlock(t, bkt, userID, 10, 20)
	tombstone1 := testutil.MockTombstone(t, bkt, userID, 0, 0, 0, 0, []string{"series"}, "request1", cortex_tsdb.StatePending)
	// request2 should be uploaded because it is in pending state
	tombstone2 := testutil.MockTombstone(t, bkt, userID, 0, time.Now().Add(-time.Minute*7).Unix()*1000, 0, 0, []string{"series"}, "request2", cortex_tsdb.StatePending)
	// request3 should not be uploaded to the idx since the time period for filtering has passed
	testutil.MockTombstone(t, bkt, userID, 0, time.Now().Add(-time.Minute*7).Unix()*1000, 0, 0, []string{"series"}, "request3", cortex_tsdb.StateProcessed)
	tombstone4 := testutil.MockTombstone(t, bkt, userID, 0, time.Now().Add(-time.Minute*5).Unix()*1000, 0, 0, []string{"series"}, "request4", cortex_tsdb.StateProcessed)
	// request5 should not be uploaded since cancelled tombstones are not required for filtering
	testutil.MockTombstone(t, bkt, userID, 0, 0, 0, 0, []string{"series"}, "request5", cortex_tsdb.StateCancelled)

	w := NewUpdater(bkt, userID, nil, blockDeletionDelay, blocksCleanupInterval, bktStoreCfg, logger)
	idx, partials, err := w.UpdateIndex(ctx, nil)
	require.NoError(t, err)
	assertBucketIndexEqual(t, idx, bkt, userID,
		[]tsdb.BlockMeta{block1},
		[]*metadata.DeletionMark{},
		[]*cortex_tsdb.Tombstone{tombstone1, tombstone2, tombstone4})
	assert.Empty(t, partials)
}

func TestUpdater_UpdateIndex_ShouldNotUploadDuplicateTombstones(t *testing.T) {
	const userID = "user-1"

	bkt, _ := testutil.PrepareFilesystemBucket(t)

	ctx := context.Background()
	logger := log.NewNopLogger()

	// There could be a scenario where a tombstone is updated to a new state
	// but the file with the old state is not deleted. The bucket index should only
	// keep the file with the most up to date state

	// In order the states are: pending, processed, cancelled
	// cancelled state can only happen during pending but should take priority over all other states

	bktStoreCfg := cortex_tsdb.BucketStoreConfig{
		SyncInterval: time.Minute,
	}

	blockDeletionDelay := time.Minute * 2
	blocksCleanupInterval := time.Minute * 3

	// Mock some blocks and tombstones in the storage.
	bkt = BucketWithGlobalMarkers(bkt)
	block1 := testutil.MockStorageBlock(t, bkt, userID, 10, 20)
	tombstone1 := testutil.MockTombstone(t, bkt, userID, 0, 0, 0, 0, []string{"series"}, "request1", cortex_tsdb.StatePending)
	tombstone2 := testutil.MockTombstone(t, bkt, userID, 0, time.Now().Add(-time.Minute*7).Unix()*1000, 0, 0, []string{"series"}, "request2", cortex_tsdb.StatePending)
	tombstone3 := testutil.MockTombstone(t, bkt, userID, 0, 0, 0, 0, []string{"series"}, "request3", cortex_tsdb.StatePending)
	tombstone4 := testutil.MockTombstone(t, bkt, userID, 0, 0, 0, 0, []string{"series"}, "request4", cortex_tsdb.StatePending)

	w := NewUpdater(bkt, userID, nil, blockDeletionDelay, blocksCleanupInterval, bktStoreCfg, logger)
	idx, partials, err := w.UpdateIndex(ctx, nil)
	require.NoError(t, err)
	assertBucketIndexEqual(t, idx, bkt, userID,
		[]tsdb.BlockMeta{block1},
		[]*metadata.DeletionMark{},
		[]*cortex_tsdb.Tombstone{tombstone1, tombstone2, tombstone3, tombstone4})
	assert.Empty(t, partials)

	tombstone1 = testutil.MockTombstone(t, bkt, userID, 0, time.Now().Add(-time.Minute*5).Unix()*1000, 0, 0, []string{"series"}, "request1", cortex_tsdb.StateProcessed)
	// request3 should no longer be included in the bucket index since the filtering time for processed states is over
	testutil.MockTombstone(t, bkt, userID, 0, time.Now().Add(-time.Minute*7).Unix()*1000, 0, 0, []string{"series"}, "request3", cortex_tsdb.StateProcessed)
	// request4 should not be uploaded since cancelled tombstones are not included in bucket index
	testutil.MockTombstone(t, bkt, userID, 0, 0, 0, 0, []string{"series"}, "request4", cortex_tsdb.StateCancelled)

	idx, partials, err = w.UpdateIndex(ctx, idx)
	require.NoError(t, err)
	assertBucketIndexEqual(t, idx, bkt, userID,
		[]tsdb.BlockMeta{block1},
		[]*metadata.DeletionMark{},
		[]*cortex_tsdb.Tombstone{tombstone1, tombstone2})
	assert.Empty(t, partials)
}

func TestUpdater_UpdateIndex_NoTenantInTheBucket(t *testing.T) {
	const userID = "user-1"

	ctx := context.Background()
	bkt, _ := testutil.PrepareFilesystemBucket(t)

	for _, oldIdx := range []*Index{nil, {}} {
		w := NewUpdater(bkt, userID, nil, blockDeletionDelay, blocksCleanupInterval, newMockBucketStoreCfg(), log.NewNopLogger())
		idx, partials, err := w.UpdateIndex(ctx, oldIdx)

		require.NoError(t, err)
		assert.Equal(t, IndexVersion1, idx.Version)
		assert.InDelta(t, time.Now().Unix(), idx.UpdatedAt, 2)
		assert.Len(t, idx.Blocks, 0)
		assert.Len(t, idx.BlockDeletionMarks, 0)
		assert.Empty(t, partials)
	}
}

func getBlockUploadedAt(t testing.TB, bkt objstore.Bucket, userID string, blockID ulid.ULID) int64 {
	metaFile := path.Join(userID, blockID.String(), block.MetaFilename)

	attrs, err := bkt.Attributes(context.Background(), metaFile)
	require.NoError(t, err)

	return attrs.LastModified.Unix()
}

func assertBucketIndexEqual(t testing.TB, idx *Index, bkt objstore.Bucket, userID string, expectedBlocks []tsdb.BlockMeta, expectedDeletionMarks []*metadata.DeletionMark, expectedTombstones SeriesDeletionTombstones) {
	assert.Equal(t, IndexVersion1, idx.Version)
	assert.InDelta(t, time.Now().Unix(), idx.UpdatedAt, 2)

	// Build the list of expected block index entries.
	var expectedBlockEntries []*Block
	for _, b := range expectedBlocks {
		expectedBlockEntries = append(expectedBlockEntries, &Block{
			ID:         b.ULID,
			MinTime:    b.MinTime,
			MaxTime:    b.MaxTime,
			UploadedAt: getBlockUploadedAt(t, bkt, userID, b.ULID),
		})
	}

	assert.ElementsMatch(t, expectedBlockEntries, idx.Blocks)

	// Build the list of expected block deletion mark index entries.
	var expectedMarkEntries []*BlockDeletionMark
	for _, m := range expectedDeletionMarks {
		expectedMarkEntries = append(expectedMarkEntries, &BlockDeletionMark{
			ID:           m.ID,
			DeletionTime: m.DeletionTime,
		})
	}

	assert.ElementsMatch(t, expectedMarkEntries, idx.BlockDeletionMarks)
	assert.ElementsMatch(t, expectedTombstones, idx.Tombstones)
}

func newMockBucketStoreCfg() cortex_tsdb.BucketStoreConfig {
	return cortex_tsdb.BucketStoreConfig{
		SyncInterval: time.Minute,
		BucketIndex: cortex_tsdb.BucketIndexConfig{
			MaxStalePeriod: time.Hour,
		},
	}
}
