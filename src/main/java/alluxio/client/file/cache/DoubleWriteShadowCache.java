package alluxio.client.file.cache;

import com.codahale.metrics.Counter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class DoubleWriteShadowCache extends ShadowCache {
    private final int mNumBloomFilter;
    private final long mBloomFilterExpectedInsertions;
    // An array of bloom filters, and each capture a segment of window
    private final AtomicReferenceArray<BloomFilter<PageId>> mSegmentBloomFilters;
    private final AtomicIntegerArray mObjEachBloomFilter;
    private final AtomicLongArray mByteEachBloomFilter;
    private final AtomicLong mShadowCachePageRead = new AtomicLong(0);
    private final AtomicLong mShadowCachePageHit = new AtomicLong(0);
    private final ScheduledExecutorService mScheduler = Executors.newScheduledThreadPool(0);
    private final AtomicLong mShadowCacheByteRead = new AtomicLong(0);
    private long mShadowCacheBytes = 0;
    private final AtomicLong mShadowCacheByteHit = new AtomicLong(0);
    private int mCurrentSegmentFilterIndex = 0;
    // capture the entire working set
    private BloomFilter<PageId> mWorkingSetBloomFilter;
    private long mShadowCachePages = 0;
    private double mAvgPageSize;

    public DoubleWriteShadowCache() {
//        long windowMs = conf.getMs(PropertyKey.USER_CLIENT_CACHE_SHADOW_WINDOW);
        mNumBloomFilter = 4;
        // include the 1 extra working set bloom filter
        long perBloomFilterMemoryOverhead =
                125*1024*1024 / (mNumBloomFilter + 1);
        // assume 3% Guava default false positive ratio
        mBloomFilterExpectedInsertions =
                (long) ((-perBloomFilterMemoryOverhead * Math.log(2) * Math.log(2)) / Math.log(0.03));
        mObjEachBloomFilter = new AtomicIntegerArray(new int[mNumBloomFilter]);
        mByteEachBloomFilter = new AtomicLongArray(new long[mNumBloomFilter]);
        mSegmentBloomFilters =
                new AtomicReferenceArray<BloomFilter<PageId>>(new BloomFilter[mNumBloomFilter]);
        for (int i = 0; i < mSegmentBloomFilters.length(); ++i) {
            mSegmentBloomFilters.set(i,
                    BloomFilter.create(PageIdFunnel.FUNNEL, mBloomFilterExpectedInsertions));
        }
        mWorkingSetBloomFilter =
                BloomFilter.create(PageIdFunnel.FUNNEL, mBloomFilterExpectedInsertions);
//        mScheduler.scheduleAtFixedRate(this::switchBloomFilter, 0, windowMs / mNumBloomFilter,
//                MILLISECONDS);
    }

    @VisibleForTesting
    public void updateWorkingSetSize() {
        updateAvgPageSize();
        long oldPages = Metrics.SHADOW_CACHE_PAGES.getCount();
        mShadowCachePages = (int) mWorkingSetBloomFilter.approximateElementCount();
        Metrics.SHADOW_CACHE_PAGES.inc(mShadowCachePages - oldPages);
        long oldBytes = Metrics.SHADOW_CACHE_BYTES.getCount();
        mShadowCacheBytes = (long) (mShadowCachePages * mAvgPageSize);
        Metrics.SHADOW_CACHE_BYTES.inc(mShadowCacheBytes - oldBytes);
    }

    public boolean put(PageId pageId, byte[] page, CacheContext cacheContext) {
        updateBloomFilterAndWorkingSet(pageId, page.length, cacheContext);
//        return mCacheManager.put(pageId, page, cacheContext);
        return true;
    }

    private void updateBloomFilterAndWorkingSet(PageId pageId, int pageLength,
                                                CacheContext cacheContext) {
        int filterIndex = mCurrentSegmentFilterIndex;
        BloomFilter<PageId> bf = mSegmentBloomFilters.get(filterIndex);
        if (!bf.mightContain(pageId)) {
            bf.put(pageId);
            mObjEachBloomFilter.getAndIncrement(filterIndex);
            mByteEachBloomFilter.getAndAdd(filterIndex, pageLength);
            mWorkingSetBloomFilter.put(pageId);
            updateFalsePositiveRatio();
            updateWorkingSetSize();
//            if (cacheContext != null) {
//                cacheContext
//                        .incrementCounter(MetricKey.CLIENT_CACHE_SHADOW_CACHE_BYTES.getName(), pageLength);
//            }
        }
        // also update sencondary bf
        int filterIndex2 = (filterIndex+mNumBloomFilter/2) % mNumBloomFilter;
        BloomFilter<PageId> bf2 = mSegmentBloomFilters.get(filterIndex2);
        if (!bf2.mightContain(pageId)) {
            bf2.put(pageId);
            mObjEachBloomFilter.getAndIncrement(filterIndex2);
            mByteEachBloomFilter.getAndAdd(filterIndex2, pageLength);
        }
    }

    private void updateFalsePositiveRatio() {
        int falsePositiveRatio = (int) mWorkingSetBloomFilter.expectedFpp() * 100;
        long oldFalsePositiveRatio = Metrics.SHADOW_CACHE_FALSE_POSITIVE_RATIO.getCount();
        Metrics.SHADOW_CACHE_FALSE_POSITIVE_RATIO.inc(falsePositiveRatio - oldFalsePositiveRatio);
    }

    /**
     * Update the avg page size statistics.
     */
    private void updateAvgPageSize() {
        int nInsert = 0;
        long nByte = 0;
        for (int i = 0; i < mSegmentBloomFilters.length(); ++i) {
            nInsert += mObjEachBloomFilter.get(i);
            nByte += mByteEachBloomFilter.get(i);
        }
        if (nInsert == 0) {
            mAvgPageSize = 0;
        } else {
            mAvgPageSize = nByte / (double) nInsert;
        }
    }

    /**
     * Replace the oldest bloom filter with a new one.
     */
    public void switchBloomFilter() {
        // put here because if when put it in other function, there is a risk that mObj and mGet are
        // read inconsistently
        updateAvgPageSize();
        mCurrentSegmentFilterIndex = (mCurrentSegmentFilterIndex + 1) % mNumBloomFilter;
        mSegmentBloomFilters.set(mCurrentSegmentFilterIndex,
                BloomFilter.create(PageIdFunnel.FUNNEL, mBloomFilterExpectedInsertions));
        // init new bf as its partner's intersect
//        int parterIndex = (mCurrentSegmentFilterIndex + mNumBloomFilter/2) % mNumBloomFilter;
//        mSegmentBloomFilters.get(parterIndex).or();
        mObjEachBloomFilter.set(mCurrentSegmentFilterIndex, 0);
        mByteEachBloomFilter.set(mCurrentSegmentFilterIndex, 0);
        mWorkingSetBloomFilter =
                BloomFilter.create(PageIdFunnel.FUNNEL, mBloomFilterExpectedInsertions);
        for (int i = 0; i < mSegmentBloomFilters.length(); ++i) {
            mWorkingSetBloomFilter.putAll(mSegmentBloomFilters.get(i));
        }
    }

    /**
     * @return ShadowCachePages
     */
    public long getShadowCachePages() {
        return mShadowCachePages;
    }

    /**
     * @return ShadowCacheBytes
     */
    public long getShadowCacheBytes() {
        return mShadowCacheBytes;
    }

    /**
     * @return ShadowCacheBytes
     */
    public long getShadowCachePageRead() {
        return mShadowCachePageRead.get();
    }

    /**
     * @return ShadowCacheBytes
     */
    public long getShadowCachePageHit() {
        return mShadowCachePageHit.get();
    }

    /**
     * @return ShadowCacheBytes
     */
    public long getShadowCacheByteRead() {
        return mShadowCacheByteRead.get();
    }

    /**
     * @return ShadowCacheBytes
     */
    public long getShadowCacheByteHit() {
        return mShadowCacheByteHit.get();
    }

    public int get(PageId pageId, int pageOffset, int bytesToRead, byte[] buffer,
                   int offsetInBuffer, CacheContext cacheContext) {
        boolean seen = false;
        for (int i = 0; i < mSegmentBloomFilters.length(); ++i) {
            seen |= mSegmentBloomFilters.get(i).mightContain(pageId);
        }
        if (seen) {
            Metrics.SHADOW_CACHE_PAGES_HIT.inc();
            Metrics.SHADOW_CACHE_BYTES_HIT.inc(bytesToRead);
            mShadowCachePageHit.getAndIncrement();
            mShadowCacheByteHit.getAndAdd(bytesToRead);
        } else {
            updateBloomFilterAndWorkingSet(pageId, bytesToRead, cacheContext);
        }
        Metrics.SHADOW_CACHE_PAGES_READ.inc();
        Metrics.SHADOW_CACHE_BYTES_READ.inc(bytesToRead);
        mShadowCachePageRead.getAndIncrement();
        mShadowCacheByteRead.getAndAdd(bytesToRead);
//        return mCacheManager.get(pageId, pageOffset, bytesToRead, buffer, offsetInBuffer, cacheContext);
        return 0;
    }


    /**
     * Funnel for PageId.
     */
    public enum PageIdFunnel implements Funnel<PageId> {
        FUNNEL;

        /**
         * @param from source
         * @param into destination
         */
        public void funnel(PageId from, PrimitiveSink into) {
            into.putUnencodedChars(from.getFileId()).putLong(from.getPageIndex());
        }
    }

    private static final class Metrics {
        private static final Counter SHADOW_CACHE_BYTES_READ = new Counter();
        private static final Counter SHADOW_CACHE_BYTES_HIT = new Counter();
        private static final Counter SHADOW_CACHE_PAGES_READ = new Counter();
        private static final Counter SHADOW_CACHE_PAGES_HIT = new Counter();
        private static final Counter SHADOW_CACHE_PAGES = new Counter();
        private static final Counter SHADOW_CACHE_BYTES = new Counter();
        private static final Counter SHADOW_CACHE_FALSE_POSITIVE_RATIO = new Counter();
    }

}


