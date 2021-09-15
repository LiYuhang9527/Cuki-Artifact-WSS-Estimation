package alluxio.client.file.cache.benchmark;

import alluxio.client.file.cache.CacheContext;
import alluxio.client.file.cache.Constants;
import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.ShadowCache;
import alluxio.client.file.cache.dataset.Dataset;
import alluxio.client.file.cache.dataset.DatasetEntry;
import alluxio.client.file.cache.dataset.RandomIntegerDataset;
import alluxio.client.file.cache.dataset.ScopeInfo;
import alluxio.client.file.cache.filter.ScopedClockCuckooFilter;
import com.google.common.hash.Funnels;

public class BFRandomBench {
    private static final long NUM_ENTRY = 10 * Constants.MB;
    private static final int WINDOW_SIZE = 1 * Constants.MB;
    private static final int NUM_UNIQUE_ENTRY = 2 * Constants.MB;
    private static final int SEED = 32713;
    private static final int BITS_PER_CLOCK = 8;
    private static final int BITS_PER_SIZE = 20;
    private static final int BITS_PER_SCOPE = 8;
    private static final PageId PAGE_ID1 = new PageId("0L", 0L);

    private static final ScopeInfo DEFAULT_SCOPE = new ScopeInfo("table1");

    public static void main(String[] args) {
        Dataset<Integer> dataset = new RandomIntegerDataset(
                NUM_ENTRY, WINDOW_SIZE, 0, NUM_UNIQUE_ENTRY, SEED);
        ShadowCache mCacheManager = new ShadowCache();
        CacheContext context = new CacheContext();
        int agingIntv = WINDOW_SIZE >> 2;
        int cnt = 0;
        long totalDuration = 0;
        long agingDuration = 0;
        long filterDuration = 0;
        long stackTick = System.currentTimeMillis();
        while (dataset.hasNext()) {
            cnt++;
            DatasetEntry<Integer> entry = dataset.next();
            long startFilterTick = System.currentTimeMillis();
            mCacheManager.put(new PageId("table1", entry.getItem()), new byte[entry.getSize()], context);
            filterDuration += System.currentTimeMillis() - startFilterTick;
            if (cnt % agingIntv == 0) {
                long startAgingTick = System.currentTimeMillis();
                mCacheManager.switchBloomFilter();
                agingDuration += System.currentTimeMillis() - startAgingTick;
            }
            totalDuration = (System.currentTimeMillis() - stackTick);
            if (cnt % (NUM_ENTRY >> 8) == 0) {
                mCacheManager.updateWorkingSetSize();
                System.out.println(cnt + "\t" + dataset.getRealEntrySize() + "\t" + mCacheManager.getShadowCacheBytes() +
                        "\t" + totalDuration + "\t" + filterDuration + "\t" + agingDuration);
            }
        }
    }
}
