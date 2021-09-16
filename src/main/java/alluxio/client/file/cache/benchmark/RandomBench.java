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

public class RandomBench {
    private static final long NUM_ENTRY = 10 * Constants.MB;
    private static final int WINDOW_SIZE = 1 * Constants.MB;
    private static final int NUM_UNIQUE_ENTRY = 2 * Constants.MB;
    private static final int SEED = 32713;
    private static final int BITS_PER_CLOCK = 4;
    private static final int BITS_PER_SIZE = 20;
    private static final int BITS_PER_SCOPE = 8;

    private static final ScopeInfo DEFAULT_SCOPE = new ScopeInfo("table1");

    public static void main(String[] args) {
        Dataset<Integer> dataset = new RandomIntegerDataset(
                NUM_ENTRY, WINDOW_SIZE, 0, NUM_UNIQUE_ENTRY, SEED);

        ScopedClockCuckooFilter<Integer> clockFilter = ScopedClockCuckooFilter.create(
                Funnels.integerFunnel(), NUM_UNIQUE_ENTRY, BITS_PER_CLOCK, BITS_PER_SIZE, BITS_PER_SCOPE);
        int ccfAgingIntv = WINDOW_SIZE >> BITS_PER_CLOCK;

        ShadowCache mCacheManager = new ShadowCache();
        CacheContext context = new CacheContext();
        int bfAgingIntv = WINDOW_SIZE >> 2; // Aging each T/4

        System.out.println(clockFilter.getSummary());
        int cnt = 0;
        long totalDuration = 0, bfTotalDuration = 0;
        long agingDuration = 0, bfAgingDuration = 0;
        long filterDuration = 0, bfFilterDuration = 0;
        int cfAgingCount = 0, bfAgingCount = 0;
        long stackTick = System.currentTimeMillis();
        System.out.println("#operation" + "\t" + "Real" + "\t" + "Real(bytes)" + "\t" +
                "MBF" + "\t" + "MBF(bytes)" + "\t" +
                "CCF" + "\t" + "CCF(bytes)");
        while (dataset.hasNext()) {
            cnt++;
            DatasetEntry<Integer> entry = dataset.next();

            long startFilterTick = System.currentTimeMillis();
            if (!clockFilter.mightContainAndResetClock(entry.getItem())) {
                clockFilter.put(entry.getItem(), entry.getSize(), entry.getScopeInfo());
            }
            filterDuration += System.currentTimeMillis() - startFilterTick;

            long startBFTick = System.currentTimeMillis();
            mCacheManager.put(new PageId("table1", entry.getItem()), new byte[entry.getSize()], context);
            bfFilterDuration += System.currentTimeMillis() - startBFTick;

            // Aging CCF
            if (cnt % ccfAgingIntv == 0) {
                cfAgingCount++;
                long startAgingTick = System.currentTimeMillis();
                clockFilter.aging();
                agingDuration += System.currentTimeMillis() - startAgingTick;
            }

            // Aging BF
            if (cnt % bfAgingIntv == 0) {
                bfAgingCount++;
                long startAgingTick = System.currentTimeMillis();
                mCacheManager.switchBloomFilter();
                bfAgingDuration += System.currentTimeMillis() - startAgingTick;
            }

            totalDuration = (System.currentTimeMillis() - stackTick);
            if (cnt % (NUM_ENTRY >> 10) == 0) {
                System.out.println(cnt + "\t" + dataset.getRealEntryNumber() + "\t" + dataset.getRealEntrySize() + "\t" +
                                mCacheManager.getShadowCachePages() + "\t" + mCacheManager.getShadowCacheBytes() + "\t" +
                        clockFilter.size() + "\t" +clockFilter.sizeInBytes());
            }
        }
        System.out.println(" \tPut/Get(ms)\tAging(ms)\tAgingCount\n" +
                "MBF\t" + bfFilterDuration + "\t" + bfAgingDuration + "\t" + bfAgingCount + "\n" +
                "CCF\t" + filterDuration + "\t" + agingDuration + "\t" + cfAgingCount);
    }
}
