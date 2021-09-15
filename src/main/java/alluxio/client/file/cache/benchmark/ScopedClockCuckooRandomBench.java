package alluxio.client.file.cache.benchmark;

import alluxio.client.file.cache.Constants;
import alluxio.client.file.cache.dataset.*;
import alluxio.client.file.cache.filter.ClockCuckooFilter;
import alluxio.client.file.cache.filter.ScopedClockCuckooFilter;
import com.google.common.hash.Funnels;

public class ScopedClockCuckooRandomBench {
    private static final long NUM_ENTRY = 10 * Constants.MB;
    private static final int WINDOW_SIZE = 1 * Constants.MB;
    private static final int NUM_UNIQUE_ENTRY = 2 * Constants.MB;
    private static final int SEED = 32713;
    private static final int BITS_PER_CLOCK = 8;
    private static final int BITS_PER_SIZE = 20;
    private static final int BITS_PER_SCOPE = 8;

    private static final ScopeInfo DEFAULT_SCOPE = new ScopeInfo("table1");

    public static void main(String[] args) {
        Dataset<Integer> dataset = new RandomIntegerDataset(
                NUM_ENTRY, WINDOW_SIZE, 0, NUM_UNIQUE_ENTRY, SEED);
        ScopedClockCuckooFilter<Integer> clockFilter = ScopedClockCuckooFilter.create(
                Funnels.integerFunnel(), NUM_UNIQUE_ENTRY, BITS_PER_CLOCK, BITS_PER_SIZE, BITS_PER_SCOPE);
        int agingIntv = WINDOW_SIZE >> BITS_PER_CLOCK;
        System.out.println(clockFilter.getSummary());
        int cnt = 0;
        long totalDuration = 0;
        long agingDuration = 0;
        long filterDuration = 0;
        long stackTick = System.currentTimeMillis();
        while (dataset.hasNext()) {
            cnt++;
            DatasetEntry<Integer> entry = dataset.next();
            long startFilterTick = System.currentTimeMillis();
            if (!clockFilter.mightContainAndResetClock(entry.getItem())) {
                clockFilter.put(entry.getItem(), entry.getSize(), entry.getScopeInfo());
            }
            filterDuration += System.currentTimeMillis() - startFilterTick;
            if (cnt % agingIntv == 0) {
                long startAgingTick = System.currentTimeMillis();
                clockFilter.aging();
                agingDuration += System.currentTimeMillis() - startAgingTick;
            }
            totalDuration = (System.currentTimeMillis() - stackTick);
            if (cnt % (NUM_ENTRY >> 8) == 0) {
                System.out.println(cnt + "\t" + dataset.getRealEntrySize() + "\t" + clockFilter.sizeInBytes() +
                        "\t" + totalDuration + "\t" + filterDuration + "\t" + agingDuration);
            }
        }
    }
}
