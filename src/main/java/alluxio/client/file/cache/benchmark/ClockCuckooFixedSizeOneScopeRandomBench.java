package alluxio.client.file.cache.benchmark;

import alluxio.client.file.cache.Constants;
import alluxio.client.file.cache.dataset.Dataset;
import alluxio.client.file.cache.dataset.DatasetEntry;
import alluxio.client.file.cache.dataset.RandomIntegerFixedLengthOneScopeDataset;
import alluxio.client.file.cache.filter.ClockCuckooFilter;
import com.google.common.hash.Funnels;

public class ClockCuckooFixedSizeOneScopeRandomBench {
    private static final long NUM_ENTRY = 10 * Constants.MB;
    private static final int WINDOW_SIZE = 1 * Constants.MB;
    private static final int NUM_UNIQUE_ENTRY = 2 * Constants.MB;
    private static final int SEED = 32713;
    private static final int BITS_PER_CLOCK = 8;

    public static void main(String[] args) {
        Dataset<Integer> dataset = new RandomIntegerFixedLengthOneScopeDataset(
                NUM_ENTRY, WINDOW_SIZE, 0, NUM_UNIQUE_ENTRY, SEED);
        ClockCuckooFilter<Integer> clockFilter = ClockCuckooFilter.create(
                Funnels.integerFunnel(), NUM_UNIQUE_ENTRY, BITS_PER_CLOCK);
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
                clockFilter.put(entry.getItem());
            }
            filterDuration += System.currentTimeMillis() - startFilterTick;
            if (cnt % agingIntv == 0) {
                long startAgingTick = System.currentTimeMillis();
                clockFilter.aging();
                agingDuration += System.currentTimeMillis() - startAgingTick;
            }
            totalDuration = (System.currentTimeMillis() - stackTick);
            if (cnt % (NUM_ENTRY >> 8) == 0) {
                System.out.println(cnt + "\t" + dataset.getRealEntryNumber() + "\t" + clockFilter.size() +
                        "\t" + totalDuration + "\t" + filterDuration + "\t" + agingDuration);
            }
        }
        System.out.println(cnt + "\t" + dataset.getRealEntryNumber() + "\t" + clockFilter.size() +
                "\t" + totalDuration + "\t" + filterDuration + "\t" + agingDuration);
    }
}
