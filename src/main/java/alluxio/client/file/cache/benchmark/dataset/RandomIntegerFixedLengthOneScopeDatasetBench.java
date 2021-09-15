package alluxio.client.file.cache.benchmark.dataset;

import alluxio.client.file.cache.Constants;
import alluxio.client.file.cache.dataset.Dataset;
import alluxio.client.file.cache.dataset.RandomIntegerFixedLengthOneScopeDataset;

public class RandomIntegerFixedLengthOneScopeDatasetBench {
    private static final long NUM_ENTRY = 10 * Constants.MB;
    private static final int WINDOW_SIZE = 1 * Constants.MB;
    private static final int NUM_UNIQUE_ENTRY = 2 * Constants.MB;
    private static final int SEED = 32713;

    public static void main(String[] args) {
        Dataset<Integer> dataset = new RandomIntegerFixedLengthOneScopeDataset(
                NUM_ENTRY, WINDOW_SIZE, 0, NUM_UNIQUE_ENTRY, SEED);
        long totalDuration = 0;
        long stackTick = System.currentTimeMillis();
        while (dataset.hasNext()) {
            dataset.next();
        }
        totalDuration = (System.currentTimeMillis() - stackTick);
        System.out.println("Time(ms): " + totalDuration);
    }
}
