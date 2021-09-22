package alluxio.client.file.cache.benchmark.concurrency;

import alluxio.client.file.cache.Constants;
import alluxio.client.file.cache.dataset.Dataset;
import alluxio.client.file.cache.dataset.GeneralDataset;
import alluxio.client.file.cache.dataset.generator.RandomIntegerEntryGenerator;
import alluxio.client.file.cache.filter.ScopedClockCuckooFilter;
import com.google.common.hash.Funnels;

import java.util.ArrayList;

public class ConcurrentBench<T> extends Thread {
    private static long NUM_ENTRIES = 10 * Constants.MB;
    private static int WINDOW_SIZE = 64 * Constants.KB;
    private static final int NUM_UNIQUE_ENTRY = 10 * Constants.MB;
    private static final int BITS_PER_CLOCK = 8;
    private static final int BITS_PER_SIZE = 20;
    private static final int BITS_PER_SCOPE = 8;

    public static void main(String[] args) {
        // parse arguments
        long totalOps = 10 * Constants.MB;
        int numThreads = 4;
        long opsPerThread = totalOps / numThreads;
        ScopedClockCuckooFilter<Integer> clockFilter = ScopedClockCuckooFilter.create(
                Funnels.integerFunnel(), NUM_UNIQUE_ENTRY, BITS_PER_CLOCK, BITS_PER_SIZE, BITS_PER_SCOPE);
        System.out.println(clockFilter.getSummary());
        Dataset<Integer> dataset = new GeneralDataset<>(
                new RandomIntegerEntryGenerator(NUM_ENTRIES,
                        0, NUM_UNIQUE_ENTRY, 0, (1<<BITS_PER_SIZE)-1,  1, 32173),
                WINDOW_SIZE);
        ClientState state = new ClientState();
        state.numThreads.set(numThreads);
        ArrayList<ClientThread<Integer>> workers = new ArrayList<>();
        for (int i=0; i < numThreads; i++) {
            workers.add(new ClientThread<>(i, clockFilter, dataset, opsPerThread, state));
        }
        long st = System.currentTimeMillis();
        for (int i=0; i < numThreads; i++) {
            workers.get(i).start();
        }

        for (int i=0; i < numThreads; i++) {
            try {
                workers.get(i).join();
            } catch (Exception e) {
                // ignored
            }
        }
        long en = System.currentTimeMillis();
        long totalDuration = en - st;
        System.out.println("Cost " + totalDuration + " ms");
        System.out.println(state);
    }
}
