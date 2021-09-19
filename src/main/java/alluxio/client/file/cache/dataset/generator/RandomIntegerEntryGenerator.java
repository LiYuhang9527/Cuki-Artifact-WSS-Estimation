package alluxio.client.file.cache.dataset.generator;

import alluxio.client.file.cache.dataset.DatasetEntry;
import alluxio.client.file.cache.dataset.ScopeInfo;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class RandomIntegerEntryGenerator implements EntryGenerator<Integer> {
    private static final int DEFAULT_LOWER_BOUND = 0;
    private static final int DEFAULT_UPPER_BOUND = 1000;
    private static final int DEFAULT_LOWER_BOUND_SIZE = 0;
    private static final int DEFAULT_UPPER_BOUND_SIZE = 1024;
    private static final int DEFAULT_NUM_SCOPES = 64;
    private static final int DEFAULT_SEED = 32173;

    private final long numEntries;
    private final int lowerBound;
    private final int upperBound;
    private final int lowerBoundSize;
    private final int upperBoundSize;
    private final int numScopes;
    private final Random random;

    private final AtomicLong count;

    public RandomIntegerEntryGenerator(long numEntries) {
        this(numEntries, DEFAULT_LOWER_BOUND, DEFAULT_UPPER_BOUND,
                DEFAULT_LOWER_BOUND_SIZE, DEFAULT_UPPER_BOUND_SIZE,
                DEFAULT_NUM_SCOPES, DEFAULT_SEED);
    }

    public RandomIntegerEntryGenerator(long numEntries,
                                       int lowerBound, int upperBound,
                                       int lowerBoundSize, int upperBoundSize,
                                       int numScopes, int seed) {
        this.numEntries = numEntries;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.lowerBoundSize = lowerBoundSize;
        this.upperBoundSize = upperBoundSize;
        this.numScopes = numScopes;
        this.random = new Random(seed);
        this.count = new AtomicLong(0);
    }

    @Override
    public DatasetEntry<Integer> next() {
        int item = lowerBound + random.nextInt(upperBound - lowerBound);
        ScopeInfo scope = new ScopeInfo("table" + (item % numScopes));
        int size = lowerBoundSize + (item * 31213) % (upperBoundSize - lowerBoundSize);
        if (size < 0) {
            size = -size;
        }
        count.incrementAndGet();
        return new DatasetEntry<>(item, size, scope);
    }

    @Override
    public boolean hasNext() {
        return count.longValue() < numEntries;
    }
}
