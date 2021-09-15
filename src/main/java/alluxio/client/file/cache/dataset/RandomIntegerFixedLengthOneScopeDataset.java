package alluxio.client.file.cache.dataset;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class RandomIntegerFixedLengthOneScopeDataset implements Dataset<Integer> {
    private static final ScopeInfo DEFAULT_SCOPE = new ScopeInfo("db1.table1");
    private static final int BYTES_PER_ITEM = 1;

    private final long numEntry;
    private final int windowSize;
    private final int lowerBound;
    private final int upperBound;
    private final Random random;
    private final Lock lock;
    private final AtomicLong count;

    //    private final ConcurrentLinkedQueue<Integer> queue;
//    private final ConcurrentHashMap<Integer, Integer> map;
    private final Queue<Integer> queue;
    private final HashMap<Integer, Integer> map;
    private int realNumber;
    private int realSize;

    public RandomIntegerFixedLengthOneScopeDataset(long numEntry, int windowSize, int lowerBound, int upperBound) {
        this.numEntry = numEntry;
        this.windowSize = windowSize;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.random = ThreadLocalRandom.current();
        this.lock = new ReentrantLock();
        this.count = new AtomicLong(0);
        this.queue = new LinkedList<>();
        this.map = new HashMap<>();
//        this.queue = new ConcurrentLinkedQueue<>();
//        this.map = new ConcurrentHashMap<>();
        this.realNumber = 0;
        this.realSize = 0;
    }

    public RandomIntegerFixedLengthOneScopeDataset(long numEntry, int windowSize, int lowerBound, int upperBound, long seed) {
        this.numEntry = numEntry;
        this.windowSize = windowSize;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.random = new Random(seed);
        this.lock = new ReentrantLock();
        this.count = new AtomicLong(0);
        this.queue = new LinkedList<>();
        this.map = new HashMap<>();
        this.realNumber = 0;
        this.realSize = 0;
    }

    @Override
    public DatasetEntry<Integer> next() {
        count.incrementAndGet();
        lock.lock();
        int r = lowerBound + random.nextInt(upperBound);
        queue.offer(r);
        map.put(r, map.getOrDefault(r, 0) + 1);
        if (queue.size() > windowSize) {
            Integer staleItem = queue.poll();
            assert staleItem != null;
            Integer itemCount = map.get(staleItem);
            assert itemCount != null;
            if (itemCount <= 1) {
                map.remove(staleItem);
            } else {
                map.put(staleItem, itemCount - 1);
            }
        }
        realNumber = map.size();
        realSize = realNumber;
        lock.unlock();
        return new DatasetEntry<>(r
                , BYTES_PER_ITEM, DEFAULT_SCOPE);
    }

    @Override
    public boolean hasNext() {
        return count.get() < numEntry;
    }

    @Override
    public int getRealEntryNumber() {
        return realNumber;
    }

    @Override
    public int getRealEntryNumber(ScopeInfo scope) {
        assert DEFAULT_SCOPE.equals(scope);
        return getRealEntryNumber();
    }

    @Override
    public int getRealEntrySize() {
        return realSize;
    }

    @Override
    public int getRealEntrySize(ScopeInfo scope) {
        assert DEFAULT_SCOPE.equals(scope);
        return getRealEntrySize();
    }
}
