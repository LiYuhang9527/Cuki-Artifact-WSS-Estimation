package alluxio.client.file.cache.cuckoofilter;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.concurrent.ThreadLocalRandom;

import static org.testng.Assert.assertEquals;

public class SegmentedLockTest {
    private static final int NUM_BUCKETS = 1024;
    private static final int NUM_LOCKS = 128;

    private SegmentedLock locks;

    @BeforeTest
    public void init() {
        create(NUM_LOCKS, NUM_BUCKETS);
    }

    private void create(int numLocks, int numBuckets) {
        locks = new SegmentedLock(NUM_LOCKS, NUM_BUCKETS);
    }

    @Test
    public void testConstruct() {
        create(1023, 128);
        assertEquals(128, locks.getNumLocks());

        create(1023, 127);
        assertEquals(128, locks.getNumLocks());

        create(513, 65);
        assertEquals(128, locks.getNumLocks());
    }

    @Test(threadPoolSize = 4, invocationCount = 12)
    public void testConcurrency() {
        for (int i = 0; i < NUM_BUCKETS; i++) {
            int r1 = ThreadLocalRandom.current().nextInt(NUM_BUCKETS);
            int r2 = ThreadLocalRandom.current().nextInt(NUM_BUCKETS);
            locks.lockTwoWrite(r1, r2);
            locks.unlockTwoWrite(r1, r2);
        }
    }
}