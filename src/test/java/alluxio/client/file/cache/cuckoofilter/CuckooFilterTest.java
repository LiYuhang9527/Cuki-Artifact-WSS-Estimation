package alluxio.client.file.cache.cuckoofilter;

import com.google.common.hash.Funnels;
import com.google.common.hash.Hashing;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.*;

public class CuckooFilterTest {
    static final int numBuckets = 16;
    static final int tagsPerBucket = 4;
    static final int bitsPerTag = 8;

    CuckooTable createCuckooTable() {
        AbstractBitSet bits = new BuiltinBitSet(numBuckets * bitsPerTag * 4);
        return new SingleCuckooTable(bits, numBuckets, tagsPerBucket, bitsPerTag);
    }

    CuckooFilter<Integer> createCuckooFilter() {
        return new CuckooFilter<>(createCuckooTable(), Funnels.integerFunnel(), Hashing.murmur3_128());
    }

    @Test
    public void testBasic() {
        CuckooFilter<Integer> filter = createCuckooFilter();
        Random random = ThreadLocalRandom.current();
        for (int k=0; k < numBuckets; k++) {
            int r = random.nextInt();
            assertTrue(filter.put(r));
            assertTrue(filter.mightContain(r));
            assertTrue(filter.delete(r));
            assertFalse(filter.mightContain(r));
        }
    }

    @Test
    public void testFilterFull() {
        CuckooFilter<Integer> filter = createCuckooFilter();
        int r = ThreadLocalRandom.current().nextInt();
        for (int j=0; j < 2*tagsPerBucket; j++) {
            assertTrue(filter.put(r));
        }
        assertEquals(2*tagsPerBucket, filter.size());
        assertFalse(filter.put(r));
        assertEquals(2*tagsPerBucket, filter.size());
    }

    @Test
    public void testCreate() {
        CuckooFilter<Integer> filter = CuckooFilter.create(Funnels.integerFunnel(), 100);
        assertEquals(32, filter.numBuckets()); // 32*4 slots are enough to hold 100 items
        assertEquals(4, filter.tagsPerBucket());
        assertEquals(8, filter.bitsPerTag());
    }

}
