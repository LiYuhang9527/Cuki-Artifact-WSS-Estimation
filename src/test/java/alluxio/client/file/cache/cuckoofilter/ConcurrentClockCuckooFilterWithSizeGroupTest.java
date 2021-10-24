package alluxio.client.file.cache.cuckoofilter;

import static org.junit.Assert.assertEquals;

import alluxio.Constants;
import alluxio.client.quota.CacheScope;

import com.google.common.hash.Funnels;
import org.junit.Before;
import org.junit.Test;

public class ConcurrentClockCuckooFilterWithSizeGroupTest {
    private static final int EXPECTED_INSERTIONS = Constants.KB;
    private static final int BITS_PER_CLOCK = 4;
    private static final int MAX_AGE = (1 << BITS_PER_CLOCK) - 1;
    private static final int BITS_PER_SIZE = 20;
    private static final int BITS_PER_SCOPE = 8;
    private static final int SIZE_GROUP_BITS = 4;

    private static final CacheScope SCOPE1 = CacheScope.create("table1");
    private static final CacheScope SCOPE2 = CacheScope.create("table2");

    private ConcurrentClockCuckooFilterWithSizeGroup<Integer> mClockFilter;

    @Before
    public void beforeTest() {
        init();
    }

    private void init() {
        mClockFilter = ConcurrentClockCuckooFilterWithSizeGroup.create(
                Funnels.integerFunnel(), EXPECTED_INSERTIONS, BITS_PER_CLOCK, BITS_PER_SIZE, BITS_PER_SCOPE);
    }

    @Test
    public void testSameSizeGroup() {
        mClockFilter.put(1, 1, SCOPE1);
        mClockFilter.put(2, 3, SCOPE1);
        assertEquals(4, mClockFilter.getItemSize());
        mClockFilter.delete(1);
        assertEquals(2, mClockFilter.getItemSize());
    }

    @Test
    public void testDifferentSizeGroup() {
      int base = 1 << (BITS_PER_SIZE - SIZE_GROUP_BITS);
      mClockFilter.put(1, base - 1, SCOPE1);
      mClockFilter.put(2, base + 1, SCOPE1);
      assertEquals(base + base, mClockFilter.getItemSize());
      mClockFilter.delete(1);
      assertEquals(base + 1, mClockFilter.getItemSize());
    }
}