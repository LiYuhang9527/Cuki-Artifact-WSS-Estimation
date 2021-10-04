package alluxio.client.file.cache.filter;

import com.google.common.hash.Funnels;
import org.junit.Test;

import static org.junit.Assert.*;

public class ScopedClockCuckooFilterTest {
    private static final int MAGIC_NUMBER = 0x50524920;
    private static final ScopeInfo DEFAULT_SCOPE = new ScopeInfo("table1");

    @Test
    public void testBasicAging() {
        ScopedClockCuckooFilter<Integer> clockFilter = ScopedClockCuckooFilter.create(
                Funnels.integerFunnel(), 100, 4, 20, 8);
        for (int item=1; item <= 64; item++) {
            assertTrue(clockFilter.put(item, 1, DEFAULT_SCOPE));
        }
        int maxAge = (1 << 4)-1;
        for (int  i=0; i <= maxAge; i++) {
            for (int item=1; item <= 64; item++) {
                assertTrue(clockFilter.mightContain(item));
                assertEquals(maxAge - i, clockFilter.getAge(item));
                assertEquals(64, clockFilter.size());
            }
            clockFilter.aging();
        }
        // item will nor survive over maxAge iterations
        for (int item=1; item <= 64; item++) {
            assertFalse(clockFilter.mightContain(item));
        }
    }

    @Test
    public void testAging() {
        ScopedClockCuckooFilter<Integer> clockFilter = ScopedClockCuckooFilter.create(
                Funnels.integerFunnel(), 100, 4, 20, 8);
        for (int i=0; i < 64; i++) {
            int item = i ^ MAGIC_NUMBER;
            clockFilter.put(item, 2, DEFAULT_SCOPE);
            assertTrue(clockFilter.mightContainAndResetClock(item));
            clockFilter.aging();
            assertTrue(clockFilter.mightContain(item));
            assertEquals(clockFilter.size(), clockFilter.size(DEFAULT_SCOPE));
            if (i > 16) {
                assertTrue(clockFilter.size(DEFAULT_SCOPE) <= 16);
                assertFalse(clockFilter.mightContain((i-16)^MAGIC_NUMBER));
            }
        }
    }

    @Test
    public void testMinorAging() {
        ScopedClockCuckooFilter<Integer> clockFilter = ScopedClockCuckooFilter.create(
                Funnels.integerFunnel(), 1000, 4, 20, 8);
        for (int i=0; i < 64 * 4; i++) {
            int item = i ^ MAGIC_NUMBER;
            clockFilter.put(item, 2, DEFAULT_SCOPE);
            assertTrue(clockFilter.mightContainAndResetClock(item));
            clockFilter.minorAging(4);
            assertTrue(clockFilter.mightContain(item));
            assertEquals(clockFilter.size(), clockFilter.size(DEFAULT_SCOPE));
            if (i > 16*4) {
                assertTrue(clockFilter.size() <= 16*4);
                assertTrue(clockFilter.size(DEFAULT_SCOPE) <= 16*4);
                assertFalse(clockFilter.mightContain((i-16*4)^MAGIC_NUMBER));
            }
        }
    }

}