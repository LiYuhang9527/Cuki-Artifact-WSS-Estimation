package alluxio.client.file.cache.filter;

import alluxio.client.file.cache.dataset.ScopeInfo;
import com.google.common.hash.Funnels;
import org.junit.Test;

import static org.junit.Assert.*;

public class ScopedClockCuckooFilterTest {
    private static final int MAGIC_NUMBER = 0x50524920;
    private static final ScopeInfo DEFAULT_SCOPE = new ScopeInfo("table1");

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
                Funnels.integerFunnel(), 100, 4, 20, 8);
        for (int i=0; i < 64 * 4; i++) {
            int item = i ^ MAGIC_NUMBER;
            clockFilter.put(item, 2, DEFAULT_SCOPE);
            assertTrue(clockFilter.mightContainAndResetClock(item));
//            clockFilter.minorAging(4);
            if ((i+1)%4 == 0) {
                clockFilter.aging();
            }
            assertTrue(clockFilter.mightContain(item));
            assertEquals(clockFilter.size(), clockFilter.size(DEFAULT_SCOPE));
            if (i > 16*4) {
                System.out.println(clockFilter.size(DEFAULT_SCOPE));
                assertTrue(clockFilter.size(DEFAULT_SCOPE) <= 16*4);
                assertFalse(clockFilter.mightContain((i-16*4)^MAGIC_NUMBER));
            }
        }
    }

}