package alluxio.client.file.cache.filter;

import com.google.common.hash.Funnels;
import org.junit.Test;

import static org.junit.Assert.*;

public class ClockCuckooFilterTest {

    private static final int MAGIC_NUMBER = 0x50524920;

    @Test
    public void testResetClock() {
        ClockCuckooFilter<Integer> clockFilter = ClockCuckooFilter.create(
                Funnels.integerFunnel(), 100, 4);
        for (int i=0; i < 64; i++) {
            int item = i ^ MAGIC_NUMBER;
            clockFilter.put(item);
            assertTrue(clockFilter.mightContainAndResetClock(item));
            clockFilter.aging();
            assertTrue(clockFilter.mightContain(item));
            if (i > 16) {
                assertTrue(clockFilter.size() < 16);
                assertFalse(clockFilter.mightContain((i-16)^MAGIC_NUMBER));
            }
        }
    }

}