package alluxio.client.file.cache.filter;

import alluxio.client.file.cache.dataset.ScopeInfo;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.concurrent.ThreadLocalRandom;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class ScopeEncoderTest {
    private static final int BITS_PER_SCOPE = 8; // 256 scopes at most
    private static final int NUM_SCOPES = (1 << BITS_PER_SCOPE);
    private static final ScopeInfo SCOPE1 = new ScopeInfo("table1");

    private ScopeEncoder scopeEncoder;

    @BeforeTest
    public void init() {
        scopeEncoder = new ScopeEncoder(BITS_PER_SCOPE);
    }

    @Test
    public void testBasic() {
        int id = scopeEncoder.encode(SCOPE1);
        assertEquals(0, id);
        assertEquals(SCOPE1, scopeEncoder.decode(id));
    }

    @Test(threadPoolSize = 4, invocationCount = 12)
    public void testConcurrentEncodeDecode() {
        for (int i = 0; i < NUM_SCOPES * 16; i++) {
            int r = ThreadLocalRandom.current().nextInt(NUM_SCOPES);
            ScopeInfo scopeInfo = new ScopeInfo("table" + r);
            int id = scopeEncoder.encode(scopeInfo);
            assertEquals(scopeInfo, scopeEncoder.decode(id));
            assertEquals(id, scopeEncoder.encode(scopeInfo));
            assertTrue(0 <= id && id < NUM_SCOPES);
        }
    }
}