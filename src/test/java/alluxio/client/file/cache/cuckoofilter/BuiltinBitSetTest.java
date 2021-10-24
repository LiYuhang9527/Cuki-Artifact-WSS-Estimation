package alluxio.client.file.cache.cuckoofilter;

import org.junit.Test;

import static org.junit.Assert.*;

public class BuiltinBitSetTest {
    @Test
    public void basicTest() {
        AbstractBitSet bitSet = new BuiltinBitSet(100);
        bitSet.set(9);
        assertTrue(bitSet.get(9));
        assertFalse(bitSet.get(8));
    }
}