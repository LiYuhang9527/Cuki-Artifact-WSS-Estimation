package alluxio.client.file.cache.filter;

import org.junit.Test;

import static org.junit.Assert.*;

public class UtilsTest {
    static final int numBuckets = 16;
    static final int tagsPerBucket = 4;
    static final int bitsPerTag = 8;

    @Test
    public void testGenerateIndexAndTag() {
        for (int i = 0; i < numBuckets; i++) {
            for (int j = 0; j < tagsPerBucket; j++) {
                IndexAndTag indexAndTag = Utils.generateIndexAndTag(i * numBuckets + j, numBuckets, bitsPerTag);
                assertTrue(0 <= indexAndTag.mBucket && indexAndTag.mBucket < numBuckets);
                assertTrue(0 < indexAndTag.mTag && indexAndTag.mTag <= ((1 << bitsPerTag) - 1));
                int altIndex = Utils.altIndex(indexAndTag.mBucket, indexAndTag.mTag, numBuckets);
                int altAltIndex = Utils.altIndex(altIndex, indexAndTag.mTag, numBuckets);
                assertEquals(indexAndTag.mBucket, altAltIndex);
            }
        }
    }
}