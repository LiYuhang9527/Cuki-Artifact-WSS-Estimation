package alluxio.client.file.cache.filter;

import java.util.concurrent.ThreadLocalRandom;

public class SingleCuckooTable implements CuckooTable {
    private AbstractBitSet bits;
    private int numBuckets;
    private final int tagsPerBucket;
    private final int bitsPerTag;

    public SingleCuckooTable(AbstractBitSet bitSet, int numBuckets, int tagsPerBucket, int bitsPerTag) {
        this.bits = bitSet;
        this.numBuckets = numBuckets;
        this.tagsPerBucket = tagsPerBucket;
        this.bitsPerTag = bitsPerTag;
        // TODO: check bits.size() and numBuckets*bitsPerTag
    }

    @Override
    public int readTag(int i, int j) {
        int tagStartIdx = getTagOffset(i, j);
        int tag = 0;
        // TODO(iluoeli): Optimize me, since per bit operation is inefficient
        for (int k = 0; k < bitsPerTag; k++) {
            // set corresponding bit in tag
            int b = 0;
            if (bits.get(tagStartIdx + k)) {
                b = 1;
                tag |= (b << k);
            }
        }
        return tag;
    }

    @Override
    public void writeTag(int i, int j, int t) {
        int tagStartIdx = getTagOffset(i, j);
        for (int k = 0; k < bitsPerTag; k++) {
            if ((t & (1L << k)) != 0) {
                bits.set(tagStartIdx + k);
            } else {
                bits.clear(tagStartIdx + k);
            }
        }
    }

    @Override
    public boolean findTagInBucket(int i, int tag) {
        for (int j = 0; j < tagsPerBucket; j++) {
            if (readTag(i, j) == tag) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean findTagInBuckets(int i1, int i2, int tag) {
        for (int j = 0; j < tagsPerBucket; j++) {
            if (readTag(i1, j) == tag || readTag(i2, j) == tag) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean deleteTagFromBucket(int i, int tag) {
        for (int j = 0; j < tagsPerBucket; j++) {
            if (readTag(i, j) == tag) {
                writeTag(i, j, 0);
                return true;
            }
        }
        return false;
    }

    @Override
    public int insertOrKickoutOne(int i, int tag) {
        for (int j = 0; j < tagsPerBucket; j++) {
            if (readTag(i, j) == 0) {
                writeTag(i, j, tag);
                return 0;
            }
        }
        int r = ThreadLocalRandom.current().nextInt(tagsPerBucket);
        int oldTag = readTag(i, r);
        writeTag(i, r, tag);
        return oldTag;
    }

    @Override
    public int numTagsPerBuckets() {
        return tagsPerBucket;
    }

    @Override
    public int numBuckets() {
        return this.numBuckets;
    }

    @Override
    public int bitsPerTag() {
        return bitsPerTag;
    }

    @Override
    public int sizeInBytes() {
        return this.bits.size() >> 3;
    }

    @Override
    public int sizeInTags() {
        return this.numBuckets * tagsPerBucket;
    }

    private int getTagOffset(int bucketIndex, int posInBucket) {
        return (bucketIndex * numBuckets * bitsPerTag) + (posInBucket * bitsPerTag);
    }
}
