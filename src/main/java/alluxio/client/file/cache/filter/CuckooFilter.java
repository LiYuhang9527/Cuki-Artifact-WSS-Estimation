package alluxio.client.file.cache.filter;

import com.google.common.hash.Funnel;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

public class CuckooFilter<T> implements Serializable {
    private static final double DEFAULT_FPP = 0.01;
    private static final double DEFAULT_LOAD_FACTOR = 0.955;
    private static final int MAX_CUCKOO_COUNT = 500;
    private static final int TAGS_PER_BUCKET = 4;

    private CuckooTable table;
    private int numBuckets;
    private int bitsPerTag;

    private final Funnel<? super T> funnel;
    private final HashFunction hasher;

    private AtomicLong numItems;

    public static <T> CuckooFilter<T> create(
            Funnel<? super T> funnel, long expectedInsertions, double fpp, double loadFactor, HashFunction hasher) {
        // TODO: make expectedInsertions a power of 2
        int bitsPerTag = Utils.optimalBitsPerTag(fpp, loadFactor);
        long numBuckets = Utils.optimalBuckets(expectedInsertions, loadFactor, TAGS_PER_BUCKET);
        long numBits = numBuckets * TAGS_PER_BUCKET * bitsPerTag;
        // TODO: check numBits overflow (< INT_MAX)
        AbstractBitSet bits = new BuiltinBitSet((int) numBits);
        CuckooTable table = new SingleCuckooTable(bits, (int) numBuckets, TAGS_PER_BUCKET, bitsPerTag);
        return new CuckooFilter<>(table, funnel, hasher);
    }

    public static <T> CuckooFilter<T> create(
            Funnel<? super T> funnel, long expectedInsertions, double fpp, double loadFactor) {
        return create(funnel, expectedInsertions, fpp, loadFactor, Hashing.murmur3_128());
    }

    public static <T> CuckooFilter<T> create(
            Funnel<? super T> funnel, long expectedInsertions, double fpp) {
        return create(funnel, expectedInsertions, fpp, DEFAULT_LOAD_FACTOR);
    }

    public static <T> CuckooFilter<T> create(
            Funnel<? super T> funnel, long expectedInsertions) {
        return create(funnel, expectedInsertions, DEFAULT_FPP);
    }

    public CuckooFilter(CuckooTable table, Funnel<? super T> funnel, HashFunction hasher) {
        this.table = table;
        this.numBuckets = table.numBuckets();
        this.bitsPerTag = table.bitsPerTag();
        this.funnel = funnel;
        this.hasher = hasher;
        numItems = new AtomicLong(0L);
    }

    public boolean put(T item) {
        IndexAndTag indexAndTag = generateIndexAndTag(item);
        int curIndex = indexAndTag.index;
        int curTag = indexAndTag.tag;
        int oldTag;
        for (int count = 0; count < MAX_CUCKOO_COUNT; count++) {
            boolean kickout = false;
            if (count > 0) {
                kickout = true;
            }
            oldTag = table.insertOrKickoutOne(curIndex, curTag);
            if (oldTag == 0) {
                numItems.incrementAndGet();
                return true;
            }
            if (kickout) {
                curTag = oldTag;
            }
            curIndex = altIndex(curIndex, curTag);
        }

        return false;
    }

    public boolean mightContain(T item) {
        boolean found = false;
        IndexAndTag indexAndTag = generateIndexAndTag(item);
        int i1 = indexAndTag.index;
        int tag = indexAndTag.tag;
        int i2 = altIndex(i1, tag);
        found = table.findTagInBuckets(i1, i2, tag);
        return found;
    }

    public boolean delete(T item) {
        IndexAndTag indexAndTag = generateIndexAndTag(item);
        int i1 = indexAndTag.index;
        int tag = indexAndTag.tag;
        int i2 = altIndex(i1, tag);
        if (table.deleteTagFromBucket(i1, tag) || table.deleteTagFromBucket(i2, tag)) {
            numItems.decrementAndGet();
            return true;
        }
        return false;
    }

    public int size() {
        return numItems.intValue();
    }

    public int numBuckets() {
        return table.numBuckets();
    }

    public int tagsPerBucket() {
        return table.numTagsPerBuckets();
    }

    public int bitsPerTag() {
        return table.bitsPerTag();
    }

    private int indexHash(int hv) {
        return Utils.indexHash(hv, numBuckets);
    }

    private int tagHash(int hv) {
        return Utils.tagHash(hv, bitsPerTag);
    }

    private int altIndex(int index, int tag) {
        return Utils.altIndex(index, tag, numBuckets);
    }

    private IndexAndTag generateIndexAndTag(T item) {
        HashCode hashCode = hasher.newHasher().putObject(item, funnel).hash();
        long hv = hashCode.asLong();
        return Utils.generateIndexAndTag(hv, numBuckets, bitsPerTag);
    }
}
