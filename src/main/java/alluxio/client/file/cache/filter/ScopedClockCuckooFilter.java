package alluxio.client.file.cache.filter;

import alluxio.client.file.cache.Constants;
import alluxio.client.file.cache.dataset.ScopeInfo;
import com.google.common.hash.Funnel;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.io.Serializable;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

public class ScopedClockCuckooFilter<T> implements Serializable {
    private static final double DEFAULT_FPP = 0.01;
    private static final double DEFAULT_LOAD_FACTOR = 0.955;
    private static final int MAX_CUCKOO_COUNT = 500;
    private static final int TAGS_PER_BUCKET = 4;

    private CuckooTable table;
    private final int numBuckets;
    private final int bitsPerTag;

    private CuckooTable clockTable;
    private final int bitsPerClock;

    private CuckooTable sizeTable;
    private final int bitsPerSize;

    private CuckooTable scopeTable;
    private final int bitsPerScope;

    private final Funnel<? super T> funnel;
    private final HashFunction hasher;

    private AtomicLong numItems;
    private AtomicLong totalBytes;
    private final HashMap<Integer, Integer> scopeToNumber;
    private final HashMap<Integer, Integer> scopeToSize;

    private int agingPointer = 0;

    public static <T> ScopedClockCuckooFilter<T> create(
            Funnel<? super T> funnel, long expectedInsertions,
            int bitsPerClock, int bitsPerSize, int bitsPerScope,
            double fpp, double loadFactor, HashFunction hasher) {
        // TODO: make expectedInsertions a power of 2
        int bitsPerTag = Utils.optimalBitsPerTag(fpp, loadFactor);
        long numBuckets = Utils.optimalBuckets(expectedInsertions, loadFactor, TAGS_PER_BUCKET);
        long numBits = numBuckets * TAGS_PER_BUCKET * bitsPerTag;
        // TODO: check numBits overflow (< INT_MAX)
        AbstractBitSet bits = new BuiltinBitSet((int) numBits);
        CuckooTable table = new SingleCuckooTable(bits, (int) numBuckets, TAGS_PER_BUCKET, bitsPerTag);

        AbstractBitSet clockBits = new BuiltinBitSet((int) (numBuckets * TAGS_PER_BUCKET * bitsPerClock));
        CuckooTable clockTable = new SingleCuckooTable(clockBits, (int) numBuckets, TAGS_PER_BUCKET, bitsPerClock);

        AbstractBitSet sizeBits = new BuiltinBitSet((int) (numBuckets * TAGS_PER_BUCKET * bitsPerSize));
        CuckooTable sizeTable = new SingleCuckooTable(sizeBits, (int) numBuckets, TAGS_PER_BUCKET, bitsPerSize);

        AbstractBitSet scopeBits = new BuiltinBitSet((int) (numBuckets * TAGS_PER_BUCKET * bitsPerScope));
        CuckooTable scopeTable = new SingleCuckooTable(scopeBits, (int) numBuckets, TAGS_PER_BUCKET, bitsPerScope);
        return new ScopedClockCuckooFilter<>(table, clockTable, sizeTable, scopeTable, funnel, hasher);
    }

    public static <T> ScopedClockCuckooFilter<T> create(
            Funnel<? super T> funnel, long expectedInsertions, int bitsPerClock, int bitsPerSize, int bitsPerScope, double fpp, double loadFactor) {
        return create(funnel, expectedInsertions, bitsPerClock, bitsPerSize, bitsPerScope ,fpp, loadFactor, Hashing.murmur3_128());
    }

    public static <T> ScopedClockCuckooFilter<T> create(
            Funnel<? super T> funnel, long expectedInsertions, int bitsPerClock, int bitsPerSize, int bitsPerScope, double fpp) {
        return create(funnel, expectedInsertions, bitsPerClock, bitsPerSize, bitsPerScope, fpp, DEFAULT_LOAD_FACTOR);
    }

    public static <T> ScopedClockCuckooFilter<T> create(
            Funnel<? super T> funnel, long expectedInsertions, int bitsPerClock, int bitsPerSize, int bitsPerScope) {
        assert funnel != null;
        assert expectedInsertions > 0;
        assert bitsPerClock > 0;
        assert bitsPerSize > 0;
        assert bitsPerScope > 0;
        return create(funnel, expectedInsertions, bitsPerClock, bitsPerSize, bitsPerScope, DEFAULT_FPP);
    }

    public ScopedClockCuckooFilter(CuckooTable table, CuckooTable clockTable, CuckooTable sizeTable, CuckooTable scopeTable,
                                   Funnel<? super T> funnel, HashFunction hasher) {
        this.table = table;
        this.numBuckets = table.numBuckets();
        this.bitsPerTag = table.bitsPerTag();
        this.clockTable = clockTable;
        this.bitsPerClock = clockTable.bitsPerTag();
        this.sizeTable = sizeTable;
        this.bitsPerSize = sizeTable.bitsPerTag();
        this.scopeTable = scopeTable;
        this.bitsPerScope = scopeTable.bitsPerTag();
        this.funnel = funnel;
        this.hasher = hasher;
        this.numItems = new AtomicLong(0L);
        this.totalBytes = new AtomicLong(0L);
        this.scopeToNumber = new HashMap<>();
        this.scopeToSize = new HashMap<>();
    }

    public boolean put(T item, int size, ScopeInfo scopeInfo) {
        IndexAndTag indexAndTag = generateIndexAndTag(item);
        int curIndex = indexAndTag.index;
        int curTag = indexAndTag.tag;
        int curTagClock = 0xffffffff;
        int curSize = size;
        int scope = encodeScope(scopeInfo);
        int curScope = scope;
        int oldTag = 0;
        int oldTagClock = 0;
        int oldTagSize = 0;
        int oldTagScope = 0;
        TagPosition pos = new TagPosition();
        int from = -1, to = -1;
        boolean success = false;
        // probe the first bucket
        success = table.insert(curIndex, curTag, pos);
        if (!success) {
            // switch to the second bucket
            curIndex = altIndex(curIndex, curTag);
        }
        for (int count = 0; !success && count < MAX_CUCKOO_COUNT; count++) {
            oldTag = table.insertOrKickoutOne(curIndex, curTag, pos);
            if (oldTag == 0) {
                success = true;
                break;
            }
            oldTagClock = clockTable.readTag(pos.getBucketIndex(), pos.getTagIndex());
            oldTagSize = sizeTable.readTag(pos.getBucketIndex(), pos.getTagIndex());
            oldTagScope = scopeTable.readTag(pos.getBucketIndex(), pos.getTagIndex());

//            to = pos.getBucketIndex();
//            if (from != -1 && from < agingPointer && to >= agingPointer) {
//                curTagClock = Math.min(curTagClock+1, 0xffff);
//            } else if (from != -1 && from >= agingPointer && to < agingPointer) {
//                curTagClock = Math.max(curTagClock-1, 0x0);
//            }
            clockTable.writeTag(pos.getBucketIndex(), pos.getTagIndex(), curTagClock);
            sizeTable.writeTag(pos.getBucketIndex(), pos.getTagIndex(), curSize);
            scopeTable.writeTag(pos.getBucketIndex(), pos.getTagIndex(), curScope);

//            from = pos.getBucketIndex();
            curTag = oldTag;
            curTagClock = oldTagClock;
            curSize = oldTagSize;
            curScope = oldTagScope;
            curIndex = altIndex(curIndex, curTag);
        }
        if (success) {
            clockTable.writeTag(pos.getBucketIndex(), pos.getTagIndex(), curTagClock);
            sizeTable.writeTag(pos.getBucketIndex(), pos.getTagIndex(), curSize);
            scopeTable.writeTag(pos.getBucketIndex(), pos.getTagIndex(), curScope);

            numItems.incrementAndGet();
            totalBytes.addAndGet(size);
            scopeToNumber.put(scope, scopeToNumber.getOrDefault(scope, 0) + 1);
            scopeToSize.put(scope, scopeToSize.getOrDefault(scope, 0) + size);
        }

        return success;
    }

    public boolean mightContainAndResetClock(T item) {
        return mightContainAndOptionalResetClock(item, true);
    }

    public boolean mightContain(T item) {
        return mightContainAndOptionalResetClock(item, false);
    }

    private boolean mightContainAndOptionalResetClock(T item, boolean shouldReset) {
        boolean found;
        IndexAndTag indexAndTag = generateIndexAndTag(item);
        int i1 = indexAndTag.index;
        int tag = indexAndTag.tag;
        int i2 = altIndex(i1, tag);
        TagPosition pos = new TagPosition();
        found = table.findTagInBuckets(i1, i2, tag, pos);
        if (found && shouldReset) {
            // set C to MAX
            clockTable.writeTag(pos.getBucketIndex(), pos.getTagIndex(), 0xffffffff);
        }
        return found;
    }

    public boolean delete(T item) {
        IndexAndTag indexAndTag = generateIndexAndTag(item);
        int i1 = indexAndTag.index;
        int tag = indexAndTag.tag;
        int i2 = altIndex(i1, tag);
        TagPosition pos = new TagPosition();
        if (table.deleteTagFromBucket(i1, tag, pos) || table.deleteTagFromBucket(i2, tag, pos)) {
            numItems.decrementAndGet();
            // Clear Clock
            clockTable.writeTag(pos.getBucketIndex(), pos.getTagIndex(), 0);
            return true;
        }
        return false;
    }

    public int aging() {
        int numCleaned = 0;
        for (int i=0; i < numBuckets; i++) {
            for (int j=0; j < TAGS_PER_BUCKET; j++) {
                int tag = table.readTag(i, j);
                if (tag == 0) {
                    continue;
                }
                int oldClock = clockTable.readTag(i, j);
                assert oldClock >= 0;
                if (oldClock > 0) {
                    clockTable.writeTag(i, j, oldClock-1);
                } else if (oldClock == 0) {
                    // evict stale item
                    numCleaned++;
                    table.writeTag(i, j, 0);
                    numItems.decrementAndGet();
                    int scope = scopeTable.readTag(i, j);
                    int size = sizeTable.readTag(i, j);
                    totalBytes.addAndGet(-size);
                    scopeToNumber.put(scope, scopeToNumber.getOrDefault(scope, 1) - 1);
                    scopeToSize.put(scope, scopeToSize.getOrDefault(scope, size) - size);
                } else {
                    // should not come to here
                    System.out.println("Error: aging()");
                }
            }
        }
        return numCleaned;
    }

    public int minorAging(int k) {
        int numCleaned = 0;
        int bucketsToAging = numBuckets/k;
        if (2*bucketsToAging + agingPointer > numBuckets) {
            bucketsToAging = numBuckets - agingPointer;
        }
        for (int p=0; p < bucketsToAging; p++) {
            int i = (agingPointer + p) % numBuckets;
            for (int j=0; j < TAGS_PER_BUCKET; j++) {
                int tag = table.readTag(i, j);
                if (tag == 0) {
                    continue;
                }
                int oldClock = clockTable.readTag(i, j);
                if (oldClock > 0) {
                    clockTable.writeTag(i, j, oldClock-1);
                } else if (oldClock == 0) {
                    // evict stale item
                    numCleaned++;
                    table.writeTag(i, j, 0);
                    numItems.decrementAndGet();
                    int scope = scopeTable.readTag(i, j);
                    int size = sizeTable.readTag(i, j);
                    totalBytes.addAndGet(-size);
                    scopeToNumber.put(scope, scopeToNumber.getOrDefault(scope, 1) - 1);
                    scopeToSize.put(scope, scopeToSize.getOrDefault(scope, size) - size);
                } else {
                    // should not come to here
                    System.out.println("Error: aging()");
                }
            }
        }
        agingPointer = (agingPointer + bucketsToAging) % numBuckets;
        return numCleaned;
    }

    public int getAge(T item) {
        boolean found;
        IndexAndTag indexAndTag = generateIndexAndTag(item);
        int i1 = indexAndTag.index;
        int tag = indexAndTag.tag;
        int i2 = altIndex(i1, tag);
        TagPosition pos = new TagPosition();
        found = table.findTagInBuckets(i1, i2, tag, pos);
        if (found) {
            // set C to MAX
            return clockTable.readTag(pos.getBucketIndex(), pos.getTagIndex());
        }
        return 0;
    }

    public String getSummary() {
        return "numBuckets: " + numBuckets() +
                "\ntagsPerBucket: " + tagsPerBucket() +
                "\nbitsPerTag: " + bitsPerTag() +
                "\nbitsPerClock: " + getBitsPerClock() +
                "\nbitsPerSize: " + bitsPerSize +
                "\nbitsPerScope: " + bitsPerScope +
                "\nSizeInMB: " + (numBuckets() * tagsPerBucket() * bitsPerTag() / 8.0 / Constants.MB +
                                numBuckets() * tagsPerBucket() * getBitsPerClock() / 8.0 / Constants.MB +
                                numBuckets() * tagsPerBucket() * bitsPerSize / 8.0 / Constants.MB +
                                numBuckets() * tagsPerBucket() * bitsPerScope / 8.0 / Constants.MB);
    }

    public int size() {
        return numItems.intValue();
    }

    public int size(ScopeInfo scopeInfo) {
        int scope = encodeScope(scopeInfo);
        return scopeToNumber.getOrDefault(scope, 0);
    }

    public int sizeInBytes() {
        return totalBytes.intValue();
    }

    public int sizeInBytes(ScopeInfo scopeInfo) {
        int scope = encodeScope(scopeInfo);
        return scopeToSize.getOrDefault(scope, 0);
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
    public int getBitsPerClock() {
        return clockTable.bitsPerTag();
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

    private int encodeScope(ScopeInfo scopeInfo) {
        int scope = scopeInfo.hashCode()%64;
        if (scope < 0) {
            scope = -scope;
        }
        return scope;
    }
}
