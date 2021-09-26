package alluxio.client.file.cache.filter;

import alluxio.client.file.cache.Constants;
import alluxio.client.file.cache.dataset.ScopeInfo;
import com.google.common.hash.Funnel;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A basic entry that records information of a path node during Cuckoo BFS search
 */
final class BFSEntry {
    public int bucket;
    public int pathcode; // encode slot position of ancestors and it own nodes
    public int depth;

    BFSEntry(int bucket, int pathcode, int depth) {
        this.bucket = bucket;
        this.pathcode = pathcode;
        this.depth = depth;
    }
}

final class CuckooRecord {
    public int bucket;
    public int slot;
    public int fingerprint;

    CuckooRecord() {
        this(-1, -1, 0);
    }

    CuckooRecord(int bucket, int slot, int fingerprint) {
        this.bucket = bucket;
        this.slot = slot;
        this.fingerprint = fingerprint;
    }
}

public class ConcurrentClockCuckooFilter<T> implements Serializable {
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

    private final ScopeEncoder scopeEncoder;

    private final SegmentedLock locks;

    private int agingPointer = 0;

    public static <T> ConcurrentClockCuckooFilter<T> create(
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
        return new ConcurrentClockCuckooFilter<>(table, clockTable, sizeTable, scopeTable, funnel, hasher);
    }

    public static <T> ConcurrentClockCuckooFilter<T> create(
            Funnel<? super T> funnel, long expectedInsertions, int bitsPerClock, int bitsPerSize, int bitsPerScope, double fpp, double loadFactor) {
        return create(funnel, expectedInsertions, bitsPerClock, bitsPerSize, bitsPerScope, fpp, loadFactor, Hashing.murmur3_128());
    }

    public static <T> ConcurrentClockCuckooFilter<T> create(
            Funnel<? super T> funnel, long expectedInsertions, int bitsPerClock, int bitsPerSize, int bitsPerScope, double fpp) {
        return create(funnel, expectedInsertions, bitsPerClock, bitsPerSize, bitsPerScope, fpp, DEFAULT_LOAD_FACTOR);
    }

    public static <T> ConcurrentClockCuckooFilter<T> create(
            Funnel<? super T> funnel, long expectedInsertions, int bitsPerClock, int bitsPerSize, int bitsPerScope) {
        assert funnel != null;
        assert expectedInsertions > 0;
        assert bitsPerClock > 0;
        assert bitsPerSize > 0;
        assert bitsPerScope > 0;
        return create(funnel, expectedInsertions, bitsPerClock, bitsPerSize, bitsPerScope, DEFAULT_FPP);
    }

    public ConcurrentClockCuckooFilter(CuckooTable table, CuckooTable clockTable, CuckooTable sizeTable, CuckooTable scopeTable,
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
        this.scopeEncoder = new ScopeEncoder(bitsPerScope);
        this.locks = new SegmentedLock(Math.min(4096, numBuckets >> 1), numBuckets);
    }

    public boolean put(T item, int size, ScopeInfo scopeInfo) {
        IndexAndTag indexAndTag = generateIndexAndTag(item);
        int fp = indexAndTag.tag;
        int b1 = indexAndTag.index;
        int b2 = altIndex(b1, fp);
        int scope = encodeScope(scopeInfo);
        TagPosition pos = new TagPosition();
        locks.lockTwoWrite(b1, b2);
        boolean done = cuckooInsertLoop(b1, b2, fp, pos);
        if (done && pos.status == CuckooStatus.OK) {
            // b1 and b2 should be insertable for fp, which means:
            // 1. b1 or b2 have at least one empty slot (this is guaranteed until we unlock two buckets);
            // 2. b1 and b2 do not contain duplicated fingerprint.
            assert (pos.getBucketIndex() >= 0 && pos.getTagIndex() >= 0);
            assert table.readTag(pos.getBucketIndex(), pos.getTagIndex()) == 0;
            assert !table.findTagInBuckets(b1, b2, fp);
            table.writeTag(pos.getBucketIndex(), pos.getTagIndex(), fp);
            clockTable.writeTag(pos.getBucketIndex(), pos.getTagIndex(), 0xffffffff);
            scopeTable.writeTag(pos.getBucketIndex(), pos.getTagIndex(), scope);
            sizeTable.writeTag(pos.getBucketIndex(), pos.getTagIndex(), size);
            // update statistics
            numItems.incrementAndGet();
            totalBytes.addAndGet(size);
            scopeToNumber.put(scope, scopeToNumber.getOrDefault(scope, 0) + 1);
            scopeToSize.put(scope, scopeToSize.getOrDefault(scope, 0) + size);
            locks.unlockTwoWrite(b1, b2);
            return true;
        }
        locks.unlockTwoWrite(b1, b2);
        return false;
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
        int b1 = indexAndTag.index;
        int tag = indexAndTag.tag;
        int b2 = altIndex(b1, tag);
        locks.lockTwoRead(b1, b2);
        TagPosition pos = new TagPosition();
        found = table.findTagInBuckets(b1, b2, tag, pos);
        if (found && shouldReset) {
            // set C to MAX
            clockTable.writeTag(pos.getBucketIndex(), pos.getTagIndex(), 0xffffffff);
        }
        locks.unlockTwoRead(b1, b2);
        return found;
    }

    public boolean delete(T item) {
        IndexAndTag indexAndTag = generateIndexAndTag(item);
        int i1 = indexAndTag.index;
        int tag = indexAndTag.tag;
        int i2 = altIndex(i1, tag);
        locks.lockTwoWrite(i1, i2);
        TagPosition pos = new TagPosition();
        if (table.deleteTagFromBucket(i1, tag, pos) || table.deleteTagFromBucket(i2, tag, pos)) {
            numItems.decrementAndGet();
            int scope = scopeTable.readTag(pos.getBucketIndex(), pos.getTagIndex());
            int size = sizeTable.readTag(pos.getBucketIndex(), pos.getTagIndex());
            scopeToNumber.put(scope, scopeToNumber.getOrDefault(scope, 0) - 1);
            scopeToSize.put(scope, scopeToSize.getOrDefault(scope, 0) - size);
            // Clear Clock
            clockTable.writeTag(pos.getBucketIndex(), pos.getTagIndex(), 0);
            locks.unlockTwoWrite(i1, i2);
            return true;
        }
        locks.unlockTwoWrite(i1, i2);
        return false;
    }

    public int aging() {
        int numCleaned = 0;
        for (int i = 0; i < numBuckets; i++) {
            for (int j = 0; j < TAGS_PER_BUCKET; j++) {
                int tag = table.readTag(i, j);
                if (tag == 0) {
                    continue;
                }
                int oldClock = clockTable.readTag(i, j);
                assert oldClock >= 0;
                if (oldClock > 0) {
                    clockTable.writeTag(i, j, oldClock - 1);
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
        int bucketsToAging = numBuckets / k;
        if (2 * bucketsToAging + agingPointer > numBuckets) {
            bucketsToAging = numBuckets - agingPointer;
        }
        for (int p = 0; p < bucketsToAging; p++) {
            int i = (agingPointer + p) % numBuckets;
            for (int j = 0; j < TAGS_PER_BUCKET; j++) {
                int tag = table.readTag(i, j);
                if (tag == 0) {
                    continue;
                }
                int oldClock = clockTable.readTag(i, j);
                if (oldClock > 0) {
                    clockTable.writeTag(i, j, oldClock - 1);
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
        locks.lockTwoRead(i1, i2);
        TagPosition pos = new TagPosition();
        found = table.findTagInBuckets(i1, i2, tag, pos);
        if (found) {
            int clock = clockTable.readTag(pos.getBucketIndex(), pos.getTagIndex());
            locks.unlockTwoRead(i1, i2);
            return clock;
        }
        locks.unlockTwoRead(i1, i2);
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
        return scopeEncoder.encode(scopeInfo);
    }

    /**
     * Assert already held the lock of buckets i1 and i2.
     */
    private boolean cuckooInsertLoop(int b1, int b2, int fp, TagPosition pos) {
        int maxRetryNum = 1;
        boolean done = false;
        while (maxRetryNum-- > 0) {
            if (cuckooInsert(b1, b2, fp, pos)) {
                done = true;
                pos.status = CuckooStatus.OK;
                break;
            }
        }
        return done;
    }

    /**
     * Assert already held the lock of buckets i1 and i2.
     */
    private boolean cuckooInsert(int b1, int b2, int fp, TagPosition pos) {
        TagPosition pos1 = new TagPosition(), pos2 = new TagPosition();
        // try find b1 and b2 firstly
        if (!tryFindInsertBucket(b1, fp, pos1)) {
            pos.setStatus(CuckooStatus.FAILURE_KEY_DUPLICATED);
            return false;
        }
        if (!tryFindInsertBucket(b2, fp, pos2)) {
            pos.setStatus(CuckooStatus.FAILURE_KEY_DUPLICATED);
            return false;
        }
        if (pos1.getTagIndex() != -1) {
            pos.setBucketAndSlot(b1, pos1.getTagIndex());
            pos.setStatus(CuckooStatus.OK);
            return true;
        }
        if (pos2.getTagIndex() != -1) {
            pos.setBucketAndSlot(b2, pos2.getTagIndex());
            pos.setStatus(CuckooStatus.OK);
            return true;
        }
        // then BFS search from b1 and b2
        boolean done = runCuckoo(b1, b2, fp, pos);
        if (done) {
            // avoid another duplicated key is inserted during runCuckoo.
            if (table.findTagInBuckets(b1, b2, fp)) {
//                locks.unlockTwoWrite(b1, b2);
                pos.setStatus(CuckooStatus.FAILURE_KEY_DUPLICATED);
                return false;
            } else {
                return true;
            }
        }
        return false;
    }

    /**
     * Assert already held the lock of buckets i1 and i2.
     * @return true iff find an empty position (stored in pos); false otherwise.
     */
    private boolean runCuckoo(int b1, int b2, int fp, TagPosition pos) {
        locks.unlockTwoWrite(b1, b2);
        int maxPathLen = Constants.MAX_BFS_PATH_LEN;
        CuckooRecord[] cuckooPath = new CuckooRecord[maxPathLen];
        for (int i = 0; i < maxPathLen; i++) {
            cuckooPath[i] = new CuckooRecord();
        }
        boolean done = false;
        while (!done) {
            int depth = cuckooPathSearch(b1, b2, fp, cuckooPath);
            if (depth < 0) {
                break;
            }
            if (cuckooPathMove(b1, b2, fp, cuckooPath, depth)) {
                pos.setBucketAndSlot(cuckooPath[0].bucket, cuckooPath[0].slot);
                pos.setStatus(CuckooStatus.OK);
                done = true;
            }
        }
        if (!done) {
            // NOTE: since we assume holding the locks of two buckets before calling this method,
            // we keep this assumptions after return.
            locks.lockTwoWrite(b1, b2);
        }
        return done;
    }

    private int cuckooPathSearch(int b1, int b2, int fp, CuckooRecord[] cuckooPath) {
        // 1. search a path
        BFSEntry x = slotBFSSearch(b1, b2, fp);
        if (x.depth == -1) {
            return -1;
        }
        // 2. re-construct path from x
        for (int i = x.depth; i >= 0; i--) {
            cuckooPath[i].slot = x.pathcode % TAGS_PER_BUCKET;
            x.pathcode /= TAGS_PER_BUCKET;
        }
        if (x.pathcode == 0) {
            cuckooPath[0].bucket = b1;
        } else {
            assert x.pathcode == 1;
            cuckooPath[0].bucket = b2;
        }
        {
            locks.lockOneWrite(cuckooPath[0].bucket);
            int tag = table.readTag(cuckooPath[0].bucket, cuckooPath[0].slot);
            if (tag == 0) {
                locks.unlockOneWrite(cuckooPath[0].bucket);
                return 0;
            }
            locks.unlockOneWrite(cuckooPath[0].bucket);
            cuckooPath[0].fingerprint = tag;
        }
        for (int i = 1; i <= x.depth; i++) {
            CuckooRecord curr = cuckooPath[i];
            CuckooRecord prev = cuckooPath[i-1];
            curr.bucket = altIndex(prev.bucket, prev.fingerprint);
            locks.lockOneWrite(curr.bucket);
            int tag = table.readTag(curr.bucket, curr.slot);
            if (tag == 0) {
                locks.unlockOneWrite(curr.bucket);
                return i;
            }
            curr.fingerprint = tag;
            locks.unlockOneWrite(curr.bucket);
        }
        return x.depth;
    }

    private BFSEntry slotBFSSearch(int b1, int b2, int fp) {
        Queue<BFSEntry> queue = new LinkedList<>();
        queue.offer(new BFSEntry(b1, 0, 0));
        queue.offer(new BFSEntry(b1, 1, 0));
        int maxPathLen = Constants.MAX_BFS_PATH_LEN;
        while (!queue.isEmpty()) {
            BFSEntry x = queue.poll();
            locks.lockOneWrite(x.bucket);
            // pick a random slot to start on
            int startingSlot = x.pathcode % TAGS_PER_BUCKET;
            for (int i = 0; i < TAGS_PER_BUCKET; i++) {
                int slot = (startingSlot + i) % TAGS_PER_BUCKET;
                int tag = table.readTag(b1, slot);
                if (tag == 0) {
                    x.pathcode = x.pathcode * TAGS_PER_BUCKET + slot;
                    locks.unlockOneWrite(x.bucket);
                    return x;
                }
                if (x.depth < maxPathLen-1) {
                    queue.offer(new BFSEntry(
                            altIndex(b1, tag),
                            x.pathcode * TAGS_PER_BUCKET + slot,
                            x.depth+1));
                }
            }
            locks.unlockOneWrite(x.bucket);
        }
        return new BFSEntry(0, 0, -1);
    }

    private boolean cuckooPathMove(int b1, int b2, int fp, CuckooRecord[] cuckooPath, int depth) {
        if (depth == 0) {
            locks.lockTwoWrite(b1, b2);
            if (table.readTag(cuckooPath[0].bucket, cuckooPath[0].slot) == 0) {
                locks.unlockTwoWrite(b1, b2);
                return true;
            } else {
                locks.unlockTwoWrite(b1, b2);
                return false;
            }
        }

        while (depth > 0) {
            CuckooRecord from = cuckooPath[depth-1];
            CuckooRecord to = cuckooPath[depth];
            if (depth == 1) {
                // NOTE: We must hold the locks of b1 and b2.
                // Or their slots may be preempted by another key if we released locks.
                locks.lockThreeWrite(b1, b2, to.bucket);
            } else {
                locks.lockTwoWrite(from.bucket, to.bucket);
            }
            int fromTag = table.readTag(from.bucket, from.slot);
            // if `to` is nonempty, or `from` is not occupied by original tag,
            // in both cases, abort this insertion.
            if (table.readTag(to.bucket, to.slot) != 0 ||
                    fromTag != from.fingerprint) {
                return false;
            }
            table.writeTag(to.bucket, to.slot, fromTag);
            clockTable.writeTag(to.bucket, to.slot, clockTable.readTag(from.bucket, from.slot));
            scopeTable.writeTag(to.bucket, to.slot, scopeTable.readTag(from.bucket, from.slot));
            sizeTable.writeTag(to.bucket, to.slot, sizeTable.readTag(from.bucket, from.slot));
            table.writeTag(from.bucket, from.slot, 0);
            if (depth == 1) {
                // is it probable to.bucket is one of b1 and b2 ?
                if (to.bucket != b1 && to.bucket != b2) {
                    locks.unlockOneWrite(to.bucket);
                }
            } else {
                locks.unlockTwoWrite(from.bucket, to.bucket);
            }
            depth--;
        }
        return true;
    }

    /**
     * Find tag `fp` in bucket b1 and b2.
     * @return the position of `fp`.
     */
    private TagPosition cuckooFind(int b1, int b2, int fp) {
        TagPosition pos = new TagPosition(-1, -1);
        if (!tryFindInsertBucket(b1, fp, pos)) {
            return pos;
        }
        if (!tryFindInsertBucket(b2, fp, pos)) {
            return pos;
        }
        pos.setTagIndex(-1);
        return pos;
    }

    /**
     * Find tag `fp` in bucket `i`.
     * @return true if no duplicated key is found, and `pos.slot` points to an empty slot (if pos.tag != -1);
     * otherwise return false, and store the position of duplicated key in `pos.slot`.
     */
    private boolean tryFindInsertBucket(int i, int fp, TagPosition pos) {
        pos.setBucketAndSlot(i, -1);
        for (int j = 0; j < TAGS_PER_BUCKET; j++) {
            int tag = table.readTag(i, j);
            if (tag != 0) {
                if (tag == fp) {
                    pos.setTagIndex(j);
                    pos.setStatus(CuckooStatus.FAILURE_KEY_DUPLICATED);
                    return false;
                }
            } else {
                pos.setTagIndex(j);
            }
        }
        return true;
    }
}
