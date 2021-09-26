package alluxio.client.file.cache.filter;

import java.util.concurrent.locks.StampedLock;

public class SegmentedLock {
    private final int numLocks;
    private final int bitsOfLock;
    private final int numBuckets;
    private final int maskBits;
    private final StampedLock[] locks;

    public SegmentedLock(int numLocks, int numBuckets) {
        int highestBit = Integer.highestOneBit(numLocks);
        if (highestBit < numLocks) {
            numLocks = highestBit << 1;
        }
        this.numLocks = numLocks;
        this.bitsOfLock = Integer.numberOfTrailingZeros(numLocks);
        this.numBuckets = numBuckets;
        int bitsOfBuckets = Integer.numberOfTrailingZeros(Integer.highestOneBit(numBuckets));
        this.maskBits = bitsOfBuckets - bitsOfLock;
        locks = new StampedLock[numLocks];
        for (int i = 0; i < numLocks; i++) {
            locks[i] = new StampedLock();
        }
    }

    public void lockOneRead(int b) {
        int i = getLockIndex(b);
        locks[i].readLock();
    }

    public void unlockOneRead(int b) {
        int i = getLockIndex(b);
        locks[i].tryUnlockRead();
    }

    public void lockOneWrite(int b) {
        int i = getLockIndex(b);
        locks[i].writeLock();
    }

    public void unlockOneWrite(int b) {
        int i = getLockIndex(b);
        locks[i].tryUnlockWrite();
    }

    public void lockTwoRead(int b1, int b2) {
        int i1 = getLockIndex(b1);
        int i2 = getLockIndex(b2);
        if (i1 > i2) {
            int tmp = i1;
            i1 = i2;
            i2 = tmp;
        }
        locks[i1].readLock();
        if (i2 != i1) {
            locks[i2].readLock();
        }
    }

    public void unlockTwoRead(int b1, int b2) {
        int i1 = getLockIndex(b1);
        int i2 = getLockIndex(b2);
        // Question: is unlock order important ?
        locks[i1].tryUnlockRead();
        locks[i2].tryUnlockRead();
    }

    public void lockTwoWrite(int b1, int b2) {
        int i1 = getLockIndex(b1);
        int i2 = getLockIndex(b2);
        if (i1 > i2) {
            int tmp = i1;
            i1 = i2;
            i2 = tmp;
        }
        locks[i1].writeLock();
        if (i2 != i1) {
            locks[i2].writeLock();
        }
    }

    public void unlockTwoWrite(int b1, int b2) {
        int i1 = getLockIndex(b1);
        int i2 = getLockIndex(b2);
        // Question: is unlock order important ?
        locks[i1].tryUnlockWrite();
        locks[i2].tryUnlockWrite();
    }

    public void lockThreeWrite(int b1, int b2, int b3) {
        int i1 = getLockIndex(b1);
        int i2 = getLockIndex(b2);
        int i3 = getLockIndex(b3);
        int tmp;
        if (i1 > i2) {
            tmp = i1;
            i1 = i2;
            i2 = tmp;
        }
        if (i2 > i3) {
            tmp = i2;
            i2 = i3;
            i3 = tmp;
        }
        if (i1 > i2) {
            tmp = i1;
            i1 = i2;
            i2 = tmp;
        }
        locks[i1].writeLock();
        if (i2 != i1) {
            locks[i2].writeLock();
        }
        if (i3 != i2) {
            locks[i3].writeLock();
        }
    }

    public int getNumLocks() {
        return numLocks;
    }

    private int getLockIndex(int b) {
        // each lock guard a consecutive buckets
        return b >> maskBits;
    }
}
