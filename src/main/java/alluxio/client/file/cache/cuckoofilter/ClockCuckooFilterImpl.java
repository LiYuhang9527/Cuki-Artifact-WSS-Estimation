/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.file.cache.cuckoofilter;

import alluxio.Constants;
import com.google.common.hash.Funnel;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

public class ClockCuckooFilterImpl<T> implements Serializable {
  private static final double DEFAULT_FPP = 0.01;
  private static final double DEFAULT_LOAD_FACTOR = 0.955;
  private static final int MAX_CUCKOO_COUNT = 500;
  private static final int TAGS_PER_BUCKET = 4;
  private final int numBuckets;
  private final int bitsPerTag;
  private final int bitsPerClock;
  private final Funnel<? super T> funnel;
  private final HashFunction hasher;
  private CuckooTable table;
  private CuckooTable clockTable;
  private AtomicLong numItems;

  public ClockCuckooFilterImpl(CuckooTable table, CuckooTable clockTable, Funnel<? super T> funnel,
                               HashFunction hasher) {
    this.table = table;
    this.numBuckets = table.getNumBuckets();
    this.bitsPerTag = table.getBitsPerTag();
    this.clockTable = clockTable;
    this.bitsPerClock = clockTable.getBitsPerTag();
    this.funnel = funnel;
    this.hasher = hasher;
    numItems = new AtomicLong(0L);
  }

  public static <T> ClockCuckooFilterImpl<T> create(Funnel<? super T> funnel, long expectedInsertions,
                                                    int bitsPerClock, double fpp, double loadFactor, HashFunction hasher) {
    // TODO: make expectedInsertions a power of 2
    int bitsPerTag = Utils.optimalBitsPerTag(fpp, loadFactor);
    long numBuckets = Utils.optimalBuckets(expectedInsertions, loadFactor, TAGS_PER_BUCKET);
    long numBits = numBuckets * TAGS_PER_BUCKET * bitsPerTag;
    // TODO: check numBits overflow (< INT_MAX)
    AbstractBitSet bits = new BuiltinBitSet((int) numBits);
    CuckooTable table = new SingleCuckooTable(bits, (int) numBuckets, TAGS_PER_BUCKET, bitsPerTag);
    AbstractBitSet clockBits =
        new BuiltinBitSet((int) (numBuckets * TAGS_PER_BUCKET * bitsPerClock));
    CuckooTable clockTable =
        new SingleCuckooTable(clockBits, (int) numBuckets, TAGS_PER_BUCKET, bitsPerClock);
    return new ClockCuckooFilterImpl<>(table, clockTable, funnel, hasher);
  }

  public static <T> ClockCuckooFilterImpl<T> create(Funnel<? super T> funnel, long expectedInsertions,
                                                    int bitsPerClock, double fpp, double loadFactor) {
    return create(funnel, expectedInsertions, bitsPerClock, fpp, loadFactor, Hashing.murmur3_128());
  }

  public static <T> ClockCuckooFilterImpl<T> create(Funnel<? super T> funnel, long expectedInsertions,
                                                    int bitsPerClock, double fpp) {
    return create(funnel, expectedInsertions, bitsPerClock, fpp, DEFAULT_LOAD_FACTOR);
  }

  public static <T> ClockCuckooFilterImpl<T> create(Funnel<? super T> funnel, long expectedInsertions,
                                                    int bitsPerClock) {
    return create(funnel, expectedInsertions, bitsPerClock, DEFAULT_FPP);
  }

  public boolean put(T item) {
    IndexAndTag indexAndTag = generateIndexAndTag(item);
    int curIndex = indexAndTag.mBucket;
    int curTag = indexAndTag.mTag;
    int curTagClock = 0xffffffff;
    int oldTag;
    int oldTagClock;
    TagPosition pos = new TagPosition();
    for (int count = 0; count < MAX_CUCKOO_COUNT; count++) {
      boolean kickout = false;
      if (count > 0) {
        kickout = true;
      }
      oldTag = table.insertOrKickoutOne(curIndex, curTag, pos);
      oldTagClock = clockTable.readTag(pos.getBucketIndex(), pos.getTagIndex());
      clockTable.writeTag(pos.getBucketIndex(), pos.getTagIndex(), curTagClock);
      if (oldTag == 0) {
        numItems.incrementAndGet();
        return true;
      }
      if (kickout) {
        curTag = oldTag;
        curTagClock = oldTagClock;
      }
      curIndex = altIndex(curIndex, curTag);
    }

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
    int i1 = indexAndTag.mBucket;
    int tag = indexAndTag.mTag;
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
    int i1 = indexAndTag.mBucket;
    int tag = indexAndTag.mTag;
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

  public void aging() {
    for (int i = 0; i < numBuckets; i++) {
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
          table.writeTag(i, j, 0);
          numItems.decrementAndGet();
        } else {
          // should not come to here
          System.out.println("Error: aging()");
        }
      }
    }
  }

  public String getSummary() {
    return "numBuckets: " + numBuckets() + "\ntagsPerBucket: " + tagsPerBucket() + "\nbitsPerTag: "
        + bitsPerTag() + "\nbitsPerClock: " + getBitsPerClock() + "\nSizeInMB: "
        + (numBuckets() * tagsPerBucket() * bitsPerTag() / 8.0 / Constants.MB
            + numBuckets() * tagsPerBucket() * getBitsPerClock() / 8.0 / Constants.MB);
  }

  public int size() {
    return numItems.intValue();
  }

  public int numBuckets() {
    return table.getNumBuckets();
  }

  public int tagsPerBucket() {
    return table.getNumTagsPerBuckets();
  }

  public int bitsPerTag() {
    return table.getBitsPerTag();
  }

  public int getBitsPerClock() {
    return clockTable.getBitsPerTag();
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
