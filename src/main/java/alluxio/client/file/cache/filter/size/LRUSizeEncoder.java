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

package alluxio.client.file.cache.filter.size;

import java.util.LinkedList;

public class LRUSizeEncoder implements ISizeEncoder {
  private final int maxSizeBits;
  private final int numGroupBits;
  private final int numBucketBits;
  private final int numGroups;
  private final int numBuckets;
  private final int numBucketsPerGroup;
  private final LRUGroup groups[];

  LRUSizeEncoder(int maxSizeBits, int numGroupBits, int numBucketBits) {
    this.maxSizeBits = maxSizeBits;
    this.numGroupBits = numGroupBits;
    this.numGroups = (1 << numGroupBits);
    this.numBucketBits = numBucketBits;
    this.numBuckets = (1 << numBucketBits);
    this.numBucketsPerGroup = (1 << (numBucketBits - numGroupBits));
    this.groups = new LRUGroup[numGroups];
    for (int i = 0; i < numGroups; i++) {
      groups[i] = new LRUGroup(numBucketBits - numGroupBits, maxSizeBits - numGroupBits);
    }
  }

  public void add(int size) {
    groups[getGroup(size)].add(maskSize(size));
  }

  public int dec(int group) {
    return groups[getGroup(group)].dec();
  }

  @Override
  public void access(int size) {
    groups[getGroup(size)].access(size);
  }

  public long getTotalSize() {
    long totalSize = 0;
    for (int i = 0; i < numGroups; i++) {
      totalSize += groups[i].getTotalSize()
          + groups[i].getTotalCount() * (1 << (maxSizeBits - numGroupBits)) * i;
    }
    return totalSize;
  }

  private int getGroup(int size) {
    return (size >> (maxSizeBits - numGroupBits));
  }

  private int maskSize(int size) {
    return (size & ((1 << (maxSizeBits - numGroupBits)) - 1));
  }

  @Override
  public int encode(int size) {
    return getGroup(size);
  }

  static class LRUGroup {
    private final int totalBits;
    private final int numBucketBits;
    private final int numBuckets;
    private final Bucket[] buckets;
    private final LinkedList<Integer> lruCache;

    LRUGroup(int numBucketBits, int totalBits) {
      this.totalBits = totalBits;
      this.numBucketBits = numBucketBits;
      this.numBuckets = (1 << numBucketBits);
      this.buckets = new Bucket[numBuckets];
      this.lruCache = new LinkedList<>();
      for (int i = 0; i < numBuckets; i++) {
        buckets[i] = new Bucket();
        lruCache.add(i);
      }
    }

    public void add(int size) {
      int b = getBucket(size);
      buckets[b].add(size);
      lruCache.remove(Integer.valueOf(b));
      lruCache.add(b);
    }

    public int dec() {
      int b = lruCache.getFirst();
      while (buckets[b].getCount() == 0) {
        lruCache.remove(Integer.valueOf(b));
        lruCache.add(b);
        b = lruCache.getFirst();
      }
      return buckets[b].decrement();
    }

    public void access(int size) {
      int b = getBucket(size);
      lruCache.remove(Integer.valueOf(b));
      lruCache.add(b);
    }

    public long getTotalCount() {
      long totalCount = 0;
      for (int i = 0; i < numBuckets; i++) {
        totalCount += buckets[i].getCount();
      }
      return totalCount;
    }

    public long getTotalSize() {
      long totalSize = 0;
      for (int i = 0; i < numBuckets; i++) {
        totalSize += buckets[i].getSize();
      }
      return totalSize;
    }

    private int getBucket(int size) {
      return size >> (totalBits - numBucketBits);
    }
  }
}

