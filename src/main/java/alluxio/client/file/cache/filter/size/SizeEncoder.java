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

public class SizeEncoder implements ISizeEncoder {
  private final int maxSizeBits;
  private final int sizeGroupBits;
  private final int numBuckets;
  private final Bucket[] buckets;

  public SizeEncoder(int maxSizeBits, int numBucketsBits) {
    this.maxSizeBits = maxSizeBits;
    this.sizeGroupBits = numBucketsBits;
    this.numBuckets = (1 << numBucketsBits);
    this.buckets = new Bucket[numBuckets];
    for (int i = 0; i < numBuckets; i++) {
      buckets[i] = new Bucket();
    }
  }

  public void add(int size) {
    buckets[getSizeGroup(size)].add(size);
  }

  public int dec(int group) {
    return buckets[group].decrement();
  }

  public long getTotalSize() {
    long totalSize = 0;
    for (int i = 0; i < numBuckets; i++) {
      totalSize += buckets[i].getSize();
    }
    return totalSize;
  }

  private int getSizeGroup(int size) {
    return (size >> (maxSizeBits - sizeGroupBits));
  }

  @Override
  public void access(int size) {
    // no-op
  }

  @Override
  public int encode(int size) {
    return getSizeGroup(size);
  }
}

