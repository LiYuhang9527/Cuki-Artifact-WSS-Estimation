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

package alluxio.client.file.cache.dataset;

import alluxio.Constants;
import alluxio.client.file.cache.filter.ScopeInfo;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class RandomIntegerDataset implements Dataset<Integer> {
  private static final ScopeInfo DEFAULT_SCOPE = new ScopeInfo("db1.table1");
  private static final int BYTES_PER_ITEM = 1;

  private final long numEntry;
  private final int windowSize;
  private final int lowerBound;
  private final int upperBound;
  private final Random random;
  private final Lock lock;
  private final AtomicLong count;

  // private final ConcurrentLinkedQueue<Integer> queue;
  // private final ConcurrentHashMap<Integer, Integer> map;
  private final Queue<DatasetEntry<Integer>> queue;
  private final HashMap<DatasetEntry<Integer>, Integer> map;
  private final HashMap<ScopeInfo, Integer> scopedNumber;
  private final HashMap<ScopeInfo, Integer> scopedSize;
  private int realNumber;
  private int realSize;

  public RandomIntegerDataset(long numEntry, int windowSize, int lowerBound, int upperBound) {
    this.numEntry = numEntry;
    this.windowSize = windowSize;
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
    this.random = ThreadLocalRandom.current();
    this.lock = new ReentrantLock();
    this.count = new AtomicLong(0);
    this.queue = new LinkedList<>();
    this.map = new HashMap<>();
    // this.queue = new ConcurrentLinkedQueue<>();
    // this.map = new ConcurrentHashMap<>();
    this.scopedNumber = new HashMap<>();
    this.scopedSize = new HashMap<>();
    this.realNumber = 0;
    this.realSize = 0;
  }

  public RandomIntegerDataset(long numEntry, int windowSize, int lowerBound, int upperBound,
      long seed) {
    this.numEntry = numEntry;
    this.windowSize = windowSize;
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
    this.random = new Random(seed);
    this.lock = new ReentrantLock();
    this.count = new AtomicLong(0);
    this.queue = new LinkedList<>();
    this.map = new HashMap<>();
    this.scopedNumber = new HashMap<>();
    this.scopedSize = new HashMap<>();
    this.realNumber = 0;
    this.realSize = 0;
  }

  @Override
  public DatasetEntry<Integer> next() {
    count.incrementAndGet();
    lock.lock();
    int r = lowerBound + random.nextInt(upperBound);
    ScopeInfo scope = new ScopeInfo("table" + (r % 64));
    int size = (r * 31213) % Constants.KB;
    if (size < 0) {
      size = -size;
    }
    DatasetEntry<Integer> entry = new DatasetEntry<>(r, size, scope);
    queue.offer(entry);
    Integer cnt = map.get(entry);
    if (cnt != null && cnt > 0) {
      map.put(entry, cnt + 1);
    } else {
      map.put(entry, 1);
      scopedSize.put(entry.getScopeInfo(),
          scopedSize.getOrDefault(entry.getScopeInfo(), 0) + entry.getSize());
      scopedNumber.put(entry.getScopeInfo(),
          scopedNumber.getOrDefault(entry.getScopeInfo(), 0) + 1);
      realSize += entry.getSize();
    }
    if (queue.size() > windowSize) {
      DatasetEntry<Integer> staleItem = queue.poll();
      assert staleItem != null;
      Integer itemCount = map.get(staleItem);
      assert itemCount != null;
      if (itemCount <= 1) {
        map.remove(staleItem);
        scopedSize.put(staleItem.getScopeInfo(),
            scopedSize.getOrDefault(staleItem.getScopeInfo(), 0) - staleItem.getSize());
        scopedNumber.put(staleItem.getScopeInfo(),
            scopedNumber.getOrDefault(staleItem.getScopeInfo(), 0) - 1);
        realSize -= staleItem.getSize();
      } else {
        map.put(staleItem, itemCount - 1);
      }
    }
    realNumber = map.size();
    lock.unlock();
    return entry;
  }

  @Override
  public boolean hasNext() {
    return count.get() < numEntry;
  }

  @Override
  public int getRealEntryNumber() {
    return realNumber;
  }

  @Override
  public int getRealEntryNumber(ScopeInfo scope) {
    return scopedNumber.getOrDefault(scope, 0);
  }

  @Override
  public int getRealEntrySize() {
    return realSize;
  }

  @Override
  public int getRealEntrySize(ScopeInfo scope) {
    return scopedSize.getOrDefault(scope, 0);
  }
}
