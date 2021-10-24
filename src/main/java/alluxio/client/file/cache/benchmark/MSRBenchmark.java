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

package alluxio.client.file.cache.benchmark;

import alluxio.client.file.cache.CacheContext;
import alluxio.Constants;
import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.ShadowCache;
import alluxio.client.file.cache.dataset.Dataset;
import alluxio.client.file.cache.dataset.DatasetEntry;
import alluxio.client.file.cache.dataset.GeneralDataset;
import alluxio.client.file.cache.dataset.generator.MSREntryGenerator;
import alluxio.client.file.cache.cuckoofilter.ScopedClockCuckooFilter;

public class MSRBenchmark {
  private static final int NUM_UNIQUE_ENTRY = 10 * Constants.MB;
  private static final int BITS_PER_CLOCK = 8;
  private static final int BITS_PER_SIZE = 20;
  private static final int BITS_PER_SCOPE = 8;
  private static long NUM_ENTRIES = 12518968;
  private static int WINDOW_SIZE = 64 * Constants.KB;

  public static void main(String[] args) {
    MSREntryGenerator generator = new MSREntryGenerator("G:/datasets/msr-cambridge1/prxy_0.csv");
    Dataset<String> dataset = new GeneralDataset<>(generator, WINDOW_SIZE);

    ScopedClockCuckooFilter<PageId> clockFilter =
        ScopedClockCuckooFilter.create(ShadowCache.PageIdFunnel.FUNNEL, NUM_UNIQUE_ENTRY,
            BITS_PER_CLOCK, BITS_PER_SIZE, BITS_PER_SCOPE);
    int ccfAgingIntv = WINDOW_SIZE >> BITS_PER_CLOCK;

    ShadowCache mCacheManager = new ShadowCache();
    CacheContext context = new CacheContext();
    int bfAgingIntv = WINDOW_SIZE >> 2; // Aging each T/4

    System.out.println(clockFilter.getSummary());
    int cnt = 0;
    long totalDuration = 0, bfTotalDuration = 0;
    long agingDuration = 0, bfAgingDuration = 0;
    long filterDuration = 0, bfFilterDuration = 0;
    int cfAgingCount = 0, bfAgingCount = 0;
    long stackTick = System.currentTimeMillis();
    System.out.println("#operation" + "\t" + "Real" + "\t" + "Real(bytes)" + "\t" + "MBF" + "\t"
        + "MBF(bytes)" + "\t" + "CCF" + "\t" + "CCF(bytes)");
    while (dataset.hasNext()) {
      cnt++;
      DatasetEntry<String> entry = dataset.next();

      long startFilterTick = System.currentTimeMillis();
      PageId item = new PageId("table1", entry.getItem().hashCode());
      if (!clockFilter.mightContainAndResetClock(item)) {
        clockFilter.put(item, entry.getSize(), entry.getScopeInfo());
      }
      filterDuration += System.currentTimeMillis() - startFilterTick;

      long startBFTick = System.currentTimeMillis();
      mCacheManager.put(item, new byte[entry.getSize()], context);
      bfFilterDuration += System.currentTimeMillis() - startBFTick;

      // Aging CCF
      if (cnt % ccfAgingIntv == 0) {
        cfAgingCount++;
        long startAgingTick = System.currentTimeMillis();
        clockFilter.aging();
        agingDuration += System.currentTimeMillis() - startAgingTick;
      }

      // Aging BF
      if (cnt % bfAgingIntv == 0) {
        bfAgingCount++;
        long startAgingTick = System.currentTimeMillis();
        mCacheManager.switchBloomFilter();
        bfAgingDuration += System.currentTimeMillis() - startAgingTick;
      }

      totalDuration = (System.currentTimeMillis() - stackTick);
      if (cnt % (WINDOW_SIZE >> 4) == 0) {
        System.out.println(
            cnt + "\t" + dataset.getRealEntryNumber() + "\t" + dataset.getRealEntrySize() + "\t"
                + mCacheManager.getShadowCachePages() + "\t" + mCacheManager.getShadowCacheBytes()
                + "\t" + clockFilter.size() + "\t" + clockFilter.sizeInBytes());
      }
    }
    System.out.println(" \tPut/Get(ms)\tAging(ms)\tAgingCount\n" + "MBF\t" + bfFilterDuration + "\t"
        + bfAgingDuration + "\t" + bfAgingCount + "\n" + "CCF\t" + filterDuration + "\t"
        + agingDuration + "\t" + cfAgingCount);
  }
}
