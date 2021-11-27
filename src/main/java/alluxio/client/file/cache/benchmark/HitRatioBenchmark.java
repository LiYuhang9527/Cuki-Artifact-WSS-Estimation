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

import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.ShadowCache;
import alluxio.client.file.cache.dataset.Dataset;
import alluxio.client.file.cache.dataset.DatasetEntry;
import alluxio.client.file.cache.dataset.GeneralDataset;
import alluxio.client.file.cache.dataset.generator.RandomEntryGenerator;
import alluxio.client.file.cache.dataset.generator.SequentialEntryGenerator;

public class HitRatioBenchmark implements Benchmark {
  private final BenchmarkContext mBenchmarkContext;
  private final BenchmarkParameters mBenchmarkParameters;
  private final ShadowCache mShadowCache;
  private Dataset<String> mDataset1;
  private Dataset<String> mDataset2;

  public HitRatioBenchmark(BenchmarkContext benchmarkContext,
      BenchmarkParameters benchmarkParameters) {
    mBenchmarkContext = benchmarkContext;
    mBenchmarkParameters = benchmarkParameters;
    mShadowCache = ShadowCache.create(benchmarkParameters);
    mShadowCache.stopUpdate();
    mDataset1 = new GeneralDataset<>(
        new SequentialEntryGenerator(mBenchmarkParameters.mMaxEntries, 1,
            (int) mBenchmarkParameters.mNumUniqueEntries + 1, 1, 1024, 1),
        (int) mBenchmarkParameters.mWindowSize);
    mDataset2 = new GeneralDataset<>(
        new RandomEntryGenerator(mBenchmarkParameters.mMaxEntries, 1,
            (int) (mBenchmarkParameters.mNumUniqueEntries * 2) + 1, 1, 1024, 1, 32713),
        (int) mBenchmarkParameters.mWindowSize);
  }

  @Override
  public void run() {
    System.out.println();
    System.out.println("HitRatioBenchmark");
    System.out.println(mShadowCache.getSummary());

    // put all items into shadow cache
    long cachePages = mBenchmarkParameters.mNumUniqueEntries;
    long cacheBytes = 0;
    for (long i = 0; i < cachePages; i++) {
      DatasetEntry<String> entry = mDataset1.next();
      PageId item = new PageId(entry.getScopeInfo().toString(), entry.getItem().hashCode());
      mShadowCache.put(item, entry.getSize(), entry.getScopeInfo());
      cacheBytes += entry.getSize();
    }

    System.out.println();
    System.out.printf("Before read, hr(num)=%f, hr(byte)=%f\n",
        mShadowCache.getShadowCachePageHit() / (double) mShadowCache.getShadowCachePageRead(),
        mShadowCache.getShadowCacheByteHit() / (double) mShadowCache.getShadowCacheByteRead());

    // then check each inserted item, should have a hit ratio of about 50%
    long pagesHit = 0;
    long bytesHit = 0;
    for (long i = 0; i < mBenchmarkParameters.mNumUniqueEntries; i++) {
      DatasetEntry<String> entry = mDataset2.next();
      PageId item = new PageId(entry.getScopeInfo().toString(), entry.getItem().hashCode());
      mShadowCache.get(item, entry.getSize(), entry.getScopeInfo());
      if (Long.parseLong(entry.getItem()) <= cachePages) {
        pagesHit++;
        bytesHit += entry.getSize();
      }
    }

    System.out.println();
    System.out.printf("Expected hr(num)=%f, hr(byte)=%f\n", pagesHit / (double) cachePages,
        bytesHit / (double) cacheBytes);
    System.out.printf("After read, hr(num)=%f, hr(byte)=%f\n",
        mShadowCache.getShadowCachePageHit() / (double) mShadowCache.getShadowCachePageRead(),
        mShadowCache.getShadowCacheByteHit() / (double) mShadowCache.getShadowCacheByteRead());
  }
}
