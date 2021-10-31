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

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class ThroughputBenchmark implements Benchmark {
  private final BenchmarkContext mBenchmarkContext;
  private final BenchmarkParameters mBenchmarkParameters;
  private final int mNumThreads;
  private final ShadowCache mShadowCache;
  private final List<CacheClient> mClients = new LinkedList<>();
  private final List<Thread> mThreads = new LinkedList<>();

  public ThroughputBenchmark(BenchmarkContext benchmarkContext, BenchmarkParameters parameters) {
    mBenchmarkContext = benchmarkContext;
    mBenchmarkParameters = parameters;
    mNumThreads = parameters.mNumThreads;
    mShadowCache = ShadowCache.create(parameters);
    mShadowCache.stopUpdate();
  }

  @Override
  public boolean prepare() {
    for (int i = 0; i < mNumThreads; i++) {
      GeneralDataset<String> dataset = new GeneralDataset<>(
          new RandomEntryGenerator(mBenchmarkParameters.mMaxEntries, 1,
              (int) mBenchmarkParameters.mNumUniqueEntries + 1, 1, 1024, 1, 1001 + i),
          (int) mBenchmarkParameters.mWindowSize);
      mClients.add(new CacheClient(i, mShadowCache, dataset));
      mThreads.add(new Thread(mClients.get(i)));
    }
    return true;
  }

  @Override
  public void run() {
    System.out.println();
    System.out.println("ConcurrencyBenchmark");
    System.out.println(mShadowCache.getSummary());
    System.out.printf("num_threads=%d\n", mNumThreads);
    long startTick = System.currentTimeMillis();
    for (int i = 0; i < mNumThreads; i++) {
      mThreads.get(i).start();
    }
    long totalOperations = 0;
    for (int i = 0; i < mNumThreads; i++) {
      try {
        mThreads.get(i).join();
      } catch (Exception e) {
        e.printStackTrace();
      }
      totalOperations += mClients.get(i).mOperationCount.get();
    }
    long duration = (System.currentTimeMillis() - startTick);
    System.out.printf("Cost %d ms, Throughput %d ops/sec\n", duration,
        totalOperations * 1000 / duration);
  }

  private static class CacheClient implements Runnable {
    private final int mThreadId;
    private final ShadowCache mShadowCache;
    private final Dataset<String> mClientDataset;
    private final AtomicLong mOperationCount = new AtomicLong(0);

    public CacheClient(int threadId, ShadowCache shadowCache, Dataset<String> clientDataset) {
      mThreadId = threadId;
      mShadowCache = shadowCache;
      mClientDataset = clientDataset;
    }

    @Override
    public void run() {
      while (mClientDataset.hasNext()) {
        DatasetEntry<String> entry = mClientDataset.next();
        mOperationCount.incrementAndGet();
        PageId item = new PageId(entry.getScopeInfo().toString(), entry.getItem().hashCode());
        int nread = mShadowCache.get(item, entry.getSize(), entry.getScopeInfo());
        if (nread <= 0) {
          mShadowCache.put(item, entry.getSize(), entry.getScopeInfo());
        }
      }
    }
  }
}
