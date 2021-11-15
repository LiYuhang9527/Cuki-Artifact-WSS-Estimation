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
import alluxio.client.file.cache.dataset.generator.EntryGenerator;
import alluxio.client.file.cache.dataset.generator.MSREntryGenerator;
import alluxio.client.file.cache.dataset.generator.RandomEntryGenerator;
import alluxio.client.file.cache.dataset.generator.SequentialEntryGenerator;
import alluxio.client.file.cache.dataset.generator.TwitterEntryGenerator;

public class AccuracyBenchmark implements Benchmark {
  private final BenchmarkContext mBenchmarkContext;
  private final BenchmarkParameters mBenchmarkParameters;
  private final ShadowCache mShadowCache;
  private Dataset<String> mDataset;

  public AccuracyBenchmark(BenchmarkContext benchmarkContext,
      BenchmarkParameters benchmarkParameters) {
    mBenchmarkContext = benchmarkContext;
    mBenchmarkParameters = benchmarkParameters;
    mShadowCache = ShadowCache.create(benchmarkParameters);
    createDataset();
    mShadowCache.stopUpdate();
  }

  private void createDataset() {
    EntryGenerator<String> generator;
    switch (mBenchmarkParameters.mDataset) {
      case "sequential":
        generator = new SequentialEntryGenerator(mBenchmarkParameters.mMaxEntries, 1,
            (int) mBenchmarkParameters.mNumUniqueEntries + 1);
        break;
      case "msr":
        generator = new MSREntryGenerator(mBenchmarkParameters.mTrace);
        break;
      case "twitter":
        generator = new TwitterEntryGenerator(mBenchmarkParameters.mTrace);
        break;
      case "random":
      default:
        generator = new RandomEntryGenerator(mBenchmarkParameters.mMaxEntries, 1,
            (int) mBenchmarkParameters.mNumUniqueEntries + 1);
    }
    mDataset = new GeneralDataset<>(generator, (int) mBenchmarkParameters.mWindowSize);
  }

  @Override
  public boolean prepare() {
    return false;
  }

  @Override
  public void run() {
    long opsCount = 0;
    long agingCount = 0;
    long agingDuration = 0;
    long cacheDuration = 0;
    double numARE = 0.0;
    double byteARE = 0.0;
    long errCnt = 0;
    long agingPeriod = mBenchmarkParameters.mWindowSize / mBenchmarkParameters.mAgeLevels;
    if(agingPeriod <= 0){
      agingPeriod = 1;
    }
    System.out.printf("agingPeriod:%d\n",agingPeriod);


    System.out.println(mShadowCache.getSummary());
    mBenchmarkContext.mStream
        .println("#operation\tReal\tReal(byte)\tEst\tEst(byte)\tHR(page)\tHR(byte)");

    long startTick = System.currentTimeMillis();
    while (mDataset.hasNext() && opsCount < mBenchmarkParameters.mMaxEntries) {
      opsCount++;
      DatasetEntry<String> entry = mDataset.next();

      PageId item = new PageId(entry.getScopeInfo().toString(), entry.getItem().hashCode());
      long startCacheTick = System.currentTimeMillis();
      int nread = mShadowCache.get(item, entry.getSize(), entry.getScopeInfo());
      if (nread <= 0) {
        mShadowCache.put(item, entry.getSize(), entry.getScopeInfo());
      }
      mShadowCache.updateTimestamp(1);
      cacheDuration += (System.currentTimeMillis() - startCacheTick);

      // Aging
      if (opsCount % agingPeriod == 0) {
        agingCount++;
        long startAgingTick = System.currentTimeMillis();
        mShadowCache.aging();
        agingDuration += System.currentTimeMillis() - startAgingTick;
      }

      // report
      if (opsCount % mBenchmarkParameters.mReportInterval == 0) {
        mShadowCache.updateWorkingSetSize();
        long realNum = mDataset.getRealEntryNumber();
        long realByte = mDataset.getRealEntrySize();
        long estNum = mShadowCache.getShadowCachePages();
        long estByte = mShadowCache.getShadowCacheBytes();
        mBenchmarkContext.mStream
            .println(opsCount + "\t" + realNum + "\t" + realByte + "\t" + estNum + "\t" + estByte);
        // accumulate error
        errCnt++;
        numARE += Math.abs(estNum / (double) realNum - 1.0);
        byteARE += Math.abs(estByte / (double) realByte - 1.0);
      }
    }
    long totalDuration = (System.currentTimeMillis() - startTick);
    System.out.println();
    System.out.println("TotalTime(ms)\t" + totalDuration);
    System.out.println();
    System.out
        .println("Put/Get(ms)\tAging(ms)\tAgingCnt\tops/sec\tops/sec(aging)\tARE(Num)\tARE(Byte)");
    System.out.printf("%d\t%d\t%d\t%.2f\t%.2f\t%.4f%%\t%.4f%%\n", cacheDuration, agingDuration,
        agingCount, opsCount * 1000 / (double) cacheDuration,
        opsCount * 1000 / (double) (cacheDuration + agingDuration), numARE * 100 / errCnt,
        byteARE * 100 / errCnt);
  }

  @Override
  public boolean finish() {
    return false;
  }
}
