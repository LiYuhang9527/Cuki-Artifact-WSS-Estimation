/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0 (the
 * "License"). You may not use this work except in compliance with the License, which is available
 * at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.file.cache.benchmark;

import alluxio.Constants;
import alluxio.client.file.cache.CacheContext;
import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.ShadowCache;
import alluxio.client.file.cache.dataset.Dataset;
import alluxio.client.file.cache.dataset.DatasetEntry;
import alluxio.client.file.cache.dataset.GeneralDataset;
import alluxio.client.file.cache.dataset.generator.EntryGenerator;
import alluxio.client.file.cache.dataset.generator.MSREntryGenerator;
import alluxio.client.file.cache.dataset.generator.RandomEntryGenerator;
import alluxio.client.file.cache.dataset.generator.TwitterEntryGenerator;
import alluxio.client.file.cache.filter.ConcurrentClockCuckooFilter;
import alluxio.client.file.cache.filter.IClockCuckooFilter;
import alluxio.client.file.cache.filter.SlidingWindowType;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import java.io.PrintStream;

public class ConcurrentBenchmark {
  private static final int BITS_PER_SIZE = 20;
  private static final int BITS_PER_SCOPE = 8;
  private static final int SLOTS_PER_BUCKET = 4;
  private static final int BITS_PER_TAG = 8;

  private static final Options OPTIONS =
      new Options().addOption("help", false, "Show help for this test.")
          .addOption("benchmark", true, "The benchmark type.")
          .addOption("trace", true, "The path to trace.")
          .addOption("max_entries", true, "The maximum number of entries will be loaded.")
          .addOption("memory", true, "The memory overhead in MB.")
          .addOption("window_size", true, "The size of sliding window.")
          .addOption("clock_bits", true, "The number of bits of clock field.")
          .addOption("opportunistic_aging", false, "Enable opportunistic aging.")
          .addOption("report_file", true,
                  "The file where reported information will be written to.");

  private static boolean mHelp;
  private static String mBenchmark;
  private static String mTrace;
  private static long mMaxEntries;
  private static int mMemoryOverhead;
  private static int mWindowSize;
  private static int mClockBits;
  private static boolean mOpportunisticAging;
  private static String mReportFile;

  private static Dataset<String> mDataset;

  private static IClockCuckooFilter<PageId> mClockFilter;
  private static int mCcfAgingPeriod;

  private static ShadowCache mCacheManager = new ShadowCache();
  private static CacheContext mContext = new CacheContext();
  private static int mBfAgingPeriod;

  private static PrintStream mStream;

  private static boolean parseArguments(String[] args) {
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd;
    try {
      cmd = parser.parse(OPTIONS, args);
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }
    mHelp = cmd.hasOption("help");
    mBenchmark = cmd.getOptionValue("benchmark", "Random");
    mTrace = cmd.getOptionValue("trace", "");
    mMaxEntries = Long.parseLong(cmd.getOptionValue("max_entries", "1024"));
    mMemoryOverhead = Integer.parseInt(cmd.getOptionValue("memory", "1"));
    mWindowSize = Integer.parseInt(cmd.getOptionValue("window_size", "512"));
    mClockBits = Integer.parseInt(cmd.getOptionValue("clock_bits", "2"));
    mOpportunisticAging = cmd.hasOption("opportunistic_aging");
    mReportFile = cmd.getOptionValue("report_file", "stdout");
    return true;
  }

  private static boolean init() {
    EntryGenerator<String> generator;
    switch (mBenchmark) {
      case "random":
        generator = new RandomEntryGenerator(mMaxEntries);
        break;
      case "msr":
        generator = new MSREntryGenerator(mTrace);
        break;
      case "twitter":
        generator = new TwitterEntryGenerator(mTrace);
        break;
      default:
        System.out.printf("Error: illegal trace %s\n", mTrace);
        return false;
    }
    mDataset = new GeneralDataset<>(generator, mWindowSize);

    // init cuckoo filter
    long budgetInBits = mMemoryOverhead * Constants.MB * 8;
    long bitsPerSlot = BITS_PER_TAG + mClockBits + BITS_PER_SIZE + BITS_PER_SCOPE;
    long totalBuckets = budgetInBits / bitsPerSlot / SLOTS_PER_BUCKET;
    long expectedInsertions = Long.highestOneBit(totalBuckets);
    if (mOpportunisticAging) {
      mClockFilter = ConcurrentClockCuckooFilter.create(ShadowCache.PageIdFunnel.FUNNEL,
          expectedInsertions, mClockBits, BITS_PER_SIZE, BITS_PER_SCOPE,
          SlidingWindowType.COUNT_BASED, mWindowSize);
    } else {
      mClockFilter = ConcurrentClockCuckooFilter.create(ShadowCache.PageIdFunnel.FUNNEL,
          expectedInsertions, mClockBits, BITS_PER_SIZE, BITS_PER_SCOPE);
    }
    mCcfAgingPeriod = mWindowSize >> mClockBits;

    // init bloom filter
    mCacheManager = new ShadowCache();
    mContext = new CacheContext();
    mBfAgingPeriod = mWindowSize >> 2; // Aging each T/4

    // init report file print stream
    if (mReportFile.equals("stdout")) {
      mStream = System.out;
    } else {
      try {
        mStream = new PrintStream(mReportFile);
      } catch (Exception e) {
        System.out.printf("Error: illegal report file %s\n", mReportFile);
        e.printStackTrace();
        return false;
      }
    }

    return true;
  }

  private static void usage() {
    new HelpFormatter().printHelp(String.format(
        "java -cp <JAR>> %s -benchmark <[random, msr, twitter]> -trace <path> -max_entries <entries> "
            + "-memory <memory> -window_size <window_size> -clock_bits <clock_bits>",
        ConcurrentBenchmark.class.getCanonicalName()),
        "run a mini benchmark to write or read a file", OPTIONS, "", true);
  }

  private static void printArguments() {
    System.out.printf(
        "-benchmark %s -trace %s -max_entries %d -memory %d -window_size %d -clock_bits %d\n",
        mBenchmark, mTrace, mMaxEntries, mMemoryOverhead, mWindowSize, mClockBits);
  }

  public static void main(String[] args) {
    if (!parseArguments(args)) {
      usage();
      System.exit(-1);
    }
    if (mHelp) {
      usage();
      System.exit(0);
    }
    if (!init()) {
      usage();
      System.exit(-1);
    }

    printArguments();
    System.out.println();
    System.out.println("CCF summary:\n" + mClockFilter.getSummary());

    int cnt = 0;
    long totalDuration = 0;
    long agingDuration = 0, bfAgingDuration = 0;
    long filterDuration = 0, bfFilterDuration = 0;
    int cfAgingCount = 0, bfAgingCount = 0;
    long stackTick = System.currentTimeMillis();
    mStream.println("#operation" + "\t" + "Real" + "\t" + "Real(bytes)" + "\t" + "MBF" + "\t"
        + "MBF(bytes)" + "\t" + "CCF" + "\t" + "CCF(bytes)");
    while (mDataset.hasNext() && cnt < mMaxEntries) {
      cnt++;
      DatasetEntry<String> entry = mDataset.next();

      long startFilterTick = System.currentTimeMillis();
      PageId item = new PageId("table1", entry.getItem().hashCode());
      if (!mClockFilter.mightContainAndResetClock(item)) {
        mClockFilter.put(item, entry.getSize(), entry.getScopeInfo());
      }
      mClockFilter.increaseOperationCount(1);
      filterDuration += System.currentTimeMillis() - startFilterTick;

      long startBFTick = System.currentTimeMillis();
      mCacheManager.put(item, new byte[entry.getSize()], mContext);
      bfFilterDuration += System.currentTimeMillis() - startBFTick;

      // Aging CCF
      if (cnt % mCcfAgingPeriod == 0) {
        cfAgingCount++;
        long startAgingTick = System.currentTimeMillis();
        mClockFilter.aging();
        agingDuration += System.currentTimeMillis() - startAgingTick;
      }

      // Aging BF
      if (cnt % mBfAgingPeriod == 0) {
        bfAgingCount++;
        long startAgingTick = System.currentTimeMillis();
        mCacheManager.switchBloomFilter();
        bfAgingDuration += System.currentTimeMillis() - startAgingTick;
      }

      if (cnt % (mWindowSize >> (mClockBits + 2)) == 0) {
        mStream.println(
            cnt + "\t" + mDataset.getRealEntryNumber() + "\t" + mDataset.getRealEntrySize() + "\t"
                + mCacheManager.getShadowCachePages() + "\t" + mCacheManager.getShadowCacheBytes()
                + "\t" + mClockFilter.getItemNumber() + "\t" + mClockFilter.getItemSize());
      }
    }
    totalDuration = (System.currentTimeMillis() - stackTick);
    System.out.println();
    System.out.println("TotalTime(ms)\t" + totalDuration);
    System.out.println(" \tPut/Get(ms)\tAging(ms)\tAgingCount\n" + "MBF\t" + bfFilterDuration + "\t"
        + bfAgingDuration + "\t" + bfAgingCount + "\n" + "CCF\t" + filterDuration + "\t"
        + agingDuration + "\t" + cfAgingCount);
  }
}
