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
import alluxio.client.file.cache.dataset.generator.SequentialEntryGenerator;
import alluxio.client.file.cache.dataset.generator.TwitterEntryGenerator;
import alluxio.client.file.cache.cuckoofilter.ConcurrentClockCuckooFilter;
import alluxio.client.file.cache.cuckoofilter.ConcurrentClockCuckooFilterWithAverageSizeGroup;
import alluxio.client.file.cache.cuckoofilter.ConcurrentClockCuckooFilterWithLRUSizeGroup;
import alluxio.client.file.cache.cuckoofilter.ConcurrentClockCuckooFilterWithSizeGroup;
import alluxio.client.file.cache.cuckoofilter.ClockCuckooFilter;
import alluxio.client.file.cache.cuckoofilter.SlidingWindowType;
import alluxio.client.file.cache.filter.BitMapWithClockSketch;

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
          .addOption("num_unique_entries", true, "The number of unique entries.")
          .addOption("memory", true, "The memory overhead in MB.")
          .addOption("window_size", true, "The size of sliding window.")
          .addOption("clock_bits", true, "The number of bits of clock field.")
          .addOption("opportunistic_aging", true, "Enable opportunistic aging.")
          .addOption("report_file", true, "The file where reported information will be written to.")
          .addOption("report_interval", true, "The interval of reporting.")
          .addOption("size_encoding", true, "The type of size encoding.")
          .addOption("size_group_bits", true, "The number of size_groups in bits.")
          .addOption("size_bucket_bits", true, "The number of buckets in bits.");

  private static boolean mHelp;
  private static String mBenchmark;
  private static String mTrace;
  private static long mMaxEntries;
  private static int mNumUniqueEntries;
  private static double mMemoryOverhead;
  private static int mWindowSize;
  private static int mClockBits;
  private static boolean mOpportunisticAging;
  private static String mReportFile;
  private static int mReportInterval;

  private static SizeEncodingType mSizeEncodingType;
  private static int mSizeGroupBits;
  private static int mSizeBucketBits;

  private static Dataset<String> mDataset;

  private static BitMapWithClockSketch<PageId> mBitmapWithClock;
  private static ClockCuckooFilter<PageId> mClockFilter;
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
    mBenchmark = cmd.getOptionValue("benchmark", "random");
    mTrace = cmd.getOptionValue("trace", "twitter");
    mMaxEntries = Long.parseLong(cmd.getOptionValue("max_entries", "1024"));
    mNumUniqueEntries = Integer.parseInt(cmd.getOptionValue("num_unique_entries", "1024"));
    mMemoryOverhead = Double.parseDouble(cmd.getOptionValue("memory", "1.0"));
    mWindowSize = Integer.parseInt(cmd.getOptionValue("window_size", "512"));
    mClockBits = Integer.parseInt(cmd.getOptionValue("clock_bits", "2"));
    mOpportunisticAging = Boolean.parseBoolean(cmd.getOptionValue("opportunistic_aging", "true"));
    mReportFile = cmd.getOptionValue("report_file", "stdout");
    int defaultReportInterval = Math.max(1, (mWindowSize >> (mClockBits + 2)));
    mReportInterval = Integer
        .parseInt(cmd.getOptionValue("report_interval", Integer.toString(defaultReportInterval)));
    mSizeEncodingType =
        Enum.valueOf(SizeEncodingType.class, cmd.getOptionValue("size_encoding", "BULKY"));
    mSizeGroupBits = Integer.parseInt(cmd.getOptionValue("size_group_bits", "4"));
    mSizeBucketBits = Integer.parseInt(cmd.getOptionValue("size_bucket_bits", "4"));
    return true;
  }

  private static boolean init() {
    EntryGenerator<String> generator;
    switch (mBenchmark) {
      case "random":
        generator = new RandomEntryGenerator(mMaxEntries, 1, mNumUniqueEntries + 1);
        break;
      case "sequential":
        generator = new SequentialEntryGenerator(mMaxEntries, 1, mNumUniqueEntries + 1);
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
    long budgetInBits = (long) (mMemoryOverhead * Constants.MB * 8);
    long bitsPerSlot = BITS_PER_TAG + mClockBits + BITS_PER_SIZE;
    long totalSlots = budgetInBits / bitsPerSlot;
    // make sure cuckoo filter size dot not exceed the memory budget
    long expectedInsertions = (long) (Long.highestOneBit(totalSlots) * 0.955);
    if (!createCuckooFilter(expectedInsertions)) {
      return false;
    }
    createBitmapWithClock();
    mCcfAgingPeriod = mWindowSize >> mClockBits;

    // init bloom filter
    mCacheManager = new ShadowCache(mMemoryOverhead);
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

  private static boolean createCuckooFilter(long expectedInsertions) {
    SlidingWindowType slidingWindowType = SlidingWindowType.NONE;

    if (mOpportunisticAging) {
      slidingWindowType = SlidingWindowType.COUNT_BASED;
    }
    switch (mSizeEncodingType) {
      case BULKY:
        mClockFilter =
            ConcurrentClockCuckooFilter.create(ShadowCache.PageIdFunnel.FUNNEL, expectedInsertions,
                mClockBits, BITS_PER_SIZE, BITS_PER_SCOPE, slidingWindowType, mWindowSize);
        break;
      case GROUP:
        mClockFilter = ConcurrentClockCuckooFilterWithSizeGroup.create(
            ShadowCache.PageIdFunnel.FUNNEL, expectedInsertions, mClockBits, BITS_PER_SIZE,
            BITS_PER_SCOPE, slidingWindowType, mWindowSize, mSizeGroupBits);
        break;
      case AVG_GROUP:
        mClockFilter = ConcurrentClockCuckooFilterWithAverageSizeGroup.create(
            ShadowCache.PageIdFunnel.FUNNEL, expectedInsertions, mClockBits, BITS_PER_SIZE,
            BITS_PER_SCOPE, slidingWindowType, mWindowSize, mSizeGroupBits);
        break;
      case LRU_GROUP:
        mClockFilter = ConcurrentClockCuckooFilterWithLRUSizeGroup.create(
            ShadowCache.PageIdFunnel.FUNNEL, expectedInsertions, mClockBits, BITS_PER_SIZE,
            BITS_PER_SCOPE, slidingWindowType, mWindowSize, mSizeGroupBits, mSizeBucketBits);
        break;
      default:
        return false;
    }
    return true;
  }

  private static boolean createBitmapWithClock(){
    long budgetInBits = (long)mMemoryOverhead * Constants.MB * 8;
    int bitsPerSlot =  mClockBits + BITS_PER_SIZE;
    long totalSlots = budgetInBits / bitsPerSlot;
    int clockWidth = (int)totalSlots;
    if(clockWidth<0){
      return false;
    }
    mBitmapWithClock = new BitMapWithClockSketch<PageId>(mWindowSize, 1, mClockBits, clockWidth, BITS_PER_SIZE, ShadowCache.PageIdFunnel.FUNNEL);
    return true;
  }
  private static void usage() {
    new HelpFormatter().printHelp(String.format(
        "java -cp <JAR>> %s -benchmark <[random, seq, msr, twitter]> -trace <path> -max_entries <entries> "
            + "-memory <memory> -window_size <window_size> -clock_bits <clock_bits> ...",
        ConcurrentBenchmark.class.getCanonicalName()),
        "run a mini benchmark to write or read a file", OPTIONS, "", true);
  }

  private static void printArguments() {
    System.out.printf(
        "-benchmark %s -trace %s -max_entries %d -memory %f -window_size %d -num_unique_entries %d -clock_bits %d -opportunistic_aging %b -size_encoding %s\n",
        mBenchmark, mTrace, mMaxEntries, mMemoryOverhead, mWindowSize, mNumUniqueEntries,
        mClockBits, mOpportunisticAging, mSizeEncodingType.toString());
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

    long opsCount = 0;
    long totalDuration = 0;
    long ccfAgingDuration = 0, mbfAgingDuration = 0, bmcAgingDuration = 0;
    long ccfFilterDuration = 0, mbfFilterDuration = 0, bmcFilterDuration = 0;
    long ccfAgingCount = 0, mbfAgingCount = 0, bmcAgingCount = 0;
    double ccfError = .0, mbfError = .0 ,bmcError = .0;
    double ccfByteError = .0, mbfByteError = .0, bmcByteError = .0;
    long errCnt = 0;
    long stackTick = System.currentTimeMillis();
    mStream.println("#operation\tReal\tReal(bytes)\tMBF\tMBF(bytes)\tCCF\tCCF(bytes)\tBMC\tBMC(bytes)");
    while (mDataset.hasNext() && opsCount < mMaxEntries) {
      opsCount++;
      DatasetEntry<String> entry = mDataset.next();

      PageId item = new PageId("table1", entry.getItem().hashCode());
      long startFilterTick = System.currentTimeMillis();
      if (!mClockFilter.mightContainAndResetClock(item, entry.getSize(), entry.getScopeInfo())) {
        mClockFilter.put(item, entry.getSize(), entry.getScopeInfo());
      }
      mClockFilter.increaseOperationCount(1);
      ccfFilterDuration += System.currentTimeMillis() - startFilterTick;

      byte[] page = new byte[entry.getSize()];
      long startBFTick = System.currentTimeMillis();
      mCacheManager.put(item, page, mContext);
      mbfFilterDuration += System.currentTimeMillis() - startBFTick;

      long bmcStart = System.currentTimeMillis();
      mBitmapWithClock.put(item,entry.getSize());
      bmcFilterDuration += System.currentTimeMillis() - bmcStart;

      bmcStart = System.currentTimeMillis();
      bmcAgingCount++;
      mBitmapWithClock.updateClock(1);
      bmcAgingDuration += System.currentTimeMillis() - bmcStart;

      // Aging CCF
      if (opsCount % mCcfAgingPeriod == 0) {
        ccfAgingCount++;
        long startAgingTick = System.currentTimeMillis();
        mClockFilter.aging();
        ccfAgingDuration += System.currentTimeMillis() - startAgingTick;
      }

      // Aging BF
      if (opsCount % mBfAgingPeriod == 0) {
        mbfAgingCount++;
        long startAgingTick = System.currentTimeMillis();
        mCacheManager.switchBloomFilter();
        mbfAgingDuration += System.currentTimeMillis() - startAgingTick;
      }

      // report
      if (opsCount % mReportInterval == 0) {
        mCacheManager.updateWorkingSetSize();
        long realNum = mDataset.getRealEntryNumber();
        long realByte = mDataset.getRealEntrySize();
        long mbfNum = mCacheManager.getShadowCachePages();
        long mbfByte = mCacheManager.getShadowCacheBytes();
        long bmcNum = (long)mBitmapWithClock.getItemNum();
        long bmcByte = mBitmapWithClock.getWorkSetSize();
        long ccfNum = mClockFilter.getItemNumber();
        long ccfByte = mClockFilter.getItemSize();
        mStream.println(opsCount + "\t" + realNum + "\t" + realByte + "\t" + mbfNum + "\t" + mbfByte
            + "\t" + ccfNum + "\t" + ccfByte + "\t" +bmcNum +"\t" +bmcByte);
        // accumulate error
        errCnt++;
        mbfError += Math.abs(mbfNum / (double) realNum - 1.0);
        mbfByteError += Math.abs(mbfByte / (double) realByte - 1.0);
        ccfError += Math.abs(ccfNum / (double) realNum - 1.0);
        ccfByteError += Math.abs(ccfByte / (double) realByte - 1.0);
        bmcError += Math.abs(bmcNum / (double) realNum - 1.0);
        bmcByteError += Math.abs(bmcByte/(double)realByte - 1.0);
      }
    }
    totalDuration = (System.currentTimeMillis() - stackTick);
    System.out.println();
    System.out.println("TotalTime(ms)\t" + totalDuration);
    System.out.println();
    System.out.println(
        "Type\tPut/Get(ms)\tAging(ms)\tAgingCnt\tops/sec\tops/sec(aging)\tErr(Num)\tErr(Byte)\n");
    System.out.printf("MBF\t%d\t%d\t%d\t%.2f\t%.2f\t%.4f%%\t%.4f%%\n", mbfFilterDuration,
        mbfAgingDuration, mbfAgingCount, opsCount * 1000 / (double) mbfFilterDuration,
        opsCount * 1000 / (double) (mbfFilterDuration + mbfAgingDuration), mbfError * 100 / errCnt,
        mbfByteError * 100 / errCnt);
    System.out.printf("CCF\t%d\t%d\t%d\t%.2f\t%.2f\t%.4f%%\t%.4f%%\n", ccfFilterDuration,
        ccfAgingDuration, ccfAgingCount, opsCount * 1000 / (double) ccfFilterDuration,
        opsCount * 1000 / (double) (ccfFilterDuration + ccfAgingDuration), ccfError * 100 / errCnt,
        ccfByteError * 100 / errCnt);
    System.out.printf("BMC\t%d\t%d\t%d\t%.2f\t%.2f\t%.4f%%\t%.4f%%\n", bmcFilterDuration,
            bmcAgingDuration, bmcAgingCount, opsCount * 1000 / (double) bmcFilterDuration,
            opsCount * 1000 / (double) (bmcFilterDuration + bmcAgingDuration), bmcError * 100 / errCnt,
            bmcByteError * 100 / errCnt);
  }

  enum SizeEncodingType {
    BULKY, GROUP, AVG_GROUP, LRU_GROUP
  }
}
