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

package alluxio.client.file.cache;

import alluxio.client.file.cache.cuckoofilter.SlidingWindowType;
import alluxio.client.file.cache.cuckoofilter.size.SizeEncodeType;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;

public class ShadowCacheParameters extends Parameters {
  @Parameter(names = "--shadow_cache")
  public String mShadowCacheType = "CCF";

  @Parameter(names = "--memory")
  public String mMemoryBudget = "1MB";

  @Parameter(names = "--window_type", converter = SlidingWindowTypeConverter.class)
  public SlidingWindowType mSlidingWindowType = SlidingWindowType.COUNT_BASED;

  @Parameter(names = "--window_size")
  public long mWindowSize = 65536;

  @Parameter(names = "--clock_bits")
  public int mClockBits = 4;

  @Parameter(names = "--fpr")
  public double mFpr = 0.01;

  // clock cuckoo specified parameters
  @Parameter(names = "--size_bits")
  public int mSizeBits = 20;

  @Parameter(names = "--scope_bits")
  public int mScopeBits = 8;

  @Parameter(names = "--opportunistic_aging", arity = 1)
  public boolean mOpportunisticAging = true;

  @Parameter(names = "--tag_bits")
  public int mTagBits = 8;

  @Parameter(names = "--size_encode", converter = SizeEncodeTypeConverter.class)
  public SizeEncodeType mSizeEncodeType = SizeEncodeType.NONE;

  @Parameter(names = "--num_size_bucket_bits")
  public int mNumSizeBucketBits = 8;

  @Parameter(names = "--size_bucket_bits")
  public int mSizeBucketBits = 12;

  // multiple bloom filter specified parameters
  @Parameter(names = "--num_blooms")
  public int mNumBloom = 4;

  // String key + page index
  @Parameter(names = "--page_bits")
  public int mPageBits = 8 * 16 + 64;

  public int mAgeLevels = 0;

  static class SlidingWindowTypeConverter implements IStringConverter<SlidingWindowType> {
    @Override
    public SlidingWindowType convert(String s) {
      return SlidingWindowType.valueOf(s.toUpperCase());
    }
  }

  static class SizeEncodeTypeConverter implements IStringConverter<SizeEncodeType> {
    @Override
    public SizeEncodeType convert(String s) {
      return SizeEncodeType.valueOf(s.toUpperCase());
    }
  }
}
