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

import com.beust.jcommander.Parameter;

public class ShadowCacheParameters {
  @Parameter(names = "--shadow_cache")
  public String mShadowCacheType = "CCF";

  @Parameter(names = "--memory")
  public String mMemoryBudget = "1MB";

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

  // multiple bloom filter specified parameters
  @Parameter(names = "--num_blooms")
  public int mNumBloom = 4;

  public int mAgeLevels = 0;
}
