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

import alluxio.client.file.cache.dataset.generator.EntryGenerator;
import alluxio.client.file.cache.dataset.generator.MSREntryGenerator;
import alluxio.client.file.cache.dataset.generator.RandomEntryGenerator;
import alluxio.client.file.cache.dataset.generator.SequentialEntryGenerator;
import alluxio.client.file.cache.dataset.generator.TwitterEntryGenerator;
import alluxio.client.file.cache.dataset.generator.YCSBEntryGenerator;

public class BenchmarkUtils {
  public static EntryGenerator<String> createGenerator(BenchmarkParameters conf) {
    switch (conf.mDataset) {
      case "sequential":
        return new SequentialEntryGenerator(conf.mMaxEntries, 1, (int) conf.mNumUniqueEntries + 1);
      case "msr":
        return new MSREntryGenerator(conf.mTrace);
      case "twitter":
        return new TwitterEntryGenerator(conf.mTrace);
      case "ycsb":
        return new YCSBEntryGenerator(conf.mTrace);
      case "random":
      default:
        return new RandomEntryGenerator(conf.mMaxEntries, 1, (int) conf.mNumUniqueEntries + 1, 1,
            1024, 1, 32749);
    }
  }
}
