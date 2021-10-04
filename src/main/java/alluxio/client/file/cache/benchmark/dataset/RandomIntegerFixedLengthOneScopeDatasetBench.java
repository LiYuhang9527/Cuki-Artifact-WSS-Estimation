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

package alluxio.client.file.cache.benchmark.dataset;

import alluxio.Constants;
import alluxio.client.file.cache.dataset.Dataset;
import alluxio.client.file.cache.dataset.RandomIntegerFixedLengthOneScopeDataset;

public class RandomIntegerFixedLengthOneScopeDatasetBench {
  private static final long NUM_ENTRY = 10 * Constants.MB;
  private static final int WINDOW_SIZE = 1 * Constants.MB;
  private static final int NUM_UNIQUE_ENTRY = 2 * Constants.MB;
  private static final int SEED = 32713;

  public static void main(String[] args) {
    Dataset<Integer> dataset = new RandomIntegerFixedLengthOneScopeDataset(NUM_ENTRY, WINDOW_SIZE,
        0, NUM_UNIQUE_ENTRY, SEED);
    long totalDuration = 0;
    long stackTick = System.currentTimeMillis();
    while (dataset.hasNext()) {
      dataset.next();
    }
    totalDuration = (System.currentTimeMillis() - stackTick);
    System.out.println("Time(ms): " + totalDuration);
  }
}
