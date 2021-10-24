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

package alluxio.client.file.cache.benchmark.concurrency;

import alluxio.Constants;
import alluxio.client.file.cache.dataset.Dataset;
import alluxio.client.file.cache.dataset.GeneralDataset;
import alluxio.client.file.cache.dataset.generator.RandomIntegerEntryGenerator;
import alluxio.client.file.cache.cuckoofilter.ScopedClockCuckooFilter;
import com.google.common.hash.Funnels;

import java.util.ArrayList;

public class ConcurrentBench<T> extends Thread {
  private static final int NUM_UNIQUE_ENTRY = 10 * Constants.MB;
  private static final int BITS_PER_CLOCK = 8;
  private static final int BITS_PER_SIZE = 20;
  private static final int BITS_PER_SCOPE = 8;
  private static long NUM_ENTRIES = 10 * Constants.MB;
  private static int WINDOW_SIZE = 64 * Constants.KB;

  public static void main(String[] args) {
    // parse arguments
    long totalOps = NUM_ENTRIES;
    int numThreads = 4;
    long opsPerThread = totalOps / numThreads;
    ScopedClockCuckooFilter<Integer> clockFilter = ScopedClockCuckooFilter.create(
        Funnels.integerFunnel(), NUM_UNIQUE_ENTRY, BITS_PER_CLOCK, BITS_PER_SIZE, BITS_PER_SCOPE);
    System.out.println(clockFilter.getSummary());
    ClientState state = new ClientState();
    state.numThreads.set(numThreads);
    ArrayList<ClientThread<Integer>> workers = new ArrayList<>();
    for (int i = 0; i < numThreads; i++) {
      Dataset<Integer> dataset = new GeneralDataset<>(new RandomIntegerEntryGenerator(NUM_ENTRIES,
          0, NUM_UNIQUE_ENTRY, 0, (1 << BITS_PER_SIZE) - 1, 1, 32173 + i), WINDOW_SIZE);
      workers.add(new ClientThread<>(i, clockFilter, dataset, opsPerThread, state));
    }
    long st = System.currentTimeMillis();
    for (int i = 0; i < numThreads; i++) {
      workers.get(i).start();
    }

    for (int i = 0; i < numThreads; i++) {
      try {
        workers.get(i).join();
      } catch (Exception e) {
        // ignored
      }
    }
    long en = System.currentTimeMillis();
    long totalDuration = en - st;
    System.out.println("Cost " + totalDuration + " ms");
    System.out.println(state);
  }
}
