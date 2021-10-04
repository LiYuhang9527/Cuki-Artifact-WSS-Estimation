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

import alluxio.client.file.cache.dataset.Dataset;
import alluxio.client.file.cache.dataset.DatasetEntry;
import alluxio.client.file.cache.filter.ScopedClockCuckooFilter;

public class ClientThread<T> extends Thread {
  private final int threadId;
  private final ScopedClockCuckooFilter<T> filter;
  private final Dataset<T> dataset;
  private final long numOps;
  private final ClientState state;

  public ClientThread(int threadId, ScopedClockCuckooFilter<T> filter, Dataset<T> dataset,
      long numOps, ClientState state) {
    this.threadId = threadId;
    this.filter = filter;
    this.dataset = dataset;
    this.numOps = numOps;
    this.state = state;
  }

  @Override
  public void run() {
    long st = System.currentTimeMillis();
    for (int i = 0; i < numOps; i++) {
      DatasetEntry<T> entry = dataset.next();
      state.numOps.incrementAndGet();
      if (!filter.mightContainAndResetClock(entry.getItem())) {
        state.numInsert.incrementAndGet();
        filter.put(entry.getItem(), entry.getSize(), entry.getScopeInfo());
      }
    }
    long en = System.currentTimeMillis();
    state.elapsedTime.addAndGet(en - st);
  }
}
