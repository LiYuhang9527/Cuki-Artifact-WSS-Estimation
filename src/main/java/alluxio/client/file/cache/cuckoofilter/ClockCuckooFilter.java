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

package alluxio.client.file.cache.cuckoofilter;

import alluxio.client.quota.CacheScope;

public interface ClockCuckooFilter<T> {

  public boolean put(T item, int size, CacheScope scopeInfo);

  public boolean mightContainAndResetClock(T item);

  public boolean mightContainAndResetClock(T item, int size, CacheScope scopeInfo);

  public boolean mightContain(T item);

  public boolean delete(T item);

  public void aging();

  public int getAge(T item);

  public String getSummary();

  public double expectedFpp();

  public int getItemNumber();

  public int getItemNumber(CacheScope scopeInfo);

  public int getItemSize();

  public int getItemSize(CacheScope scopeInfo);

  public void increaseOperationCount(int count);

  public int getNumBuckets();

  public int getTagsPerBucket();

  public int getBitsPerTag();

  public int getBitsPerClock();

  public int getBitsPerSize();

  public int getBitsPerScope();
}
