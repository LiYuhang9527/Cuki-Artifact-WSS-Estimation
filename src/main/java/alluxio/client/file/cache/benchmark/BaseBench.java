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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.client.file.cache.CacheContext;
import alluxio.Constants;
import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.ShadowCache;

public class BaseBench {
  private static final int PAGE_SIZE_BYTES = Constants.KB;
  private static final int BLOOMFILTER_NUM = 4;
  private static final PageId PAGE_ID1 = new PageId("0L", 0L);
  private static final PageId PAGE_ID2 = new PageId("1L", 1L);
  private static final byte[] PAGE1 = new byte[PAGE_SIZE_BYTES];
  private static final byte[] PAGE2 = new byte[PAGE_SIZE_BYTES];
  private static final byte[] mBuf = new byte[PAGE_SIZE_BYTES];

  public static void main(String[] args) {
    ShadowCache mCacheManager = new ShadowCache();
    CacheContext context = new CacheContext();
    mCacheManager.put(PAGE_ID1, PAGE1, context);
    mCacheManager.get(PAGE_ID1, 0, PAGE1.length, mBuf, 0, context);
    mCacheManager.updateWorkingSetSize();
    assertEquals(1, mCacheManager.getShadowCachePages());
  }

}
