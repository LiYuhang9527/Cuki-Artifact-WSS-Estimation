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

package alluxio.client.file.cache.example;

import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.ShadowCache;
import com.github.mgunlogson.cuckoofilter4j.CuckooFilter;
import com.github.mgunlogson.cuckoofilter4j.Utils;

public class CuckooExample {
  public static void main(String[] args) {
    CuckooFilter<PageId> filter = new CuckooFilter.Builder<>(ShadowCache.PageIdFunnel.FUNNEL, 10000)
        .withFalsePositiveRate(0.01).withHashAlgorithm(Utils.Algorithm.Murmur3_32).build();
    for (int i = 0; i < 1000; i++) {
      PageId page = new PageId("1L", i);
      filter.put(page);
      if (!filter.mightContain(page)) {
        System.out.println("error");
      }
    }
    System.out.println(filter.getCount());

    PageId toDelete = new PageId("1L", 5);
    filter.delete(toDelete);
    System.out.println(filter.mightContain(toDelete));
  }
}
