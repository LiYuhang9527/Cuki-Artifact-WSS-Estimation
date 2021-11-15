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

import static java.lang.Math.min;

import alluxio.Constants;
import alluxio.client.quota.CacheScope;
import alluxio.util.FormatUtils;

import com.google.common.hash.Funnel;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class BitMapWithSlidingSketchShadowCacheManager implements ShadowCache {
  protected int mClockWidth;
  protected int mBitsPerSize;
  protected int mBitsPerScope;
  protected long mWindowSize;
  protected int mHashNum;
  protected int lastUpdateIdx;
  protected long updateLen;
  protected Funnel<PageId> mFunnel;
  protected List<HashFunction> hashFuncs = new LinkedList<HashFunction>();
  protected int[] traceSizeNew;
  protected int[] traceSizeOld;
  protected CacheScope[] traceScopeNew;
  protected CacheScope[] traceScopeOld;
  private long hitNum;
  private long queryNum;
  private long querySize;
  private long hitSize;
  private long realNum;

  public BitMapWithSlidingSketchShadowCacheManager(ShadowCacheParameters params) {
    mBitsPerSize = params.mSizeBits;
    mWindowSize = params.mWindowSize;
    mFunnel = PageIdFunnel.FUNNEL;
    mBitsPerScope = params.mScopeBits;
    mHashNum = 1;
    long memoryInBits = FormatUtils.parseSpaceSize(params.mMemoryBudget) * 8;
    mClockWidth = (int)(memoryInBits / ((mBitsPerSize+params.mScopeBits) * 2));
    for (int i = 0; i < mHashNum; i++) {
      hashFuncs.add(Hashing.murmur3_32(i + 10086));
    }
    updateLen = mClockWidth / mWindowSize;
    traceSizeNew = new int[mClockWidth];
    traceSizeOld = new int[mClockWidth];
    traceScopeNew = new CacheScope[mClockWidth];
    traceScopeOld = new CacheScope[mClockWidth];
  }

  @Override
  public boolean put(PageId pageId, int size, CacheScope scope) {
    queryNum++;
    querySize+=size;
    for (HashFunction hashFunc : hashFuncs) {
      int pos = Math.abs(hashFunc.newHasher().putObject(pageId, mFunnel).hash().asInt() % mClockWidth);
      if(traceSizeNew[pos]!=0){
        hitNum++;
        hitSize+=size;
      }
      traceSizeNew[pos] = size;
      traceScopeNew[pos] = scope;
      //traceSizeOld[pos] = 0;
      //traceScopeOld[pos] = null;
    }
    return true;
  }
  @Override
  public int get(PageId pageId, int bytesToRead, CacheScope scope) {
    queryNum++;
    boolean found = true;
    for (HashFunction hashFunc : hashFuncs) {
      int pos = Math.abs(hashFunc.newHasher().putObject(pageId, mFunnel).hash().asInt() % mClockWidth);
      if(traceSizeNew[pos]==0&&traceSizeOld[pos]==0){
        found = false;
        break;
      }
    }
    if(found){
      hitNum++;
      return 1;
    }else{
      return 0;
    }
  }

  @Override
  public void aging() {
    updateClock(1);
  }

  public void updateClock(int insertTimesPerUpdate) {
    long temp = updateLen * insertTimesPerUpdate;
    int subAll = (int)(temp / mClockWidth);
    int len = (int)(temp % mClockWidth);

    int beg = lastUpdateIdx;
    int end = min(mClockWidth, lastUpdateIdx + len);
    // 处理subAll圈
    updateRange(beg, end, subAll + 1);
    if (end - beg < len) {
      end = len - (end - beg);
      beg = 0;
      updateRange(beg, end, subAll + 1);
    }
    // 处理subAll圈
    // 按圈处理 比for要快
    if (end > lastUpdateIdx) {
      updateRange(end, mClockWidth, subAll);
      updateRange(0, lastUpdateIdx, subAll);
    } else {
      updateRange(end, lastUpdateIdx, subAll);
    }
    lastUpdateIdx = end;
  }

  public void updateRange(int beg, int end, int val) {
    if (val == 0) {
      return;
    }
    while (beg < end) {
      if (val > 1) {
        traceSizeOld[beg] = 0;
      } else {
        traceSizeOld[beg] = traceSizeNew[beg];
      }
      traceSizeNew[beg] = 0;
      beg++;
    }
  }

  public double getHitRate() {
    return hitNum / queryNum;
  }

  @Override
  public boolean delete(PageId pageId) {
    for (HashFunction hashFunc : hashFuncs) {
      int pos = Math.abs(hashFunc.newHasher().putObject(pageId, mFunnel).hash().asInt() % mClockWidth);
      traceSizeNew[pos] = 0;
      traceScopeNew[pos] = null;
      traceSizeOld[pos] = 0;
      traceScopeOld[pos] = null;
      // TODO TRANCEOLD[pos] = 0 ?
    }
    return true;
  }

  @Override
  public void updateWorkingSetSize() { }

  @Override
  public void stopUpdate() { }

  @Override
  public void updateTimestamp(long increment) { }

  @Override
  public long getShadowCachePages() {
    double u = 0;
    for (int i = 0; i < mClockWidth; ++i){
      if (traceSizeOld[i] == 0 && traceSizeNew[i] == 0) {
        u++;
      }
    }

    realNum = (long)(-mClockWidth / (double) mHashNum * Math.log(u / mClockWidth));
    return realNum;// 源码里并没有/hashNum，原作者也没考虑
  }

  @Override
  public long getShadowCachePages(CacheScope scope) {
    return 0;
  }

  @Override
  public long getShadowCacheBytes() {
    long workSetSize = 0;
    for (int i = 0; i < mClockWidth; i++) {
      // TODO- 合并策略
      double traceSize = traceSizeNew[i] + traceSizeOld[i];
      workSetSize += traceSize;
    }
    Set<Integer> set = new HashSet<>();
    return workSetSize;
  }

  @Override
  public long getShadowCacheBytes(CacheScope scope) {
    return 0;
  }

  @Override
  public long getShadowCachePageRead() {
    return queryNum;
  }

  @Override
  public long getShadowCachePageHit() {
    return hitNum;
  }

  @Override
  public long getShadowCacheByteRead() {
    return querySize;
  }

  @Override
  public long getShadowCacheByteHit() {
    return hitSize;
  }

  @Override
  public double getFalsePositiveRatio() {
    return 0;
  }

  @Override
  public long getSpaceBits() {
    return mClockWidth*(mBitsPerScope+mBitsPerSize)*2;
  }

  @Override
  public String getSummary() {

    return "bitMapWithSliding: \nbitsPerSize: " + mBitsPerSize +  "\nbitsPerScope: " + mBitsPerScope
            + "\nSizeInMB: "
            + mClockWidth*(mBitsPerScope+mBitsPerSize)*2 / 8.0 / Constants.MB;
  }
}
