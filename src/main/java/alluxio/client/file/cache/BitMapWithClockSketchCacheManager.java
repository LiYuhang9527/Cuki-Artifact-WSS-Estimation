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

import java.util.LinkedList;
import java.util.List;

public class BitMapWithClockSketchCacheManager implements ShadowCache {
  protected static int mClockWidth;
  protected static int mBitsPerSize;
  protected static int mBitsPerClock;
  protected int mBitsPerScope;
  protected long mWindowSize;
  protected long mHashNum;
  protected int lastUpdateIdx;
  protected int updateLen;
  protected Funnel<PageId> mFunnel;


  protected List<HashFunction> hashFuncs = new LinkedList<HashFunction>();
  protected int[] clock;
  protected int[] traceSize;
  // TODO - SCOPE 存储重复，能不能从某一个位置开始都存某个scope的pageid
  protected CacheScope[] scopes;
  private long workSetSize;
  private long missNum;
  private long hitSize;
  private long queryNum;
  private long querySize;



  public BitMapWithClockSketchCacheManager(ShadowCacheParameters parameters) {
    mBitsPerClock = parameters.mClockBits;
    mBitsPerSize = parameters.mSizeBits;
    mWindowSize = parameters.mWindowSize;
    mBitsPerScope = parameters.mScopeBits;
    mFunnel = PageIdFunnel.FUNNEL;
    mHashNum = 1;
    long memoryInBits = FormatUtils.parseSpaceSize(parameters.mMemoryBudget) * 8;
    mClockWidth = (int) (memoryInBits / (parameters.mClockBits + parameters.mSizeBits + parameters.mScopeBits));
    for (int i = 0; i < mHashNum; i++) {
      hashFuncs.add(Hashing.murmur3_32(i + 10086));
    }
    updateLen = (int)(((1 << mBitsPerClock) - 2) * mClockWidth / mWindowSize);
    clock = new int[mClockWidth];
    traceSize = new int[mClockWidth];
    scopes = new CacheScope[mClockWidth];
  }

  @Override
  public boolean put(PageId pageId, int size, CacheScope scope) {
    queryNum++;
    querySize+=size;
    for (HashFunction hashFunc : hashFuncs) {
      int pos = Math.abs(hashFunc.newHasher().putObject(pageId, mFunnel).hash().asInt() % mClockWidth);
      if (clock[pos] == 0) {
        missNum++;
        traceSize[pos] = size;
        scopes[pos] = scope;
        workSetSize += traceSize[pos];
      }else{
        hitSize+=size;
      }
      clock[pos] = (1 << mBitsPerClock) - 1;
    }
    return true;
  }

  @Override
  public int get(PageId pageId, int bytesToRead, CacheScope scope) {
    for (HashFunction hashFunc : hashFuncs) {
      int pos =
              Math.abs(hashFunc.newHasher().putObject(pageId, mFunnel).hash().asInt() % mClockWidth);
      if (clock[pos] == 0) {
        return 0;
      }
    }
    return 1;
  }

  @Override
  public boolean delete(PageId pageId) {
    for (HashFunction hashFunc : hashFuncs) {
      int pos =
              Math.abs(hashFunc.newHasher().putObject(pageId, mFunnel).hash().asInt() % mClockWidth);
      clock[pos] =  0;
    }
    return false;
  }


  public void aging() {
    for (int i = 0; i < mClockWidth; i++) {
      if(clock[i]>0){
        clock[i] -= 1;
      }
      if (clock[i] == 0) {
        workSetSize -= traceSize[i];
        traceSize[i] = 0;
      }
    }
  }

  @Override
  public void updateWorkingSetSize() {
    return;
  }

  @Override
  public void stopUpdate() {

  }

  @Override
  public void updateTimestamp(long increment) {

  }

  @Override
  public long getShadowCachePages() {
    double u = 0;
    for (int i = 0; i < mClockWidth; ++i)
      u += clock[i] == 0 ? 1 : 0;
    if(u==.0){
      return 0;
    }
    double pages = -mClockWidth / (double) mHashNum * Math.log(u / mClockWidth);
    return (long)(pages); // 源码里并没有/hashNum，原作者也没考虑
  }

  @Override
  public long getShadowCachePages(CacheScope scope) {
    int scopeNum = 0;
    for(int i=0; i<mClockWidth; ++i){
      if(scopes[i].equals(scope)){
        scopeNum++;
      }
    }
    return scopeNum;
  }

  @Override
  public long getShadowCacheBytes() {
    return workSetSize;
  }

  @Override
  public long getShadowCacheBytes(CacheScope scope) {
    long scopeSize = 0;
    for(int i=0;i<mClockWidth;i++){
      if(scopes[i].equals(scope)){
        scopeSize+=traceSize[i];
      }
    }
    return scopeSize;
  }

  @Override
  public long getShadowCachePageRead() {
    return queryNum;
  }

  @Override
  public long getShadowCachePageHit() {
    return queryNum-missNum;
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
    return mClockWidth*(mBitsPerScope+mBitsPerClock+mBitsPerSize);
  }

  @Override
  public String getSummary() {
    return "bitmapWithClockSketch\nbitsPerClock: " + mBitsPerClock
            + "\nbitsPerSize: " + mBitsPerSize + "\nbitsPerScope: " + mBitsPerScope + "\nSizeInMB: "
            + (mClockWidth * mBitsPerClock / 8.0 / Constants.MB
            + mClockWidth * mBitsPerSize / 8.0 / Constants.MB
            + mClockWidth * mBitsPerScope / 8.0 / Constants.MB);
  }

  public void updateClock(int insertTimesPerUpdate) {
    int temp = updateLen * insertTimesPerUpdate;
    int subAll = temp / mClockWidth;
    int len = temp % mClockWidth;

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
    while (beg < end) {
      if (clock[beg] <= val) {
        clock[beg] = 0;
        workSetSize -= traceSize[beg];
        traceSize[beg] = 0;
      } else {
        clock[beg] -= val; // 忽然想到老化
      }
      beg++;
    }
  }

}
