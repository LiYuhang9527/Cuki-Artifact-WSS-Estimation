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

package alluxio.client.file.cache.filter;

import com.google.common.hash.Funnel;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import static java.lang.Math.min;

public class BitMapWithSlidingSketch<T> implements Serializable {
  protected static int mClockWidth;
  protected static int mBitsPerSize;
  protected static int mBitsPerClock;
  protected int mWindowSize;
  protected int mHashNum;
  protected int lastUpdateIdx;
  protected int updateLen;
  protected Funnel<? super T> mFunnel;
  protected List<HashFunction> hashFuncs = new LinkedList<HashFunction>();
  protected int[] traceSizeNew;
  protected int[] traceSizeOld;
  double missNum;
  double queryNum;

  /**
   * @param windowSize : 滑动窗口大小
   * @param hashNum: 使用的hash函数个数
   * @param bitsPerSize : 每个单元格为记录size所需要的bits
   * @param clockWidth: 时钟总单元格个数
   */
  public BitMapWithSlidingSketch(int windowSize, int hashNum, int clockWidth, int bitsPerSize,
      Funnel<? super T> funnel) {
    mBitsPerSize = bitsPerSize;
    mWindowSize = windowSize;
    mFunnel = funnel;
    mHashNum = hashNum;
    mClockWidth = clockWidth;
    for (int i = 0; i < hashNum; i++) {
      hashFuncs.add(Hashing.murmur3_32(i + 10086));
    }
    updateLen = mClockWidth / mWindowSize;
    traceSizeNew = new int[clockWidth];
    traceSizeOld = new int[clockWidth];
  }

  public static <T> BitMapWithSlidingSketch<T> create(int windowSize, int memory, int hashNum,
      int bitsPerSize, Funnel<? super T> funnel) {
    return new BitMapWithSlidingSketch<>(windowSize, hashNum, memory / (bitsPerSize * 2),
        bitsPerSize, funnel);
  }

  public void put(T trace, int size) {
    queryNum++;
    for (HashFunction hashFunc : hashFuncs) {
      int pos =
          Math.abs(hashFunc.newHasher().putObject(trace, mFunnel).hash().asInt() % mClockWidth);
      traceSizeNew[pos] = size;
      // TODO TRANCEOLD[pos] = 0
    }
  }

  public boolean get(T trace) {
    for (HashFunction hashFunc : hashFuncs) {
      int pos =
          Math.abs(hashFunc.newHasher().putObject(trace, mFunnel).hash().asInt() % mClockWidth);
      if (traceSizeNew[pos] == 0 && traceSizeOld[pos] == 0) {
        return false;
      }
    }
    return true;
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

  public double getWorkSetSize() {
    double workSetSize = 0;
    for (int i = 0; i < mClockWidth; i++) {
      // TODO- 合并策略
      double traceSize = (traceSizeNew[i] + traceSizeOld[i]) / 2.0;
      workSetSize += traceSize;
    }
    return workSetSize;
  }

  public double getHitRate() {
    return 1 - missNum / queryNum;
  }

  double getItemNum() {
    double u = 0;
    for (int i = 0; i < mClockWidth; ++i)
      if (traceSizeOld[i] == 0 && traceSizeNew[i] == 0) {
        u++;
      }
    return -mClockWidth / (double) mHashNum * Math.log(u / mClockWidth); // 源码里并没有/hashNum，原作者也没考虑
  }

}
