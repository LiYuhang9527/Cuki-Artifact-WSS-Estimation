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

public class BitMapWithClockSketch<T> implements Serializable {
  protected static int mClockWidth;
  protected static int mBitsPerSize;
  protected static int mBitsPerClock;
  protected int mWindowSize;
  protected int mHashNum;
  protected int lastUpdateIdx;
  protected int updateLen;
  protected Funnel<? super T> mFunnel;

  protected List<HashFunction> hashFuncs = new LinkedList<HashFunction>();
  protected int[] clock;
  protected int[] traceSize;
  long workSetSize = 0;
  double missNum;
  double queryNum;

  /**
   * @param windowSize : 滑动窗口大小
   * @param hashNum: 使用的hash函数个数
   * @param bitsPerClock: 时钟每个单元格的大小，单位是bit
   * @param bitsPerSize : 每个单元格为记录size所需要的bits
   * @param clockWidth: 时钟总单元格个数
   */
  public BitMapWithClockSketch(int windowSize, int hashNum, int bitsPerClock, int clockWidth,
      int bitsPerSize, Funnel<? super T> funnel) {
    mBitsPerClock = bitsPerClock;
    mBitsPerSize = bitsPerSize;
    mWindowSize = windowSize;
    mFunnel = funnel;
    mHashNum = hashNum;
    mClockWidth = clockWidth;
    for (int i = 0; i < hashNum; i++) {
      hashFuncs.add(Hashing.murmur3_32(i + 10086));
    }
    updateLen = ((1 << mBitsPerClock) - 2) * mClockWidth / mWindowSize;
    clock = new int[clockWidth];
    traceSize = new int[clockWidth];
  }

  public static <T> BitMapWithClockSketch<T> create(int windowSize, int memory, int hashNum,
      int bitsPerClock, int bitsPerSize, Funnel<? super T> funnel) {
    return new BitMapWithClockSketch<T>(windowSize, hashNum, bitsPerClock,
        memory / (bitsPerClock + bitsPerSize), bitsPerSize, funnel);
  }

  public void put(T trace, int size) {
    queryNum++;
    for (HashFunction hashFunc : hashFuncs) {
      int pos =
          Math.abs(hashFunc.newHasher().putObject(trace, mFunnel).hash().asInt() % mClockWidth);
      if (clock[pos] == 0) {
        missNum++;
        traceSize[pos] = size;
        workSetSize += traceSize[pos];
      }
      clock[pos] = (1 << mBitsPerClock) - 1;
    }
  }

  public boolean get(T trace) {
    for (HashFunction hashFunc : hashFuncs) {
      int pos =
          Math.abs(hashFunc.newHasher().putObject(trace, mFunnel).hash().asInt() % mClockWidth);
      if (clock[pos] == 0) {
        return false;
      }
    }
    return true;
  }

  public void aging() {
    for (int i = 0; i < mClockWidth; i++) {
      clock[i] -= 1;
      if (clock[i] == 0) {
        workSetSize -= traceSize[i];
        traceSize[i] = 0;
      }
    }
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

  public long getWorkSetSize() {
    return workSetSize;
  }

  public double getHitRate() {
    return 1 - missNum / queryNum;
  }

  public double getItemNum() {
    double u = 0;
    for (int i = 0; i < mClockWidth; ++i)
      u += clock[i] == 0 ? 1 : 0;
    return -mClockWidth / (double) mHashNum * Math.log(u / mClockWidth); // 源码里并没有/hashNum，原作者也没考虑
  }

}
