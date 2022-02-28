package alluxio.client.file.cache;

import alluxio.Constants;
import alluxio.client.quota.CacheScope;
import alluxio.util.FormatUtils;
import alluxio.util.TinyTable.TinyTable;
import alluxio.util.TinyTable.TinyTableWithCounters;

import com.google.common.hash.Funnel;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class SWAMPSketchShadowCacheManager implements ShadowCache{
  protected long mWindowSize;
  protected int mFingerPrintSize;
  protected int[] cyclicFingerBuffer;
  protected int mBucketCapacity;
  protected int mBitsPerSize;
  protected int mBucketNum;
  protected double mLoadF; //said "recommend this be 0.2"
  protected HashFunction mHashFunction;
  protected Funnel<PageId> mFunnel;
  protected int curIdx;
  protected long deleteTime;
  protected long addTime;
  private final AtomicLong mShadowCachePageRead = new AtomicLong(0);
  private final AtomicLong mShadowCachePageHit = new AtomicLong(0);
  private final AtomicLong mShadowCacheByteRead = new AtomicLong(0);
  private final AtomicLong mShadowCacheByteHit = new AtomicLong(0);
  private final AtomicLong mBucketsSet = new AtomicLong(0);
  private final AtomicLong mTotalSize = new AtomicLong(0);
  protected boolean debugMode;

  protected Map<Integer,Integer> debugMapPageToCode;
  protected Set<Integer> debugCodeSet;

  // no scope
  protected TinyTable tinyTable;
  public SWAMPSketchShadowCacheManager(ShadowCacheParameters params) {
    mWindowSize = params.mWindowSize;
    mFingerPrintSize = params.mTagBits;
    mBucketCapacity = params.mTagsPerBucket;
    mBitsPerSize = params.mSizeBits;
    long memoryInBits = FormatUtils.parseSpaceSize(params.mMemoryBudget) * 8;
    if(memoryInBits<(mWindowSize*mFingerPrintSize*2)){
      System.out.println("[+] swamp‘s memory too small!, at least"+mWindowSize*mFingerPrintSize*2/8.0/Constants.MB+"MB");
      System.exit(-1);
    }
    mBucketNum = (int)((memoryInBits-(mWindowSize*mFingerPrintSize)) / (mBucketCapacity*(mFingerPrintSize+mBitsPerSize)+136));
    mLoadF = mBucketCapacity/(mWindowSize/(double)mBucketNum);
    tinyTable = new TinyTable(mFingerPrintSize,mBitsPerSize,mBucketCapacity,mBucketNum);
    cyclicFingerBuffer = new int[(int)mWindowSize];
    mHashFunction = Hashing.murmur3_32((int)System.currentTimeMillis());
    mFunnel = PageIdFunnel.FUNNEL;
    debugMapPageToCode = new HashMap<>();
    debugCodeSet =  new HashSet<>();
    debugMode = false;
  }

  private void info(String s){
    if(debugMode){
      System.out.print(s);
    }
  }



  private void updateCurIdx(){
    if(curIdx==mWindowSize-1){
      curIdx = 0;
    }else{
      curIdx = curIdx + 1;
    }
  }

  @Override
  public boolean put(PageId pageId, int size, CacheScope scope) {
    //info(String.format("try to put page:%s\n",pageId.toString()));
    int hashcode = mHashFunction.newHasher().putObject(pageId,mFunnel).hash().asInt();
    int prev = cyclicFingerBuffer[curIdx];
    if(prev!=0){
      long start = System.currentTimeMillis();
      delete(prev);
      deleteTime+= System.currentTimeMillis()-start;
    }
    long bucketNum = tinyTable.getNum(hashcode);
    //System.out.println("bucketBUm:"+bucketNum);
    if(bucketNum>=63){
      updateCurIdx();
      return false;
    }

    boolean isContain = tinyTable.containItemWithSize(hashcode);
    long start = System.currentTimeMillis();
    if(!tinyTable.addItem(hashcode,size)){
      addTime += System.currentTimeMillis()-start;;
      updateCurIdx();
      return false;
    }
    addTime += System.currentTimeMillis()-start;
    cyclicFingerBuffer[curIdx] = hashcode;
    updateCurIdx();

    if(!isContain){
      mBucketsSet.addAndGet(1);
      mTotalSize.addAndGet(size);
    }

    return true;
  }

  @Override
  public int get(PageId pageId, int bytesToRead, CacheScope scope) {
    mShadowCachePageRead.incrementAndGet();
    mShadowCacheByteRead.addAndGet(bytesToRead);
    int hashcode = mHashFunction.newHasher().putObject(pageId,mFunnel).hash().asInt();
    if(!tinyTable.containItemWithSize(hashcode)){
      return 0;
    }
    this.put(pageId,bytesToRead,scope);
    mShadowCachePageHit.incrementAndGet();
    mShadowCacheByteHit.addAndGet(bytesToRead);
    return bytesToRead;
  }


  public boolean delete(int hashcode){
    long prevSize = tinyTable.getItemSize(hashcode);
    if(prevSize!=-1) {
      tinyTable.RemoveItem(hashcode,(int)prevSize);
      if (!tinyTable.containItemWithSize(hashcode)) {
        mTotalSize.addAndGet(-prevSize);
        mBucketsSet.addAndGet(-1);
      }
      cyclicFingerBuffer[curIdx] = 0;
      return true;
    }
    return false;
  }
  @Override
  public boolean delete(PageId pageId) {

    return false;
  }

  @Override
  public void aging() {

  }

  @Override
  public void updateWorkingSetSize() {

  }

  @Override
  public void stopUpdate() {

  }

  @Override
  public void updateTimestamp(long increment) {

  }

  @Override
  public long getShadowCachePages() {
    return mBucketsSet.get();
  }

  @Override
  public long getShadowCachePages(CacheScope scope) {
    return 0;
  }

  @Override
  public long getShadowCacheBytes() {
    return mTotalSize.get();
  }

  @Override
  public long getShadowCacheBytes(CacheScope scope) {
    return 0;
  }

  @Override
  public long getShadowCachePageRead() {
    return mShadowCachePageRead.get();
  }

  @Override
  public long getShadowCachePageHit() {
    return mShadowCachePageHit.get();
  }

  @Override
  public long getShadowCacheByteRead() {
    return mShadowCacheByteRead.get();
  }

  @Override
  public long getShadowCacheByteHit() {
    return mShadowCacheByteHit.get();
  }

  @Override
  public double getFalsePositiveRatio() {
    return 0;
  }

  @Override
  public long getSpaceBits() {
    // maybe not right, we should check this !
    return mFingerPrintSize*mWindowSize+ (long)mBucketNum*mBucketCapacity*(mFingerPrintSize+mBitsPerSize)+mBucketNum*136L;
  }

  @Override
  public String getSummary() {
    return "SWAMP: \nbitsPerTag: " + mFingerPrintSize
        + "\nbucketNum: "+ mBucketNum
        + "\nSizeInMB: " + getSpaceBits() / 8.0 / Constants.MB
        + "\nDeleteTime: " +deleteTime
        + "\nTinyAddTime: " +addTime
        + "\nfindEmptyTime" +tinyTable.findEmptyTime
        + "\nscaleUpTime: "+tinyTable.addItemTime;
  }
}
