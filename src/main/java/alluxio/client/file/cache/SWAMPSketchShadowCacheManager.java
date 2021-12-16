package alluxio.client.file.cache;

import alluxio.Constants;
import alluxio.client.quota.CacheScope;
import alluxio.util.FormatUtils;
import alluxio.util.TinyTable.RankIndexingTechnique.RankIndexingTechnique;
import alluxio.util.TinyTable.TinyTable;
import alluxio.util.TinyTable.TinyTableWithCounters;

import com.google.common.hash.Funnel;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SWAMPSketchShadowCacheManager implements ShadowCache{
  protected long mWindowSize;
  protected int mFingerPrintSize;
  protected int[] cyclicFingerBuffer;
  protected int mBucketCapacity;
  protected int mBucketNum;
  protected double mLoadF; //said "recommend this be 0.2"
  protected HashFunction mHashFunction;
  protected Funnel<PageId> mFunnel;
  protected int curIdx;
  protected long pageNum;
  protected long pageSize;
  protected long pageReadNum;
  protected long pageReadSize;
  protected long hitNum;
  protected long hitByte;
  protected boolean debugMode;

  protected Map<Integer,Integer> debugMapPageToCode;
  protected Set<Integer> debugCodeSet;

  // no scope
  protected TinyTableWithCounters tinyTableWithCounters;
  protected TinyTable tinyTable;
  protected long putTime;
  protected long deleteTime;
  // we can use a hashcode to find string's 64bit long
  // and also filed
  // so we can define a hashcode's func
  // Objects.hash(name,a,a)
  public SWAMPSketchShadowCacheManager(ShadowCacheParameters params) {
    mWindowSize = params.mWindowSize;
    mFingerPrintSize = params.mTagBits;
    mBucketCapacity = params.mTagsPerBucket;
    long memoryInBits = FormatUtils.parseSpaceSize(params.mMemoryBudget) * 8;
    mBucketNum = (int)((memoryInBits-(mWindowSize*mFingerPrintSize)) / (mBucketCapacity*(mFingerPrintSize+32)+136));
    //mBucketNum = (int)((memoryInBits-(mWindowSize*mFingerPrintSize)) / (mBucketCapacity*mFingerPrintSize))*2;
    mLoadF = mBucketCapacity/(mWindowSize/(double)mBucketNum);
    tinyTableWithCounters = new TinyTableWithCounters(mFingerPrintSize,mBucketCapacity,mBucketNum);
    tinyTable = new TinyTable(mFingerPrintSize,mBucketCapacity,mBucketNum);
    cyclicFingerBuffer = new int[(int)mWindowSize];
    mHashFunction = Hashing.murmur3_32((int)System.currentTimeMillis());
    mFunnel = PageIdFunnel.FUNNEL;
    pageNum = 0;
    pageSize = 0;
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
    pageReadNum++;
    pageReadSize++;
    if(prev!=0){
      delete(prev);
    }
    long bucketNum = tinyTable.getNum(hashcode);
    //System.out.println("bucketBUm:"+bucketNum);
    if(bucketNum>=63){
      updateCurIdx();
      return false;
    }
    cyclicFingerBuffer[curIdx] = hashcode;
    updateCurIdx();
    if(tinyTable.containItem(hashcode)){
      hitNum++;
      hitByte+=size;
    }else{
      tinyTableWithCounters.StoreValue(hashcode,size);
      pageNum ++;
      pageSize += size;
    }
    //todo this have out of bound bug , seems need to change chainlength, however though  work at the beginning,but fail at 61504's op,so we need to read article and find how to set this
    tinyTable.addItem(hashcode);


    return true;
  }

  @Override
  public int get(PageId pageId, int bytesToRead, CacheScope scope) {
    return 0;
  }


  public boolean delete(int hashcode){
    if(tinyTable.containItem(hashcode)) {
      long prevSize = tinyTableWithCounters.GetValue(hashcode);
      tinyTable.RemoveItem(hashcode);
      if (!tinyTable.containItem(hashcode)) {
        //tinyTableWithCounters.RemoveItem(prev);
        pageSize -= prevSize;
        pageNum -= 1;
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
    return pageNum;
  }

  @Override
  public long getShadowCachePages(CacheScope scope) {
    return 0;
  }

  @Override
  public long getShadowCacheBytes() {
    return pageSize;
  }

  @Override
  public long getShadowCacheBytes(CacheScope scope) {
    return 0;
  }

  @Override
  public long getShadowCachePageRead() {
    return pageReadNum;
  }

  @Override
  public long getShadowCachePageHit() {
    return hitNum;
  }

  @Override
  public long getShadowCacheByteRead() {
    return pageReadSize;
  }

  @Override
  public long getShadowCacheByteHit() {
    return hitByte;
  }

  @Override
  public double getFalsePositiveRatio() {
    return 0;
  }

  @Override
  public long getSpaceBits() {
    // maybe not right, we should check this !
    return mFingerPrintSize*mWindowSize+ (long)mBucketNum*mBucketCapacity*(mFingerPrintSize+32)+mBucketNum* 136L;
  }

  @Override
  public String getSummary() {
    return "SWAMP: \nbitsPerTag: " + mFingerPrintSize
        + "\nSizeInMB: " + getSpaceBits() / 8.0 / Constants.MB;
  }
}
