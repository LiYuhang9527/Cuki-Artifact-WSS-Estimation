package alluxio.client.file.cache;

import alluxio.Constants;
import alluxio.client.quota.CacheScope;
import alluxio.util.LRU;

import java.util.HashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class AccurateEstimationShadowCacheManager implements ShadowCache{
  private final Lock lock;
  private final LRU itemLRU;
  private final HashMap<PageId,ItemAttribute> itemToAttribute;
  private final HashMap<CacheScope, Integer> scopeToNumber;
  private final HashMap<CacheScope, Integer> scopeToSize;

  private final int bitsPerItem;
  private final int bitsPerScope;
  private final int bitsPerSize;
  private final int bitsPerTimestamp;
  private final long windowSize;
  private long hitCount;
  private long readCount;
  private long realNumber;
  private long realSize;
  private long readSize;
  private long hitSize;
  private long timestampNow;

  private static class ItemAttribute{
    int size;
    CacheScope scope;
    long timeStamp;

    public ItemAttribute(int size, CacheScope scope, long timeStamp) {
      this.size = size;
      this.scope = scope;
      this.timeStamp = timeStamp;
    }
  }

  public AccurateEstimationShadowCacheManager(ShadowCacheParameters params){
    this.windowSize = params.mWindowSize;
    this.lock = new ReentrantLock();
    this.itemLRU = new LRU();
    this.itemToAttribute = new HashMap<>();
    this.scopeToNumber = new HashMap<>();
    this.scopeToSize = new HashMap<>();
    this.bitsPerItem = params.mPageBits;
    this.bitsPerScope = params.mScopeBits;
    this.bitsPerSize = params.mSizeBits;
    this.bitsPerTimestamp = 64;
    this.realNumber = 0;
    this.realSize = 0;
    this.timestampNow = 0;
    this.hitCount = 0;
    this.readCount = 0;
    this.readSize = 0;
    this.hitSize = 0;
  }

  @Override
  public boolean put(PageId pageId, int size, CacheScope scope) {
    boolean success;
    lock.lock();
    readCount++;
    readSize += size;
    if(pageId == null){
      return false;
    }
    ItemAttribute attribute = itemToAttribute.get(pageId);
    if (attribute == null) {
      itemToAttribute.put(pageId, new ItemAttribute(size,scope,timestampNow));
      success = itemLRU.put(pageId);
      scopeToSize.put(scope, scopeToSize.getOrDefault(scope, 0) + size);
      scopeToNumber.put(scope, scopeToNumber.getOrDefault(scope, 0) + 1);
      realSize += size;
    }else{
      hitCount++;
      hitSize+=size;
      attribute.timeStamp = timestampNow;
      success = itemLRU.put(pageId);
    }
    realNumber = itemLRU.getSize();
    lock.unlock();
    return success;
  }

  @Override
  public int get(PageId pageId, int bytesToRead, CacheScope scope) {
    return 0;
  }

  @Override
  public boolean delete(PageId pageId) {
    boolean b1 = itemLRU.remove(pageId);
    boolean b2 = itemToAttribute.remove(pageId)!=null;
    return b1&&b2;
  }


  public void aging(){
    long oldTimestamp = timestampNow - windowSize;
    if(oldTimestamp>0){
      while(true){
        PageId item = itemLRU.peek();
        ItemAttribute itemAttribute = itemToAttribute.get(item);
        if(itemAttribute.timeStamp < oldTimestamp){
          itemLRU.poll();
          realSize -= itemAttribute.size;
          scopeToNumber.put(itemAttribute.scope,scopeToNumber.getOrDefault(itemAttribute.scope,0)-1);
          scopeToSize.put(itemAttribute.scope,scopeToSize.getOrDefault(itemAttribute.scope,0)-itemAttribute.size);
          itemToAttribute.remove(item);
        }else{
          break;
        }
      }
    }
    realNumber = itemLRU.getSize();
  }


  @Override
  public void updateWorkingSetSize() {
    aging();
  }

  @Override
  public void stopUpdate() {

  }

  @Override
  public void updateTimestamp(long increment) {
    this.timestampNow += increment;
  }

  @Override
  public long getShadowCachePages() {
    return realNumber;
  }

  @Override
  public long getShadowCachePages(CacheScope scope) {
    return scopeToNumber.get(scope);
  }

  @Override
  public long getShadowCacheBytes() {
    return realSize;
  }

  @Override
  public long getShadowCacheBytes(CacheScope scope) {
    return scopeToSize.get(scope);
  }

  @Override
  public long getShadowCachePageRead() {
    return readCount;
  }

  @Override
  public long getShadowCachePageHit() {
    return hitCount;
  }

  @Override
  public long getShadowCacheByteRead() {
    return readSize;
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
    long scopeNum = scopeToNumber.size();
    long space = realNumber*(bitsPerItem+bitsPerTimestamp+bitsPerScope+bitsPerTimestamp);
    space+=scopeNum*(bitsPerScope*2+32+bitsPerSize);
    return space;
  }

  @Override
  public String getSummary() {
    return "\nbitsPerItem: " + bitsPerItem +  "\nbitsPerSize: " + bitsPerSize
        + "\nbitsPerScope: " + bitsPerScope + "\nSizeInMB: "
        + (windowSize*(bitsPerItem+bitsPerTimestamp+bitsPerScope+bitsPerTimestamp) / 8.0 / Constants.MB
        + windowSize*(bitsPerScope*2+32+bitsPerSize) / 8.0 / Constants.MB);
  }
}
