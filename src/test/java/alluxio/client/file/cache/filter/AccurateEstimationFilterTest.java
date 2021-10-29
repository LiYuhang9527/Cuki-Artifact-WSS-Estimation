package alluxio.client.file.cache.filter;

import alluxio.client.file.cache.dataset.GeneralDataset;
import org.junit.Test;

public class AccurateEstimationFilterTest {
    AccurateEstimationFilter<Integer> mAccurateEstimationFilter;
    GeneralDataset<String> mGeneralDataset;
    @Test
    public void basicTest(){
        int[] numList = new int[]{2,3,2};
        int windowSize = 2;
        mAccurateEstimationFilter = new AccurateEstimationFilter<>(windowSize,10,10,10,10,SlidingWindowType.COUNT_BASED);
        for(int i =0;i<3;i++){
            mAccurateEstimationFilter.put(numList[i],2,i,new ScopeInfo("FLIED"));
        }
        mAccurateEstimationFilter.printItemTime();
        System.out.println(mAccurateEstimationFilter.getItemNum());
        System.out.println(mAccurateEstimationFilter.getItemSize());

    }

}
