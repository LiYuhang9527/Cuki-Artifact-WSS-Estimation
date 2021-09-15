package alluxio.client.file.cache.dataset;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class RandomIntegerDatasetTest {
    private static final long NUM_ENTRY = 1000*100;
    private static final int WINDOW_SIZE = 1000;
    private static final int NUM_UNIQUE_ENTRY = 500;

    @Test
    public void testBasic() {
        Dataset<Integer> dataset = new RandomIntegerDataset(NUM_ENTRY, WINDOW_SIZE, 0, NUM_UNIQUE_ENTRY);
        while (dataset.hasNext()) {
            dataset.next();
            assertTrue(dataset.getRealEntryNumber() <= NUM_UNIQUE_ENTRY);
        }
        System.out.println(dataset.getRealEntrySize(new ScopeInfo("table1")));
    }

}