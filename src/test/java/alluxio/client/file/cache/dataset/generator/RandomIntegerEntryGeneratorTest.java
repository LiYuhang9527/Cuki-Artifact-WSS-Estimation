package alluxio.client.file.cache.dataset.generator;

import alluxio.client.file.cache.dataset.Dataset;
import alluxio.client.file.cache.dataset.DatasetEntry;
import alluxio.client.file.cache.dataset.GeneralDataset;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RandomIntegerEntryGeneratorTest {

    @Test
    public void testBasic() {
        RandomIntegerEntryGenerator generator = new RandomIntegerEntryGenerator(1000);
        Dataset<Integer> dataset = new GeneralDataset<>(generator, 64);
        int count = 0;
        while (dataset.hasNext()) {
            count++;
            DatasetEntry<Integer> entry = dataset.next();
            assertTrue(dataset.getRealEntryNumber() <= 64);
        }
        assertEquals(1000, count);
    }

}