package alluxio.client.file.cache.dataset.generator;

import alluxio.client.file.cache.dataset.DatasetEntry;
import alluxio.client.file.cache.dataset.GeneralDataset;
import org.junit.Test;

import static org.junit.Assert.*;

public class MSREntryGeneratorTest {
    private static final String MSR_SAMPLE_RELATIVE_PATH = "data/prxy_0_100.csv";

    @Test
    public void testBasic() {
        String path = getClass().getResource("/").getPath() + MSR_SAMPLE_RELATIVE_PATH;
        MSREntryGenerator generator = new MSREntryGenerator(path);
        GeneralDataset<String> dataset = new GeneralDataset<>(generator, 100);
        int count = 0;
        while (dataset.hasNext()) {
            DatasetEntry<String> e = dataset.next();
            count++;
        }
        assertEquals(100, count);
        assertEquals(99, dataset.getRealEntryNumber()); // 1 duplicated entry
    }
}