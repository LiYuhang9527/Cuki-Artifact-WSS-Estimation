package alluxio.client.file.cache.dataset.generator;

import alluxio.client.file.cache.dataset.DatasetEntry;
import alluxio.client.file.cache.dataset.GeneralDataset;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TwitterEntryGeneratorTest {
    private static final String TWITTER_TRACE_SAMPLE_RELATIVE_PATH = "data/cluster37.0_100.csv";

    @Test
    public void testBasic() {
        String path = getClass().getResource("/").getPath() + TWITTER_TRACE_SAMPLE_RELATIVE_PATH;
        EntryGenerator<String> generator = new TwitterEntryGenerator(path);
        GeneralDataset<String> dataset = new GeneralDataset<>(generator, 100);
        int count = 0;
        while (dataset.hasNext()) {
            DatasetEntry<String> e = dataset.next();
            count++;
        }
        assertEquals(100, count);
        assertEquals(74, dataset.getRealEntryNumber());
    }
}