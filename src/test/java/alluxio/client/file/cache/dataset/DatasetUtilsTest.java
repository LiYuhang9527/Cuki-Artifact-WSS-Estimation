package alluxio.client.file.cache.dataset;

import org.junit.Test;

import static org.junit.Assert.*;


public class DatasetUtilsTest {
    @Test
    public void testWindowsFileTimeToUnixSeconds() {
        long winFileTime = 128166391024154329L;
        long unixSeconds = 1172165502L;
        assertEquals(unixSeconds, DatasetUtils.WindowsFileTimeToUnixSeconds(winFileTime));
    }
}