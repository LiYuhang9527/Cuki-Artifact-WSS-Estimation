package alluxio.client.file.cache.dataset;

public class DatasetUtils {
    public static final long WINDOWS_TICK = 10000000L;
    public static final long SEC_TO_UNIX_EPOCH  = 11644473600L;

    public static final long WindowsFileTimeToUnixSeconds(long winFileTime) {
        return (winFileTime / WINDOWS_TICK - SEC_TO_UNIX_EPOCH);
    }
}
