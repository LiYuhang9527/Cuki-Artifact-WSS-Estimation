package alluxio.client.file.cache.dataset.generator;

import alluxio.client.file.cache.dataset.DatasetEntry;
import alluxio.client.file.cache.dataset.DatasetUtils;
import alluxio.client.file.cache.dataset.ScopeInfo;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class MSREntryGenerator implements EntryGenerator<String> {
    private static int NUM_ENTRIES_PER_LOAD = 1000;

    private final String path;

    private BufferedReader reader;
    private Queue<DatasetEntry<String>> entries;
    private AtomicBoolean fileReady;

    private final AtomicLong count;

    public MSREntryGenerator(String path) {
        this.path = path;
        this.count = new AtomicLong(0);
        this.entries = new LinkedList<>();
        this.fileReady = new AtomicBoolean(false);
        openFile();
    }

    @Override
    public DatasetEntry<String> next() {
        if (entries.isEmpty() && fileReady.get()) {
            loadEntry(NUM_ENTRIES_PER_LOAD);
        }
        count.incrementAndGet();
        return entries.poll();
    }

    @Override
    public boolean hasNext() {
        return !entries.isEmpty() || fileReady.get();
    }

    private void openFile() {
        try {
            FileReader reader = new FileReader(path);
            this.reader = new BufferedReader(reader);
            this.fileReady.set(true);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void loadEntry(int num) {
        try {
            int nloaded = 0;
            while (nloaded < num) {
                String line = reader.readLine();
                if (line == null) {
                    fileReady.set(false);
                    return;
                }
                DatasetEntry<String> entry = parseMSREntry(line);
                if (entry != null) {
                    entries.offer(entry);
                    nloaded++;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static DatasetEntry<String> parseMSREntry(String line) {
        // Timestamp(windowsFileTime),Hostname,DiskNumber,Type,Offset,Size,ResponseTime
        String[] tokens = line.split(",");
        assert tokens.length == 7;
        // ignore write requests
//        if (!"Read".equals(tokens[3])) {
//            return null;
//        }
        long timestamp = DatasetUtils.WindowsFileTimeToUnixSeconds(Long.parseLong(tokens[0]));
        String scope = tokens[1] + "-" + tokens[2]; // Scope format `Hostname-DiskNumber`
        String offset = tokens[4];
        int size = Integer.parseInt(tokens[5]);
        // the size of each item should be not more than 2^20 byte
        size = Math.min((1<<20)-1, size);
        // ignore ResponseTime
        return new DatasetEntry<>(offset, size, new ScopeInfo(scope), timestamp);
    }
}
