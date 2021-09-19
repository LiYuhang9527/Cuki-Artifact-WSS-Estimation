package alluxio.client.file.cache.dataset.generator;

import alluxio.client.file.cache.dataset.DatasetEntry;
import alluxio.client.file.cache.dataset.ScopeInfo;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class TwitterEntryGenerator implements EntryGenerator<String> {
    private static int NUM_ENTRIES_PER_LOAD = 1000;

    private final String path;

    private BufferedReader reader;
    private Queue<DatasetEntry<String>> entries;
    private AtomicBoolean fileReady;

    private final AtomicLong count;

    public TwitterEntryGenerator(String path) {
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
                DatasetEntry<String> entry = parseTwitterEntry(line);
                if (entry != null) {
                    entries.offer(entry);
                    nloaded++;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static DatasetEntry<String> parseTwitterEntry(String line) {
        // Timestamp(seconds),Key,KeySize,ValueSize,ClientId,Operation,TTL
        String[] tokens = line.split(",");
//        for (String s : tokens) {
//            System.out.println(s);
//        }
        assert tokens.length == 7;
        // ignore write requests
//        if (!"get".equals(tokens[5])) {
//            return null;
//        }
        long timestamp = Long.parseLong(tokens[0]);
        String namespaceAndKey = tokens[1];
        int pi = namespaceAndKey.lastIndexOf("-");
        String scope = namespaceAndKey.substring(0, pi); // Scope format `Namespace1-Namespace2-...`
        String key = namespaceAndKey.substring(pi+1);
        int size = Integer.parseInt(tokens[2]) + Integer.parseInt(tokens[3]);
        // the size of each item should be not more than 2^20 byte
        size = Math.min((1<<20)-1, size);
        // ignore ResponseTime
        return new DatasetEntry<>(key, size, new ScopeInfo(scope), timestamp);
    }
}
