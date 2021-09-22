package alluxio.client.file.cache.benchmark.concurrency;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ClientState {
    public AtomicInteger numThreads = new AtomicInteger(0);
    public AtomicLong numOps = new AtomicLong(0);
    public AtomicLong numInsert = new AtomicLong(0);
    public AtomicLong elapsedTime = new AtomicLong(0); // time in

    @Override
    public String toString() {
        return "ClientState:" +
                "\nnumThreads=" + numThreads +
                "\nnumOps=" + numOps +
                "\nnumInsert=" + numInsert +
                "\nelapsedTime=" + elapsedTime +
                "\nops/sec=" + numThreads.get() * numOps.get() * 1000.0 / elapsedTime.get() +
                "";
    }
}
