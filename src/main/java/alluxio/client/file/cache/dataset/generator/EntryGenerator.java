package alluxio.client.file.cache.dataset.generator;

import alluxio.client.file.cache.dataset.DatasetEntry;

public interface EntryGenerator<T> {
    DatasetEntry<T> next();

    boolean hasNext();
}
