package alluxio.client.file.cache.dataset;

public interface Dataset<T> {
    public DatasetEntry<T> next();

    public boolean hasNext();

    public int getRealEntryNumber();

    public int getRealEntryNumber(ScopeInfo scope);

    public int getRealEntrySize();

    public int getRealEntrySize(ScopeInfo scope);
}
