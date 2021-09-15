package alluxio.client.file.cache.dataset;

public class DatasetEntry<T> {
    private T item;
    private int size;
    private ScopeInfo scopeInfo;

    public DatasetEntry(T item, int size, ScopeInfo scopeInfo) {
        this.item = item;
        this.size = size;
        this.scopeInfo = scopeInfo;
    }

    public T getItem() {
        return item;
    }

    public int getSize() {
        return size;
    }

    public ScopeInfo getScopeInfo() {
        return scopeInfo;
    }

    public void setItem(T item) {
        this.item = item;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public void setScopeInfo(ScopeInfo scopeInfo) {
        this.scopeInfo = scopeInfo;
    }
}
