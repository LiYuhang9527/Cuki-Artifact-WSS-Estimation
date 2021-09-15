package alluxio.client.file.cache.dataset;

import java.util.Objects;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DatasetEntry<?> that = (DatasetEntry<?>) o;
        return size == that.size &&
                Objects.equals(item, that.item) &&
                Objects.equals(scopeInfo, that.scopeInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(item, size, scopeInfo);
    }
}
