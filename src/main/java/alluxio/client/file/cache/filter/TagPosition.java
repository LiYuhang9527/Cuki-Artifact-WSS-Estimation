package alluxio.client.file.cache.filter;

public class TagPosition {
    private int bucketIndex;
    private int tagIndex;

    public TagPosition(int bucketIndex, int tagIndex) {
        this.bucketIndex = bucketIndex;
        this.tagIndex = tagIndex;
    }

    public TagPosition() {
        this(-1, -1);
    }

    boolean valid() {
        return bucketIndex >= 0 && tagIndex >= 0;
    }

    public int getBucketIndex() {
        return bucketIndex;
    }

    public int getTagIndex() {
        return tagIndex;
    }

    public void setBucketIndex(int bucketIndex) {
        this.bucketIndex = bucketIndex;
    }

    public void setTagIndex(int tagIndex) {
        this.tagIndex = tagIndex;
    }

    @Override
    public String toString() {
        return "TagPosition{" +
                "bucketIndex=" + bucketIndex +
                ", tagIndex=" + tagIndex +
                '}';
    }
}
