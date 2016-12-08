package indexingTopology.util;

/**
 * Created by acelzj on 11/28/16.
 */
public class FileScanMetrics {

    private Long fileReadingTime;

    private Long leafDeserializationTime;

    private Long treeDeserializationTime;

    private Long totalTime;

    private Long searchTime;

    public FileScanMetrics() {

    }

    public void setFileReadingTime(Long fileReadingTime) {
        this.fileReadingTime = fileReadingTime;
    }

    public void setLeafDeserializationTime(Long leafDeserializationTime) {
        this.leafDeserializationTime = leafDeserializationTime;
    }

    public void setTreeDeserializationTime(Long treeDeserializationTime) {
        this.treeDeserializationTime = treeDeserializationTime;
    }

    public void setTotalTime(Long totalTime) {
        this.totalTime = totalTime;
    }

    public void setSearchTime(Long searchTime) {
        this.searchTime = searchTime;
    }

    public Long getFileReadingTime() {
        return fileReadingTime;
    }

    public Long getLeafDeserializationTime() {
        return leafDeserializationTime;
    }

    public Long getTreeDeserializationTime() {
        return treeDeserializationTime;
    }

    public Long getTotalTime() {
        return totalTime;
    }

    public Long getSearchTime() {
        return searchTime;
    }

    public void addWithAnotherMetrics(FileScanMetrics otherMetrics) {
        setTotalTime(otherMetrics.getTotalTime() + totalTime);
        setFileReadingTime(otherMetrics.getFileReadingTime() + fileReadingTime);
        setLeafDeserializationTime(otherMetrics.getLeafDeserializationTime() + leafDeserializationTime);
        setTreeDeserializationTime(otherMetrics.getTreeDeserializationTime() + treeDeserializationTime);
        setSearchTime(otherMetrics.getSearchTime() + searchTime);
    }

}
