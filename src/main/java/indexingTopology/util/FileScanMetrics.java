package indexingTopology.util;

import java.io.Serializable;

/**
 * Created by acelzj on 11/28/16.
 */
public class FileScanMetrics implements Serializable {

    private Long templateReadingTime;

    private Long totalTime;

    private Long searchTime;

    private Long fileOpenAndCloseTime;

    private Long leafReadingTime;

    private Long tupleGettingTime;

    private Long leafBytesReadingTime;

    private Long numberOfRecords;

    private Long lengthReadTime;

    private Long totalBytesReadTime;

    private String fileName;

    private Long subqueryStartTime;
    private Long subqueryEndTime;

    public String debugInfo;
    public FileScanMetrics() {

    }

    public void setTupleGettingTime(Long time) {
        this.tupleGettingTime = time;
    }

    public void setLeafReadTime(Long time) {
        this.leafReadingTime = time;
    }

    public void setTemplateReadingTime(Long time) {
        this.templateReadingTime = time;
    }

    public void setFileOpenAndCloseTime(Long time) {
        fileOpenAndCloseTime = time;
    }

    public void setSearchTime(Long time) {
        searchTime = time;
    }


    public void setTotalTime(Long totalTime) {
        this.totalTime = totalTime;
    }

    public void setTreeReadTime(Long searchTime) {
        this.searchTime = searchTime;
    }

    public Long getLeaveReadTime() {
        return leafReadingTime;
    }

    public Long getSearchTime() {
        return searchTime;
    }

    public Long getTupleGetTime() {
        return tupleGettingTime;
    }


    public Long getTotalTime() {
        return totalTime;
    }

    public Long getFileOpenAndCloseTime() {
        return fileOpenAndCloseTime;
    }

    public void setNumberOfRecords(Long numberOfRecords) {
        this.numberOfRecords = numberOfRecords;
    }

    public Long getNumberOfRecords() {
        return numberOfRecords;
    }

    public void setLengthReadTime(Long time) {
        this.lengthReadTime = time;
    }

    public void setTotalBytesReadTime(Long time) {
        this.totalBytesReadTime = time;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public void setSubqueryStartTime(Long subqueryStartTime) {
        this.subqueryStartTime = subqueryStartTime;
    }

    public void setSubqueryEndTime(Long subqueryEndTime) {
        this.subqueryEndTime = subqueryEndTime;
    }

    public Long getSubqueryStartTime() {
        return subqueryStartTime;
    }

    public Long getSubqueryEndTime() {
        return subqueryEndTime;
    }

//    public void addWithAnotherMetrics(FileScanMetrics otherMetrics) {
//        setTotalTime(otherMetrics.getTotalTime() + totalTime);
//        setFileReadingTime(otherMetrics.getFileReadingTime() + fileReadingTime);
//        setLeafDeserializationTime(otherMetrics.getLeafDeserializationTime() + leafDeserializationTime);
//        setTreeDeserializationTime(otherMetrics.getTreeDeserializationTime() + treeDeserializationTime);
//        setSearchTime(otherMetrics.getSearchTime() + searchTime);
//    }

    public void setLeafBytesReadingTime(Long time) {
        this.leafBytesReadingTime = time;
    }


    @Override
    public String toString() {
        return "total time " + totalTime + " file open and close time " + fileOpenAndCloseTime
                + " tuple search time " + tupleGettingTime + " template reading time " + templateReadingTime
                + " leaf reading time " + leafReadingTime + " search offset time " + searchTime
                + " leaf bytes reading time" + leafBytesReadingTime + " length read time" + lengthReadTime
                + " total bytes read time" + totalBytesReadTime + " file name" + fileName
                + " subquery start time " + subqueryStartTime + " subquery end time " + subqueryEndTime;
    }
}
