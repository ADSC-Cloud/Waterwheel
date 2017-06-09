package indexingTopology.util;

import indexingTopology.bloom.DataChunkBloomFilters;

/**
 * Created by acelzj on 28/3/17.
 */
public class FileInformation {
    private String fileName;
    private Domain domain;
    private Long numberOfRecords;
    private DataChunkBloomFilters bloomFilters;

    public FileInformation(String fileName, Domain domain) {
        this.fileName = fileName;
        this.domain = domain;
    }

    public FileInformation(String fileName, Domain domain, Long numberOfRecords, DataChunkBloomFilters bloomFilters) {
        this.fileName = fileName;
        this.domain = domain;
        this.numberOfRecords = numberOfRecords;
        this.bloomFilters = bloomFilters;
    }

    public String getFileName() {
        return fileName;
    }

    public Domain getDomain() {
        return domain;
    }

    public Long getNumberOfRecords() {
        return numberOfRecords;
    }

    public DataChunkBloomFilters getBloomFilters() {
        return bloomFilters;
    }
}
