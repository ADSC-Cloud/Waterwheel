package indexingTopology.util;

/**
 * Created by acelzj on 28/3/17.
 */
public class FileInformation {
    private String fileName;
    private Domain domain;
    private Long numberOfRecords;

    public FileInformation(String fileName, Domain domain) {
        this.fileName = fileName;
        this.domain = domain;
    }

    public FileInformation(String fileName, Domain domain, Long numberOfRecords) {
        this.fileName = fileName;
        this.domain = domain;
        this.numberOfRecords = numberOfRecords;
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
}
