package indexingTopology.client;

import java.io.IOException;

/**
 * Created by robert on 8/3/17.
 */
public interface GeoTemporalQueryHandle {
    void handle(GeoTemporalQueryRequest clientQueryRequest) throws IOException;
}
