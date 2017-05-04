package indexingTopology.client;

import java.io.IOException;

/**
 * Created by robert on 8/3/17.
 */
public interface AppendRequestHandle {
    void handle(AppendTupleRequest tuple) throws IOException;
}
