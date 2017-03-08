package indexingTopology.client;

import java.io.IOException;

/**
 * Created by robert on 8/3/17.
 */
interface AppendRequestHandle {
    void handle(AppendRequest tuple) throws IOException;
}
