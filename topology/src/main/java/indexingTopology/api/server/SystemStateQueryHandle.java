package indexingTopology.api.server;

import indexingTopology.api.client.SystemStateRequest;
import indexingTopology.common.SystemState;

import java.io.IOException;

/**
 * Created by billlin on 2017/7/31.
 */
public class SystemStateQueryHandle extends ServerHandle {

    private SystemState systemState;

    public SystemStateQueryHandle(SystemState systemState){
        this.systemState = systemState;
    }

    public void handle(SystemStateRequest request) throws IOException {
        objectOutputStream.writeUnshared(systemState);
        objectOutputStream.reset();
    }
}
