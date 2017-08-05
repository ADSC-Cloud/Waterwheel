package indexingTopology.api.client;

import indexingTopology.common.SystemState;

import java.io.IOException;

/**
 * Created by billlin on 2017/7/31.
 */
public class SystemStateQueryClient extends ClientSkeleton{
    public SystemStateQueryClient(String serverHost, int port) {
        super(serverHost, port);
    }

    public SystemState query() throws IOException, ClassNotFoundException {
        objectOutputStream.writeUnshared(new SystemStateRequest());
        return (SystemState) objectInputStream.readUnshared();
    }


//    public QueryCoordinatorBolt
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        SystemStateQueryClient sys = new SystemStateQueryClient("localhost",20000);
        sys.connect();
        SystemStateQueryClient sys2 = new SystemStateQueryClient("localhost",20001);
        Thread.sleep(3000);
        SystemState systemState = sys.query();
        System.out.println("throughput: " + systemState.throughout);
        sys.close();
    }
}
