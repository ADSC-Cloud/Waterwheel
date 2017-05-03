package indexingTopology.client;

import indexingTopology.data.DataTuple;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by robert on 2/5/17.
 */
public class PerformanceTester {

    public static class PerformanceTesterServerHandle extends ServerHandle implements QueryHandle, AppendRequestHandle {

        int count = 0;
        @Override
        public void handle(AppendRequest tuple) throws IOException {
            if((int)tuple.dataTuple.get(0) != count) {
                System.err.println(String.format("Received: %d, Expected %d", (int)tuple.dataTuple.get(0), count));
            }
            count++;
            if (count % 1000 == 0) {
                System.out.println("Server: received " + count);
            }
//            objectOutputStream.writeObject(new MessageResponse("received!"));
        }

        @Override
        public void handle(QueryRequest clientQueryRequest) throws IOException {

        }
    }

    static Thread launchServerDaemon(int port) {
        Thread thread =
        new Thread(() -> {
            final Server server = new Server(1024, PerformanceTesterServerHandle.class, new Class[]{}, null);
            try {
                server.startDaemon();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        thread.start();
        return thread;
    }

    static double testAppendThroughput(String host, int tuples, int payload) throws IOException, ClassNotFoundException,
            InterruptedException {

        IngestionClient client = new IngestionClient(host, 1024);
        client.connect();
        char[] charArray = new char[payload];
        Arrays.fill(charArray, ' ');
        String str = new String(charArray);
        long start = System.currentTimeMillis();
        int count = 0;
        while(count <= tuples) {
            client.append(new DataTuple(count, 3, str));
            count ++;
            if (count % 1000 == 0) {
                System.out.println("Client: append " + count);
            }
        }
        double ret = tuples / (System.currentTimeMillis() - (double)start) * 1000;
//        thread.join();
        return ret;
    }

    static double testAppendThroughput(int tuples, int payload) throws IOException, ClassNotFoundException,
            InterruptedException {
        return testAppendThroughput("localhost", tuples, payload);
    }

    static public void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String option = args[0];
        System.out.println("args: " + option);
        if(option.equals("both")) {
            System.out.println("both!");
            launchServerDaemon(1024);
            double throughput = testAppendThroughput(10000,64);
            System.out.println("Throughput: " + throughput);
        } else if (option.equals("server")) {
            System.out.println("Server!");
            launchServerDaemon(1024);
        } else if (option.equals("client")) {
            System.out.println("Client!");
            String serverHost = "localhost";
            if (args.length > 1 && args[1] != null) {
                serverHost = args[1];
            }
            double throughput = testAppendThroughput(serverHost, 1000000,64);
            System.out.println("Throughput: " + throughput);
        }
    }


}
