package indexingTopology.client;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * Created by robert on 2/3/17.
 */
public class ClientSkeleton {

    Socket client;

    String serverHost;

    ObjectInputStream objectInputStream;

    ObjectOutputStream objectOutputStream;

    int port;

    public ClientSkeleton(String serverHost, int port) {
        this.serverHost = serverHost;
        this.port = port;
    }

    public void connect() throws IOException{
        client = new Socket(serverHost, port);
        objectOutputStream = new ObjectOutputStream((client.getOutputStream()));
        objectInputStream = new ObjectInputStream(client.getInputStream());
        System.out.println("Connected with " + serverHost);
    }

    public void connectWithTimeout(int timeoutInMilliseconds) throws IOException {
        long startTime = System.currentTimeMillis();
        while (true) {
            try {
                connect();
                break;
            } catch (IOException e) {
                if (System.currentTimeMillis() - startTime >= timeoutInMilliseconds) {
                    System.err.println(String.format("Connect to %s:%d fails.", serverHost, port));
                    throw e;
                }
            }
        }
    }

    public void close() throws IOException {
        objectInputStream.close();
        objectOutputStream.close();
        client.close();
    }

    public static void main(String[] args) throws Exception {
        QueryClient queryClient = new QueryClient("localhost", 10001);
        queryClient.connect();
        IResponse queryResponse = queryClient.temporalRangeQuery(0.0, 10000.0, 0, Long.MAX_VALUE );
        System.out.println("Query one is submitted!");
        System.out.println(queryResponse);

//
//        OneTuplePerTransferIngestionClientSkeleton ingestionClient = new OneTuplePerTransferIngestionClientSkeleton("localhost", 10000);
//        ingestionClient.connect();
//        Response ingestionResponse = ingestionClient.append(new DataTuple(100L, 200.3, "payload", System.currentTimeMillis()));
//        System.out.print(ingestionResponse);
    }

}
