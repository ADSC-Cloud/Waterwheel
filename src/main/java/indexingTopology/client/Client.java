package indexingTopology.client;

import indexingTopology.data.DataTuple;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

/**
 * Created by robert on 2/3/17.
 */
public class Client {

    Socket client;

    String serverHost;

    ObjectInputStream objectInputStream;

    ObjectOutputStream objectOutputStream;

    int port;

    public Client(String serverHost, int port) {
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
                if (System.currentTimeMillis() - startTime >= timeoutInMilliseconds)
                    throw e;
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
        Response queryResponse = queryClient.temporalRangeQuery(0.0, 10000.0, 0, Long.MAX_VALUE );
        System.out.println("Query one is submitted!");
        System.out.println(queryResponse);

//
//        OneTuplePerTransferIngestionClient ingestionClient = new OneTuplePerTransferIngestionClient("localhost", 10000);
//        ingestionClient.connect();
//        Response ingestionResponse = ingestionClient.append(new DataTuple(100L, 200.3, "payload", System.currentTimeMillis()));
//        System.out.print(ingestionResponse);
    }

}
