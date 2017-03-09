package indexingTopology.client;

import indexingTopology.data.DataTuple;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

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

    public void close() throws IOException {
        objectInputStream.close();
        objectOutputStream.close();
        client.close();
    }

    public Response queryAPINumberOne() throws IOException, ClassNotFoundException {
        objectOutputStream.writeObject("string");
        return (Response)objectInputStream.readObject();
    }

    public Response temporalRangeQuery(Number lowKey, Number highKey, long startTime, long endTime) throws IOException,
            ClassNotFoundException {
        objectOutputStream.writeObject(new QueryRequest<Number>(lowKey, highKey, startTime, endTime));
        return (Response) objectInputStream.readObject();
    }

    public Response append(DataTuple dataTuple) throws IOException, ClassNotFoundException {
        objectOutputStream.writeObject(new AppendRequest(dataTuple));
        return (Response) objectInputStream.readObject();
    }





    public static void main(String[] args) throws Exception {
        Client client = new Client("localhost", 10001);
        client.connect();
        Response response = client.temporalRangeQuery(0.0, 10000.0, 0, Long.MAX_VALUE );
        System.out.println("Query one is submitted!");
        System.out.println(response);


//        Client client = new Client("localhost", 10000);
//        client.connect();
//        Response response = client.append(new DataTuple(100L, 200.3, "payload", System.currentTimeMillis()));
//        System.out.print(response);
    }

}
