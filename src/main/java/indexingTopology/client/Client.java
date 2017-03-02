package indexingTopology.client;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;

/**
 * Created by robert on 2/3/17.
 */
public class Client {

    Socket client;

    String serverHost;

    int port;

    public Client(String serverHost, int port) {
        this.serverHost = serverHost;
        this.port = port;
    }

    public void connect() throws IOException{
        client = new Socket(serverHost, port);
    }


    public static void main(String[] args) throws Exception {
        Socket client = new Socket("localhost", 10000);
        ObjectInputStream objectInputStream = new ObjectInputStream(client.getInputStream());
        Object object = objectInputStream.readObject();
        System.out.println("I get " + object);
    }

}
