package indexingTopology.client;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * Created by robert on 3/3/17.
 */
public class ServerHandle implements Runnable {

    ObjectInputStream objectInputStream;
    ObjectOutputStream objectOutputStream;
    Socket client;

    public ServerHandle(Socket client) {
        this.client = client;
    }
    @Override
    public void run() {
            try {
                objectInputStream = new ObjectInputStream(client.getInputStream());
                objectOutputStream = new ObjectOutputStream(client.getOutputStream());
                while (true) {
                    try {
                        final Object newObject = objectInputStream.readObject();
                        System.out.println("Received: " + newObject);
                        final Response response = new Response();
                        response.message = "This is the result of your query!";
                        objectOutputStream.writeObject(response);
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            } catch (IOException io) {
                io.printStackTrace();

            }
    }
}
