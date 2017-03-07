package indexingTopology.client;

import indexingTopology.DataTuple;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * Created by robert on 3/3/17.
 */
public abstract class ServerHandle implements Runnable {

    ObjectInputStream objectInputStream;
    ObjectOutputStream objectOutputStream;
    Socket client;

    public ServerHandle(Socket client) {
        this.client = client;
    }
    @Override
    public void run() {
            try {
                while (true) {
                    try {
                        objectInputStream = new ObjectInputStream(client.getInputStream());
                        objectOutputStream = new ObjectOutputStream(client.getOutputStream());
                        final Object newObject = objectInputStream.readObject();
                        System.out.println("Received: " + newObject);
                        final Response response = handleInputObject(newObject);
                        System.out.println("Handled: " + newObject);
                        objectOutputStream.writeObject(response);
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            } catch (IOException io) {
                io.printStackTrace();

            }
    }

    private Response handleInputObject(Object object) {
        if (object instanceof String) {
            final Response response = new Response();
            response.message = "This is the result of " + object;
        } else if (object instanceof DataTuple) {
            return handleTupleAppend((DataTuple) object);
        } else if (object instanceof ClientQueryRequest) {
            return handleClientQueryRequest((ClientQueryRequest)object);
        }
        final Response response = new Response();
        response.message = "Not supported " + object;
        return response;

    }

    abstract Response handleClientQueryRequest(ClientQueryRequest clientQueryRequest);

    abstract Response handleTupleAppend(DataTuple tuple);
}
