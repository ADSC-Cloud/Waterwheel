package indexingTopology.client;

import indexingTopology.DataTuple;

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
            response.message = "This is the result of your query!";
            return response;
        } else if (object instanceof ClientQueryRequest) {
            DataTuple dataTuple = new DataTuple();
            dataTuple.add("ID 1");
            dataTuple.add(100);
            dataTuple.add(3.14);

            DataTuple dataTuple1 = new DataTuple();
            dataTuple1.add("ID 2");
            dataTuple1.add(200);
            dataTuple1.add(6.34);

            QueryResult queryResult = new QueryResult();
            queryResult.add(dataTuple);
            queryResult.add(dataTuple1);
            return queryResult;
        }

        Response response = new Response();
        response.message = "Unknown command!";
        return response;

    }
}
