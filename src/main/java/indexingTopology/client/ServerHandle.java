package indexingTopology.client;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import java.net.Socket;

/**
 * Created by robert on 3/3/17.
 */
public abstract class ServerHandle implements Runnable{

    ObjectInputStream objectInputStream;
    protected ObjectOutputStream objectOutputStream;
    Socket client;

//    public ServerHandle(Socket client) {
//        this.client = client;
//    }

    public ServerHandle() {

    }

    void setClientSocket(Socket clientSocket) {
        client = clientSocket;
    }

    @Override
    public void run() {
            try {
                objectInputStream = new ObjectInputStream(client.getInputStream());
                objectOutputStream = new ObjectOutputStream(client.getOutputStream());
                while (true) {
                    try {
                        final Object newObject = objectInputStream.readUnshared();
//                        System.out.println("Received: " + newObject);
                        handleInputObject(newObject);
//                        System.out.println("Handled: " + newObject);
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            } catch (IOException io) {
                io.printStackTrace();

            }
    }

    void handleInputObject(Object object) {
        try {
            Method method = this.getClass().getMethod("handle", object.getClass());
            method.invoke(this, object);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
