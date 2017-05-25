package indexingTopology.client;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.Socket;
import java.net.SocketTimeoutException;

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
                client.setSoTimeout(1000);
                objectInputStream = new ObjectInputStream(client.getInputStream());
                objectOutputStream = new ObjectOutputStream(client.getOutputStream());
                while (true) {
                    try {
//                        System.out.println("try to read");
                        final Object newObject = objectInputStream.readUnshared();
//                        System.out.println("Received: " + newObject);
                        handleInputObject(newObject);
//                        System.out.println("Handled: " + newObject);
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    } catch (SocketTimeoutException e) {
                        if (Thread.currentThread().isInterrupted()) {
                            System.out.println("ServerHandle is interrupted");
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }
            } catch (EOFException e) {
                System.out.println("Client is closed.");
            } catch (IOException io) {
                System.out.println("IOException occurs.");
                if (!client.isClosed())
                    io.printStackTrace();
            }
    }

    void handleInputObject(Object object) {
        Method method = null;
        try {
            method = this.getClass().getMethod("handle", object.getClass());
            method.invoke(this, object);
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
    }
}
