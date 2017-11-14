package indexingTopology.api.server;

import indexingTopology.api.client.EOF;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;

/**
 * Created by robert on 3/3/17.
 */
public abstract class ServerHandle implements Runnable{

    ObjectInputStream objectInputStream;
    protected ObjectOutputStream objectOutputStream;
    Socket client;

//    public ServerHandle(Socket api) {
//        this.api = api;
//    }

    public ServerHandle() {

    }

    void setClientSocket(Socket clientSocket) {
        client = clientSocket;
    }

    @Override
    public void run() {
        try {
            client.setSoTimeout(100000);
            client.setTcpNoDelay(true);
            objectInputStream = new ObjectInputStream(client.getInputStream());
            objectOutputStream = new ObjectOutputStream(client.getOutputStream());


        } catch (SocketException e1) {
            e1.printStackTrace();
        } catch (IOException e1) {
            e1.printStackTrace();
        }

        while (true) {
            try {
                Object newObject = objectInputStream.readUnshared();
                if (newObject instanceof EOF) {
                    client.close();
                    break;
                }
                handleInputObject(newObject);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (SocketTimeoutException e) {
                if (Thread.currentThread().isInterrupted()) {
                    System.out.println("ServerHandle is interrupted");
                    Thread.currentThread().interrupt();
                    break;
                }
            } catch (EOFException e) {
                try {
                    client.close();
                    System.out.println("EOFException occurs, close api");
                    break;
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            } catch (IOException io) {
                io.printStackTrace();
                if (client.isClosed())
                    break;
//                System.out.println("IOException occurs.");
                io.printStackTrace();
                try {
                    client.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                break;
//                if (!api.isClosed()) {
//                    io.printStackTrace();
//                    try {
//                        api.close();
//                        break;
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//
//                } else {
//                    break;
//                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        System.out.println("ServerHandling thread terminated.");
        close();
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
            System.err.println(String.format("Object: %s", object.getClass()));
        }
    }

    public void close() {

    }
}
