package indexingTopology.client;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by robert on 2/3/17.
 */
public class Server {

    int port;

    ServerSocket serverSocket;

    ObjectInputStream inputStream;

    public Server(int port) {
        this.port = port;
    }

    void startDaemon() throws IOException{
        serverSocket = new ServerSocket(port);
        Socket client = serverSocket.accept();
        inputStream = new ObjectInputStream(client.getInputStream());
    }


    public static void main(String[] args) throws Exception {
        final ServerSocket serverSocket = new ServerSocket(10000);
        new Thread(new Runnable() {
            @Override
            public void run() {
                while(true) {
                    try {
                        Socket client = serverSocket.accept();
                        System.out.println(String.format("We accepted a connection from %s", client.getInetAddress().toString()));
                        new ObjectOutputStream(client.getOutputStream()).writeObject("You are stupid!");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
    }
}
