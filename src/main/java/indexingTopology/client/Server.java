package indexingTopology.client;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by robert on 2/3/17.
 */
public class Server {

    int port;

    ServerSocket serverSocket;

    ExecutorService executorService;

    public Server(int port) {
        this.port = port;
    }

    void startDaemon() throws IOException{
        serverSocket = new ServerSocket(port);
        executorService = Executors.newCachedThreadPool();
        while (true) {
            Socket client = serverSocket.accept();
            executorService.submit(new ServerHandle(client));
        }
//        while(true) {
//            Socket client = serverSocket.accept();
//            System.out.println("accepted new client: " + client.getInetAddress().toString());
//            new Thread(new Runnable() {
//                @Override
//                public void run() {
//                    try {
//                        System.out.println("Try to create input and output streams!");
//                        objectOutputStream = new ObjectOutputStream(client.getOutputStream());
//                        System.out.println("object output stream is created!");
//                        objectInputStream = new ObjectInputStream(client.getInputStream());
//                        System.out.println("object input stream is created!");
//                        while (true) {
//                            try {
//                                final Object newObject = objectInputStream.readObject();
//                                System.out.println("Received: " + newObject);
//                                final Response response = new Response();
//                                response.message = "This is the result of your query!";
//                                objectOutputStream.writeObject(response);
//                            } catch (ClassNotFoundException e) {
//                                e.printStackTrace();
//                            }
//                        }
//                    } catch (IOException io) {
//                        io.printStackTrace();
//
//                    }
//                    System.out.println("client is closed!");
//                }
//            }).start();
    }


    public static void main(String[] args) throws Exception {
        final Server server = new Server(10000);
        server.startDaemon();
    }
}
