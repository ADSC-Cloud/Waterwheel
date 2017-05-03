package indexingTopology.client;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.InvocationTargetException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by robert on 2/3/17.
 */
public class Server<T extends ServerHandle> {

    private int port;

    private ServerSocket serverSocket;

    private ExecutorService executorService;

    private Class<ServerHandle> SomeServerHandle;

    private Object[] serverHandleArgs;

    private Class<?>[] classTypes;

    public Server(int port, Class<ServerHandle> SomeServerHandle, Class<?>[] classTypes, Object... args) {
        this.port = port;
        this.SomeServerHandle = SomeServerHandle;
        this.serverHandleArgs = args;
        this.classTypes = classTypes;
    }

    public void startDaemon() throws IOException{
        serverSocket = new ServerSocket(port);
        executorService = Executors.newCachedThreadPool();

        while (true) {
            Socket client = serverSocket.accept();
            try {
                ServerHandle handle;
//                if (args.length == 0) {
//                    handle = SomeServerHandle.getDeclaredConstructor(Socket.class).newInstance(client);
//                } else {
//                    ArrayList<Class<?>> classTypes = new ArrayList<>();
//                    for (int i = 0; i < args.length; i++) {
//                        classTypes.add(args[i].getClass());
//                    }
//                    handle = SomeServerHandle.getDeclaredConstructor(Socket.class, classTypes.toArray()).
//                }
                MethodHandle constructor = MethodHandles.publicLookup().findConstructor(SomeServerHandle, MethodType.methodType(void.class, classTypes));
                System.out.println("serverHandleArgs: " + serverHandleArgs);
                int servcerHandleArgsCount = 0;
                if(serverHandleArgs != null) {
                    servcerHandleArgsCount = serverHandleArgs.length;
                }

                if (servcerHandleArgsCount> 4) {
                    throw new RuntimeException("ServerHandle parameters cannot exceed 4.");
                }
                switch (servcerHandleArgsCount) {
                    case 0: handle = (ServerHandle) constructor.invoke(); break;
                    case 1: handle = (ServerHandle) constructor.invoke(serverHandleArgs[0]);break;
                    case 2: handle = (ServerHandle) constructor.invoke(serverHandleArgs[0], serverHandleArgs[1]);break;
                    case 3: handle = (ServerHandle) constructor.invoke(serverHandleArgs[0], serverHandleArgs[1], serverHandleArgs[2]);break;
                    case 4: handle = (ServerHandle) constructor.invoke(serverHandleArgs[0], serverHandleArgs[1], serverHandleArgs[2], serverHandleArgs[3]);break;
                    default:                    throw new RuntimeException("ServerHandle parameters cannot exceed 4.");
                }
                handle.setClientSocket(client);
                executorService.submit(handle);
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            } catch (Throwable throwable) {
                throwable.printStackTrace();
            }
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
        final Server server = new Server(10000, FakeServerHandle.class, new Class[]{int.class}, new Integer(1));
        server.startDaemon();
    }
}
