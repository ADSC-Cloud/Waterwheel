package indexingTopology.api.server;

import indexingTopology.api.client.QueryClient;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.InvocationTargetException;
import java.net.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

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

    private boolean closed = false;

    private List<Future> futureList;

    public Server(int port, Class<ServerHandle> SomeServerHandle, Class<?>[] classTypes, Object... args) {
        this.port = port;
        this.SomeServerHandle = SomeServerHandle;
        this.serverHandleArgs = args;
        this.classTypes = classTypes;
        this.futureList = new ArrayList<>();
    }

    public void startDaemon(){
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<ServerSocket> serverSocketFuture = executor.submit(() -> {
            ServerSocket socket = null;
            while (socket == null) {
                try {
                    socket = new ServerSocket(port);
                } catch (IOException e) {
//                    e.printStackTrace();
                    System.err.println(String.format("Api Server: Cannot bind socket on port %d. Retry after 1 second.", port));
                    Thread.sleep(1000);
                }
            }
            return socket;
        });

        try {
            serverSocket = serverSocketFuture.get(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
        executor.shutdownNow();
        if (serverSocket == null) {
            System.err.println("Cannot create server on port after having retried for 30 seconds.");
            return;
        }

        executorService = Executors.newCachedThreadPool();


        futureList.add(executorService.submit(() ->{
            Socket client = null;
            while (true) {
            try {
                serverSocket.setSoTimeout(200);
                client = serverSocket.accept();
                ServerHandle handle;
//                if (args.length == 0) {
//                    handle = SomeServerHandle.getDeclaredConstructor(Socket.class).newInstance(api);
//                } else {
//                    ArrayList<Class<?>> classTypes = new ArrayList<>();
//                    for (int i = 0; i < args.length; i++) {
//                        classTypes.add(args[i].getClass());
//                    }
//                    handle = SomeServerHandle.getDeclaredConstructor(Socket.class, classTypes.toArray()).
//                }
                MethodHandle constructor = MethodHandles.publicLookup().findConstructor(SomeServerHandle, MethodType.methodType(void.class, classTypes));
//                System.out.println("serverHandleArgs: " + serverHandleArgs);
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
                futureList.add(executorService.submit(handle));
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            } catch (SocketTimeoutException e) {
                if (Thread.currentThread().isInterrupted() || closed)
//                    break;
                    throw new InterruptedException();
            } catch (RejectedExecutionException e) {
                throw e;
            } catch (SocketException e) {
                if (closed)
                    break;
            }

            catch (Throwable throwable) {
                throwable.printStackTrace();
                if (Thread.interrupted() || closed)
//                    break;
                    throw new InterruptedException();
            }

            if (Thread.currentThread().isInterrupted()) {
                Thread.currentThread().interrupt();
                break;
            }

        }
         return null;}
        ));
    }

    public void endDaemon() {
        System.out.println("EndDaemon is called!");
        closed = true;
        try {
            serverSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            executorService.shutdownNow();
            for (Future future : futureList) {
                future.cancel(true);
//            while (future.isDone()) {
//                System.err.println("Waiting a task to be done!");
//                try {
//                    Thread.sleep(1000);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) throws Exception {
        final Server server = new Server(10000, FakeServerHandle.class, new Class[]{int.class}, new Integer(1));
        final QueryClient clientSkeleton = new QueryClient("localhost", 10000);
        System.out.println("start");
        server.startDaemon();
        clientSkeleton.connect();
        System.out.println("started");
        clientSkeleton.temporalRangeQuery(0,0,0,0);
        clientSkeleton.close();
        server.endDaemon();
        System.out.println("end");
    }
}
