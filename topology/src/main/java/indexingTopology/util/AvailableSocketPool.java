package indexingTopology.util;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by robert on 6/7/17.
 */
public class AvailableSocketPool {
    private int startPort = 10000;
    private int endPort = 20000;
    private Set<Integer> allocated = new HashSet<>();
    public int getAvailablePort() {
        int availablePort = -1;
        ServerSocket socket = null;
        for (int i = startPort; i <= endPort; i++) {
            try {
                if (allocated.contains(i))
                    continue;
                socket = new ServerSocket(i);
                availablePort = i;
                break;
            } catch (IOException e) {
                // ignore and continue to find available port.
//                e.printStackTrace();
            }
        }
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            allocated.add(availablePort);
        }
        return availablePort;
    }

    public void returnPort(int port) {
        allocated.remove(port);
    }

    public static void main(String[] args) {
        AvailableSocketPool socketPool = new AvailableSocketPool();
        System.out.println(socketPool.getAvailablePort());
        System.out.println(socketPool.getAvailablePort());
        System.out.println(socketPool.getAvailablePort());
        System.out.println(socketPool.getAvailablePort());
    }
}
