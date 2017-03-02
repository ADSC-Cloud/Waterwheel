package indexingTopology.util;

import indexingTopology.config.TopologyConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.CountDownLatch;


/**
 * Created by acelzj on 24/2/17.
 */
public class ZookeeperHandler {

    ZooKeeper zooKeeper;

    ZooKeeperConnection connection;

    public ZookeeperHandler() throws Exception {
        connection = new ZooKeeperConnection();
        zooKeeper = connection.connect(TopologyConfig.ZOOKEEPER_HOST);
    }

    public void create(String path, byte[] data) throws KeeperException,InterruptedException {
        Stat stat = zooKeeper.exists(path, true);
        if (stat != null) {
            zooKeeper.delete(path, zooKeeper.exists(path,true).getVersion());
        }

        zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    public void setData(String path, byte[] data) throws KeeperException, InterruptedException {
        zooKeeper.setData(path, data, zooKeeper.exists(path,true).getVersion());
    }

    public byte[] getData(String path) throws KeeperException, InterruptedException {
        byte[] bytes;
        bytes = zooKeeper.getData(path, false, null);
        return bytes;
    }


}
