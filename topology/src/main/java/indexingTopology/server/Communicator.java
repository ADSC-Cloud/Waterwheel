package indexingTopology.server;

/**
 * Created by robert on 17/7/17.
 */
public interface Communicator {
    void sentDirect(String streamId, int receiverId, Object message);
    void sendShuffle(String streamId, Object message);
}

