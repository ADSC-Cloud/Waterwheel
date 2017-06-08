package indexingTopology.api;

import indexingTopology.api.client.AppendRequest;
import indexingTopology.api.client.AppendRequestBatchMode;
import indexingTopology.api.client.IngestionClientBatchMode;
import indexingTopology.api.server.AppendRequestHandle;
import indexingTopology.api.server.IAppendRequestBatchModeHandle;
import indexingTopology.api.server.Server;
import indexingTopology.api.server.ServerHandle;
import indexingTopology.data.DataSchema;
import indexingTopology.data.DataTuple;
import junit.framework.TestCase;

import java.io.IOException;

/**
 * Created by robert on 29/5/17.
 */
public class IngestionClientAndServerTest extends TestCase {

    protected long count;

    public void testIngestion() throws IOException, InterruptedException {
        DataSchema dataSchema = new DataSchema();
        dataSchema.addIntField("a1");
        dataSchema.addLongField("a2");
        dataSchema.addDoubleField("a3");
        dataSchema.addVarcharField("a4", 100);

//        BlockingArrayQueue<DataTuple> inputQueue = new BlockingArrayQueue<>();
        count = 0;
        Server server = new Server(10000, AppendServerHandle.class, new Class[]{IngestionClientAndServerTest.class}, this);
        server.startDaemon();

        int tuples = 100000;

        IngestionClientBatchMode ingestionClientBatchMode = new IngestionClientBatchMode("localhost", 10000, dataSchema, 1024);
        ingestionClientBatchMode.connectWithTimeout(50000);
        for (int i = 0; i < tuples; i++) {
            DataTuple dataTuple = new DataTuple(i, 100L, 3.23, "StringValue");
            ingestionClientBatchMode.appendInBatch(dataTuple);
        }
        ingestionClientBatchMode.flush();
        Thread.sleep(1000);
        assertEquals(tuples, count);
        ingestionClientBatchMode.close();
        server.endDaemon();

    }

    public void testIngestionLarge() throws IOException, InterruptedException {
        DataSchema dataSchema = new DataSchema();
        dataSchema.addIntField("a1");
        dataSchema.addLongField("a2");
        dataSchema.addDoubleField("a3");
        dataSchema.addVarcharField("a4", 100);

//        BlockingArrayQueue<DataTuple> inputQueue = new BlockingArrayQueue<>();
        count = 0;
        Server server = new Server(10000, AppendServerHandle.class, new Class[]{IngestionClientAndServerTest.class}, this);
        server.startDaemon();

        long tuples = 10000000;

        IngestionClientBatchMode ingestionClientBatchMode = new IngestionClientBatchMode("localhost",
                10000, dataSchema, 10240);
        ingestionClientBatchMode.connectWithTimeout(50000);
        for (int i = 0; i < tuples; i++) {
            DataTuple dataTuple = new DataTuple(i, 100L, 3.23, "StringValue");
            ingestionClientBatchMode.appendInBatch(dataTuple);
        }
        ingestionClientBatchMode.flush();
        Thread.sleep(1000);
        assertEquals(tuples, count);
        ingestionClientBatchMode.close();
        server.endDaemon();

    }


    public static class AppendServerHandle extends ServerHandle implements AppendRequestHandle, IAppendRequestBatchModeHandle {

        IngestionClientAndServerTest that;

        public AppendServerHandle(IngestionClientAndServerTest that) {
            this.that = that;
        }

        @Override
        public void handle(AppendRequest tuple) throws IOException {
//            DataTuple dataTuple = tuple.dataTuple;
            that.count ++;
//            try {
//                Thread.sleep(1);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
        }

        @Override
        public void handle(AppendRequestBatchMode tuple) throws IOException {
            if (tuple.requireAck) {
                objectOutputStream.writeUnshared("received");
                objectOutputStream.reset();
            }
            tuple.dataTupleBlock.deserialize();

            tuple.dataTupleBlock.tuples.forEach(t -> {
                that.count ++;

            });

//            try {
//                Thread.sleep(10);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
        }
    }
}
