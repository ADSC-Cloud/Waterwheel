package indexingTopology.client.test;

import indexingTopology.client.IngestionClientBatchMode;
import indexingTopology.data.DataSchema;
import indexingTopology.data.DataTuple;
import indexingTopology.util.FrequencyRestrictor;

import java.io.IOException;

/**
 * Created by robert on 31/5/17.
 */
public class ClientTest {
    static public void main(String [] args) throws IOException {

        String host = "localhost";
        if (args.length != 0) {
            host = args[0];
        }
        DataSchema schema = new DataSchema();
        schema.addIntField("a1");
        schema.addLongField("a2");
        schema.addVarcharField("a3", 100);
        schema.addDoubleField("a4");

        IngestionClientBatchMode clientBatchMode = new IngestionClientBatchMode(host, 10000,
                schema, 1024);
        clientBatchMode.connectWithTimeout(10000);

        while (true) {
            clientBatchMode.appendInBatch(new DataTuple(1, 2L, "3", 4.0));
//            clientBatchMode.flush();
//            break;
        }
    }
}
