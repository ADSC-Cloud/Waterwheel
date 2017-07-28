package indexingTopology.index;

import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;
import indexingTopology.compression.Compressor;
import indexingTopology.compression.CompressorFactory;
import indexingTopology.compression.Decompressor;
import indexingTopology.config.TopologyConfig;
import indexingTopology.exception.UnsupportedGenericException;
import indexingTopology.metrics.TimeMetrics;

import java.io.IOException;
import java.util.Random;

/**
 * Created by robert on 28/7/17.
 */
public class BTreeCompressionTest {
    static public void main(String[] args) throws UnsupportedGenericException, IOException {
        TopologyConfig config = new TopologyConfig();
        BTree<Double, Long> bTree = new BTree<>(128, config);

        final int numberOfTuples = 1024 * 512;
        final int numberOfKeys = 1024 * 1024;

        Double low = 0.0;
        Double high = 1000.0;

        Random random = new Random();

        DataSchema schema = new DataSchema();
        schema.addDoubleField("key");
        schema.addVarcharField("payload", 50);
        schema.addLongField("timestamp");

        for (int i = 0; i < numberOfTuples; i++) {
            double key = 1 / (high - low) * random.nextInt(numberOfKeys);
            DataTuple tuple = new DataTuple();
            tuple.add(key);
            tuple.add(Double.toString(random.nextDouble()));
            tuple.add(System.currentTimeMillis());
            bTree.insert(key, schema.serializeTuple(tuple));
        }

        TimeMetrics metrics = new TimeMetrics();

        metrics.startEvent("serialize with compression");
        byte[] compressedData = bTree.serializeLeavesWithCompressor(CompressorFactory.compressor(CompressorFactory.Algorithm.Snappy));
        metrics.endEvent("serialize with compression");

        metrics.startEvent("serialize w.o compression");
        byte[] data = bTree.serializeLeaves();
        metrics.endEvent("serialize w.o compression");



        metrics.startEvent("compress");
        Compressor compressor = CompressorFactory.compressor(CompressorFactory.Algorithm.Snappy);
        byte[] furtherCompressed = compressor.compress(compressedData);
        metrics.endEvent("compress");

        System.out.println("Local Compression Ratio: " + (double)compressedData.length / data.length);
        System.out.println("Local + Global Compression Ratio: " + (double)furtherCompressed.length / data.length);
//        System.out.println(String.format("%.4f MB after compression", compressed.length / 1024.0 / 1024.0));
        System.out.println(metrics);

        System.out.println(String.format("%.1f MB --> %.1f MB --> %.1f MB", data.length / 1024.0 / 1024.0,
                compressedData.length / 1024.0 / 1024.0, furtherCompressed.length / 1024.0 / 1024.0));
    }
}
