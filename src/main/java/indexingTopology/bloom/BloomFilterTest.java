package indexingTopology.bloom;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.google.common.io.LittleEndianDataInputStream;
import com.sun.glass.ui.SystemClipboard;

import java.io.*;

/**
 * Created by Robert on 3/14/17.
 */
public class BloomFilterTest {
    public static void main(String[] arsg) throws IOException {
        BloomFilter<Long> bloomFilter = BloomFilter.create(Funnels.longFunnel(), 10000, 0.003);
        BloomFilter<CharSequence> charSequenceBloomFilter = BloomFilter.create(Funnels.stringFunnel(Charsets.UTF_8),1000);


        for (int i = 0; i < 500; i++) {
            bloomFilter.put(i + 0L);
            charSequenceBloomFilter.put("" + i);
        }

        for (int i = 500; i < 1000; i++) {
            if (bloomFilter.mightContain(i + 0L)) {
                System.out.println("bloomFilter: False Positive!");
            }
        }

        for (int i = 500; i < 1000; i++) {
            if (charSequenceBloomFilter.mightContain(i+ "")) {
                System.out.println("charSequenceBloomFilter: False Positive!");
            }
        }

        ByteArrayOutputStream os = new ByteArrayOutputStream(10000);
        bloomFilter.writeTo(os);
        System.out.println(os.toByteArray().length + " bits");


    }
}
