package indexingTopology.util;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by acelzj on 7/21/16.
 */
public class testDiv {
    public static void main(String[] args) throws IOException {
        double a = 100.0;
        double b = 2.0;
        double result = a / b;
        /*
        System.out.println(Character.SIZE / Byte.SIZE);
        File file = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/input_data_new");
        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
        String text = bufferedReader.readLine();
        System.out.println(text);
        String [] tuple = text.split(" ");
        for (int i = 0; i < tuple.length; ++i) {
            System.out.println(tuple[i]);
        }*/
        final String s1 = new String("Hello");
        final String s2 = new String("Hello");
        System.out.println(s1.equals(s2));
        System.out.println(s1 == s2);
        final Map<Integer, Integer> map = new ConcurrentHashMap<Integer, Integer>();
        for (int i = 0; i < 100; ++i) {
            map.put(i, i);
        }
//        System.out.println(s1.equals(s2));
        Thread testThread = new Thread(new Runnable() {
            public void run() {
                int count = 0;
                while (true) {
                    for (Integer id : map.keySet()) {
                        map.remove(id);
                    }
                    if (map.keySet().size() == 0) {
                        System.out.println("count " + count);
                        break;
                    }
                    ++count;
                }
            }
        });
        testThread.start();
        System.out.println("Finished");

    }
}
