package indexingTopology.util;

import java.io.*;

/**
 * Created by acelzj on 7/21/16.
 */
public class testDiv {
    public static void main(String[] args) throws IOException {
        double a = 100.0;
        double b = 2.0;
        double result = a / b;
        System.out.println(Character.SIZE / Byte.SIZE);
        File file = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/input_data_new");
        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
        String text = bufferedReader.readLine();
        System.out.println(text);
        String [] tuple = text.split(" ");
        for (int i = 0; i < tuple.length; ++i) {
            System.out.println(tuple[i]);
        }
    }
}
