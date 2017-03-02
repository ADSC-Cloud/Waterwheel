package indexingTopology.util;

import java.io.Serializable;
import java.util.*;

/**
 * Created by acelzj on 13/2/17.
 */
public class Permutation implements Serializable{

    private int keys;
    List<Integer> list;


    public Permutation(int keys) {
        this.keys = keys;
        shuffle();
    }

    public Permutation(int keys, long seed) {
        this.keys = keys;
        shuffle(seed);
    }

    public synchronized void shuffle() {
        shuffle(System.currentTimeMillis());
    }

    public synchronized void shuffle(long seed) {
        list = new ArrayList<>();
        for (int i = 0; i < keys; i++) {
            list.add(i);
        }
        Collections.shuffle(list, new Random(seed));
    }

    public synchronized Integer get(int index) {
        return list.get(index);
    }

    public String toString() {
        String ret = "";
        for (Integer key: list) {
            ret += key + " ";
        }
        return ret;
    }
}
