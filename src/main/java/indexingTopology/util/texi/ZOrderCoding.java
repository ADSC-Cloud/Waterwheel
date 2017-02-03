package indexingTopology.util.texi;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Robert on 12/10/16.
 */
public class ZOrderCoding implements Serializable {


    int xBits;
    int yBits;
    public ZOrderCoding(int max) {
        xBits = (int) Math.ceil(Math.log(max)/Math.log(2));
        yBits = xBits;
    }

    private int getBits(int number) {
        int ret = number;
        for (int i = 0; i < xBits - 1; i++) {
            final int mask = 1 << (xBits - i - 1);
            ret = ret | ((mask & number) << (xBits - i - 1));
            ret = ret & (~ mask);
        }
        return  ret;
    }

    public int getZCode(int x, int y) {
        return getBits(x) | (getBits(y) << 1);
    }

    public List<Integer> getZCodesInRectangle(int x1, int x2, int y1, int y2) {
        List<Integer> codes = new ArrayList<Integer>();
        for (int i = x1; i <= x2; i++) {
            for (int j = y1; j <= y2; j++) {
                codes.add(getZCode(i, j));
            }
        }
        return codes;
    }

    public Intervals getIntervalsOfCodesInRectangle(int x1, int x2, int y1, int y2) {
        List<Integer> codes = getZCodesInRectangle(x1, x2, y1, y2);
        Intervals intervals = new Intervals();
        for(Integer code: codes) {
            intervals.addPoint(code);
        }
        return intervals;
    }


    public int getMaxZCode() {
        return (int)Math.pow(2, xBits + yBits);
    }



    static public void main(String[] args) {
        ZOrderCoding zOrderCoding = new ZOrderCoding(256);
        System.out.println("bits:" + zOrderCoding.xBits);
        System.out.println("getBits(3): " + zOrderCoding.getBits(3));
        System.out.println("getBits(4): " + zOrderCoding.getBits(4));
    }
}
