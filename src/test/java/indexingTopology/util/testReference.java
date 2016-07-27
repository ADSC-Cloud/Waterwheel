package indexingTopology.util;

/**
 * Created by acelzj on 7/27/16.
 */
public class testReference {
        int a;
        public testReference() {
            a = 2;
        }

        public void setA() {
            a = 3;
        }

        public int getA() {
            return a;
        }
    public static void main(String[] args) {
        testReference apple = new testReference();
        testReference pear = apple;
        apple.setA();
        System.out.println(apple.getA());
        System.out.println(pear.getA());
    }
}
