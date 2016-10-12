package indexingTopology;

/**
 * Created by dmir on 9/30/16.
 */
public class TestArrayList {

    public static void main(String[] args) {
        LockedList list = new LockedList();
        for (int i = 0; i < 3; ++i) {
            Thread queryThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    int i = 0;
                    while (true) {
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    long start = System.nanoTime();
//                    list.accquireReadLock();
//                        System.out.println(list.get(i++));
                        list.get(i++);
//                    list.releaseReadLock();
//                    System.out.println(System.nanoTime() - start);
                    }
                }
            });
            queryThread.start();
        }

        Thread insertThread = new Thread(new Runnable() {
            @Override
            public void run() {
                int key = 0;
                int offset = 0;
                while (true) {
//                    long start = System.nanoTime();
//                    try {
//                        Thread.sleep(1);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
                    list.accquireWriteLock();
                    list.insert(key++, offset++);
//                    System.out.println(System.nanoTime() - start);
//                    list.releaseWriteLock();
                }
            }
        });
        insertThread.start();
    }

}
