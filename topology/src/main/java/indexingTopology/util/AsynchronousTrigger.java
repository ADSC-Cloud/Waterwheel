package indexingTopology.util;

import java.util.HashMap;

/**
 * Created by robert on 15/11/17.
 */
public class AsynchronousTrigger {

    public long addFuture(Runnable runnable) {
        long ret = currentID++;
        idToRunnable.put(ret, runnable);
        return ret;
    }

    public boolean trigger(long id) {
        if (idToRunnable.containsKey(id)) {
            idToRunnable.get(id).run();
            idToRunnable.remove(id);
            return true;
        }
        return false;
    }

    private long currentID = 0;
    private HashMap<Long, Runnable> idToRunnable = new HashMap<>();

    static public void main(String[] args) {
        AsynchronousTrigger trigger = new AsynchronousTrigger();
        Integer value = 0;
        long id = trigger.addFuture(() -> value++);
        boolean triggered = trigger.trigger(id);
        if (triggered)
            System.out.println("Triggered!\n");
    }
}
