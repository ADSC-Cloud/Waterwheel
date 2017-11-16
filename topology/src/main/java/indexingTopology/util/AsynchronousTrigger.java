package indexingTopology.util;

import java.util.HashMap;
import java.util.function.Function;

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

    public long addFunction(Function function) {
        long ret = currentID++;
        idToFunction.put(ret, function);
        return ret;
    }

    public Object triggerFunction(long id, Object argument) {
        Function function = idToFunction.get(id);
        if (function != null) {
            idToFunction.remove(id);
            return function.apply(argument);
        } else {
            return null;
        }
    }

    private long currentID = 0;
    private HashMap<Long, Runnable> idToRunnable = new HashMap<>();

    private HashMap<Long, Function> idToFunction = new HashMap<>();

    static public void main(String[] args) {

    }
}
