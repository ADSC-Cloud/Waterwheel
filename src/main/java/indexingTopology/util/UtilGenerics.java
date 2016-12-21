package indexingTopology.util;

import org.apache.storm.tuple.Tuple;
import indexingTopology.exception.UnsupportedGenericException;
import org.apache.commons.lang.SerializationUtils;

import javax.sql.rowset.serial.SerialArray;
import java.nio.ByteBuffer;

/**
 * Created by parijatmazumdar on 08/10/15.
 */
public class UtilGenerics {
    public static int sizeOf(Class c) throws UnsupportedGenericException {
        if (c==Double.class)
            return 8;
        else if (c==Integer.class)
            return 4;
        else
            throw new UnsupportedGenericException(c.getName()+" is not supported yet!");
    }

    public static <TKey> void putIntoByteBuffer(ByteBuffer b, TKey k) throws UnsupportedGenericException {
        if (k.getClass()==Double.class)
            b.putDouble((Double) k);
        else if (k.getClass()==Integer.class)
            b.putInt((Integer) k);
        else
            throw new UnsupportedGenericException("key type not supported");
    }

    public static <TValue> byte[] toBytes(TValue v) throws UnsupportedGenericException {
        if (v.getClass()== Tuple.class)
            return ((Tuple) v).toString().getBytes();
        else
            throw new UnsupportedGenericException("value type not supported");
    }
}
