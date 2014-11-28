package MatchMaker;

import com.ml.ira.ObjectSizeFetcher;
import junit.framework.TestCase;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.vectorizer.encoders.StaticWordValueEncoder;
import org.apache.mahout.vectorizer.encoders.WordValueEncoder;

/**
 * Created by yaming_deng on 14-4-18.
 */
public class EncoderTest extends TestCase {

    public void testWordValueEncoder() throws Exception {
        WordValueEncoder encoder = new StaticWordValueEncoder("abc");
        Vector input = new RandomAccessSparseVector(100);
        encoder.addToVector("5", input);
        System.out.println(input.asFormatString());
    }

    public void testVector() throws Exception {
        Vector vector = new RandomAccessSparseVector(10);
        vector.setQuick(0, 1.0);
        vector.setQuick(1, 2.0);
        vector.setQuick(2, 3.0);
        vector.setQuick(3, 0.0);
        int count = vector.getNumNonZeroElements();
    }

    public void testBytes() throws Exception {
        Object o = new Integer(1);
        System.out.println(ObjectSizeFetcher.getObjectSize(o));
        Integer i2 = new Integer(1);
        System.out.println(ObjectSizeFetcher.getObjectSize(i2));
    }
}
