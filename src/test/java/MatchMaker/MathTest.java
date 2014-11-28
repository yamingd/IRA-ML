package MatchMaker;

import junit.framework.TestCase;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.function.Functions;

import java.util.Random;

/**
 * Created by yaming_deng on 14-5-12.
 */
public class MathTest extends TestCase {

    Random random;
    Vector vector;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        random = new Random();
        vector = genVector(5);
        System.out.println(vector.toString());
    }

    private Vector genVector(int size){
        Vector vector = new RandomAccessSparseVector(size);
        for (int i=0; i<size; i++){
            vector.set(i, random.nextInt(10));
        }
        return vector;
    }

    public void testLengthSquard() throws Exception {
        // length(x,y,...) = sqrt( xx + yy + ...);
        // vector (3,4) = sqrt(33 + 44) = sqrt(25) = 5
        System.out.println(vector.getLengthSquared());
    }

    public void testAssign() throws Exception {
        Vector v2 = genVector(5);
        System.out.println(v2.toString());
        vector.assign(v2, Functions.PLUS);
        System.out.println(vector.toString());
    }

    public void testTimes() throws Exception {
        Vector v2 = vector.times(vector);
        System.out.println(v2.toString());
    }

    public void testDistanceSquared() throws Exception {
        Vector v2 = genVector(5);
        System.out.println(v2.toString());
        double distance = vector.getDistanceSquared(v2);
        System.out.println(distance);
    }

    public void testZSum() throws Exception {
        System.out.println(vector.zSum());
    }

    public void testZeroSet(){
        vector.setQuick(0, 0);
        System.out.println(vector);
        System.out.println(vector.size());
    }
}
