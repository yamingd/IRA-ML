package com.ml.ira;

import com.google.common.primitives.Longs;
import org.apache.hadoop.io.WritableComparable;
import org.apache.mahout.math.Varint;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by yaming_deng on 14-4-14.
 */
public class IDPairWritable implements WritableComparable<IDPairWritable>, Cloneable  {


    private int aID;
    private int bID;

    public IDPairWritable() {
        // do nothing
    }

    public IDPairWritable(int aID, int bID) {
        this.aID = aID;
        this.bID = bID;
    }

    public int getAID() {
        return aID;
    }

    public int getBID() {
        return bID;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Varint.writeSignedVarInt(aID, out);
        Varint.writeSignedVarInt(bID, out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        aID = Varint.readSignedVarInt(in);
        bID = Varint.readSignedVarInt(in);
    }

    @Override
    public int compareTo(IDPairWritable that) {
        int aCompare = compare(aID, that.getAID());
        return aCompare == 0 ? compare(bID, that.getBID()) : aCompare;
    }

    private static int compare(long a, long b) {
        return a < b ? -1 : a > b ? 1 : 0;
    }

    @Override
    public int hashCode() {
        return Longs.hashCode(aID) + 31 * Longs.hashCode(bID);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof IDPairWritable) {
            IDPairWritable that = (IDPairWritable) o;
            return aID == that.getAID() && bID == that.getBID();
        }
        return false;
    }

    @Override
    public String toString() {
        return aID + "\t" + bID;
    }

    @Override
    public IDPairWritable clone() {
        return new IDPairWritable(aID, bID);
    }

}
