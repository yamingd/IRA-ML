package com.ml.ira.payment;

import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.Varint;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by yaming_deng on 14-4-14.
 */
public class PayRecordWritable implements Writable, Cloneable  {

    private float[] value;

    public PayRecordWritable() {
    }

    public PayRecordWritable(float[] value) {
        this.value = value;
    }

    public float[] getValue() {
        return value;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Varint.writeSignedVarInt(value.length, out);
        for (float v : value) {
            out.writeFloat(v);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int len = Varint.readSignedVarInt(in);
        value = new float[len];
        for(int i=0; i<len; i++){
            value[i] = in.readFloat();
        }
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof PayRecordWritable)) {
            return false;
        }
        PayRecordWritable vo = (PayRecordWritable)obj;
        return this.value.equals(vo.getValue());
    }

    @Override
    protected PayRecordWritable clone() throws CloneNotSupportedException {
        return new PayRecordWritable(this.value);
    }

    @Override
    public String toString() {
       StringBuilder s = new StringBuilder(100);
       int size = this.value.length;
       for(int i=0; i<size-1; i++){
           s.append(this.value[i]);
           s.append(",");
       }
       s.append(this.value[size-1]);
       return s.toString();
    }
}
