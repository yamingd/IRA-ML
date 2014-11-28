package com.ml.ira.payment;

import org.apache.hadoop.io.WritableComparable;
import org.apache.mahout.math.Varint;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by yaming_deng on 14-4-14.
 */
public class PayRecordScaleWritable implements WritableComparable<PayRecordScaleWritable>, Cloneable  {

    private int payed;
    private int attrIdx;
    private int value;

    public PayRecordScaleWritable() {
    }

    public PayRecordScaleWritable(int payed, int attrIdx, int value) {
        this.payed = payed;
        this.attrIdx = attrIdx;
        this.value = value;
    }

    public int getAttrIdx() {
        return attrIdx;
    }

    public int getValue() {
        return value;
    }

    public int getPayed() {
        return payed;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Varint.writeSignedVarInt(payed, out);
        Varint.writeSignedVarInt(attrIdx, out);
        Varint.writeSignedVarInt(value, out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        payed = Varint.readSignedVarInt(in);
        attrIdx = Varint.readSignedVarInt(in);
        value = Varint.readSignedVarInt(in);
    }

    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return false;
        }
        if (!(obj instanceof PayRecordScaleWritable)) {
            return false;
        }
        PayRecordScaleWritable vo = (PayRecordScaleWritable)obj;
        return this.payed == vo.getPayed() && this.attrIdx == vo.getAttrIdx() && this.value == vo.getValue();
    }

    @Override
    protected PayRecordScaleWritable clone() throws CloneNotSupportedException {
        return new PayRecordScaleWritable(this.payed, this.attrIdx, this.value);
    }

    @Override
    public String toString() {
       return String.format("%d\t%d\t%d", this.payed, this.attrIdx, this.value);
    }

    @Override
    public int compareTo(PayRecordScaleWritable o) {
        if (this.payed > o.getPayed()){
            return 1;
        }else if(this.payed < o.getPayed()){
            return -1;
        }
        if (this.attrIdx > o.getAttrIdx()){
            return 1;
        }else if(this.attrIdx < o.getAttrIdx()){
            return -1;
        }
        if (this.value > o.getValue()){
            return 1;
        }else if(this.value < o.getValue()){
            return -1;
        }
        return 0;
    }
}
