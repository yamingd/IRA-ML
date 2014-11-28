package com.ml.ira.propotion;

import com.google.common.hash.HashCode;
import org.apache.hadoop.io.WritableComparable;
import org.apache.mahout.math.Varint;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by yaming_deng on 14-4-14.
 */
public class AttrValueWritable implements WritableComparable<AttrValueWritable>, Cloneable  {

    private int attr;
    private int value;
    private String name;
    private int count = 0;

    public AttrValueWritable() {
    }

    public AttrValueWritable(int attr, int value, String name) {
        this.attr = attr;
        this.value = value;
        this.name = name;
    }

    public int getAttr() {
        return attr;
    }

    public int getValue() {
        return value;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(name);
        Varint.writeSignedVarInt(attr, out);
        Varint.writeSignedVarInt(value, out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        name = in.readUTF();
        attr = Varint.readSignedVarInt(in);
        value = Varint.readSignedVarInt(in);
    }

    @Override
    public int hashCode() {
        return HashCode.fromString(this.toString()).asInt();
    }

    @Override
    public boolean equals(Object obj) {
        if (null == obj) {
            return false;
        }
        if (!(obj instanceof AttrValueWritable)) {
            return false;
        }
        AttrValueWritable vo = (AttrValueWritable)obj;
        return attr == vo.getAttr() && value == vo.getValue();
    }

    @Override
    protected AttrValueWritable clone() throws CloneNotSupportedException {
        return new AttrValueWritable(attr, value, name);
    }

    @Override
    public String toString() {
        return name+"\t" + attr+"\t"+value;
    }

    @Override
    public int compareTo(AttrValueWritable o) {
        if (attr > o.getAttr()){
            return 1;
        }
        if (attr < o.getAttr()){
            return -1;
        }
        if (value > o.getValue()){
            return 1;
        }
        if (value < o.getValue()){
            return -1;
        }
        return 0;
    }
}
