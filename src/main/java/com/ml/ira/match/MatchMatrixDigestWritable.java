package com.ml.ira.match;

import com.google.common.primitives.Longs;
import org.apache.hadoop.io.WritableComparable;
import org.apache.mahout.math.Varint;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by yaming_deng on 14-4-14.
 */
public class MatchMatrixDigestWritable implements WritableComparable<MatchMatrixDigestWritable>, Cloneable  {

    private int attr;
    private int type;
    private boolean label;
    private String value;

    public MatchMatrixDigestWritable() {
        super();
    }

    public MatchMatrixDigestWritable(int attr, int type, boolean label, String value) {
        this.attr = attr;
        this.type = type;
        this.label = label;
        this.value = value;
    }

    public int getAttr() {
        return attr;
    }

    public int getType() {
        return type;
    }

    public boolean isLabel() {
        return label;
    }

    public String getValue() {
        return value;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null){
            return false;
        }
        if (!(obj instanceof MatchMatrixDigestWritable)){
            return false;
        }
        MatchMatrixDigestWritable obj1 = (MatchMatrixDigestWritable) obj;
        if (this.attr != obj1.getAttr()){
            return false;
        }
        return true;
    }

    @Override
    protected MatchMatrixDigestWritable clone() throws CloneNotSupportedException {
        return new MatchMatrixDigestWritable(this.attr, this.type, this.label, this.value);
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append(this.attr);
        b.append("\t");
        b.append(this.type);
        b.append("\t");
        b.append(this.label ? "1" : "0");
        b.append("\t");
        b.append(this.value);
        return b.toString();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        Varint.writeSignedVarInt(attr, dataOutput);
        Varint.writeSignedVarInt(this.type, dataOutput);
        dataOutput.writeBoolean(this.label);
        dataOutput.writeUTF(this.value);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        attr = Varint.readSignedVarInt(dataInput);
        this.type = Varint.readSignedVarInt(dataInput);
        this.label = dataInput.readBoolean();
        this.value = dataInput.readUTF();
    }

    @Override
    public int compareTo(MatchMatrixDigestWritable o) {
        if (this.attr > o.getAttr()){
            return 1;
        }else if(this.attr < o.getAttr()){
            return -1;
        }
        return 0;
    }

    @Override
    public int hashCode() {
        return Longs.hashCode(this.attr);
    }
}
