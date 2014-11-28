package com.ml.ira.roi;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.WritableComparable;
import org.apache.mahout.math.Varint;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by yaming_deng on 14-4-25.
 */
public class InvestGroupWritable implements WritableComparable<InvestGroupWritable>, Cloneable {

    private String[] columns;
    private int type;

    public InvestGroupWritable() {
    }

    public InvestGroupWritable(int type, String[] columns) {
        this.type = type;
        this.columns = columns;
    }

    public String[] getColumns() {
        return columns;
    }

    public int getType() {
        return type;
    }

    @Override
    public int compareTo(InvestGroupWritable o) {
        if (o == null || o.columns == null)
            return 1;
        if (this.type > o.getType()){
            return 1;
        } else if(this.type < o.getType()){
            return -1;
        }
        for (int i = 0; i < columns.length; i++) {
            int cmp = columns[i].compareTo(o.getColumns()[i]);
            if (cmp!=0){
                return cmp;
            }
        }
        return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Varint.writeSignedVarInt(type, out);
        Varint.writeSignedVarInt(columns.length, out);
        for (int i = 0; i < columns.length; i++) {
            out.writeUTF(columns[i]);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.type = Varint.readSignedVarInt(in);
        int len = Varint.readSignedVarInt(in);
        columns = new String[len];
        for (int i = 0; i < len; i++) {
            columns[i] = in.readUTF();
        }
    }

    @Override
    protected InvestGroupWritable clone() throws CloneNotSupportedException {
        return new InvestGroupWritable(this.type, this.columns);
    }

    @Override
    public String toString() {
        return StringUtils.join(columns, ",") + ",";
    }

    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }
}
