package com.ml.ira.roi;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.Varint;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by yaming_deng on 14-4-25.
 */
public class InvestRecordWritable implements Writable, Cloneable   {

    private float[] columns;

    public InvestRecordWritable() {
    }

    public InvestRecordWritable(float[] columns) {
        this.columns = columns;
    }

    public float[] getColumns() {
        return columns;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Varint.writeSignedVarInt(columns.length, out);
        for (int i = 0; i < columns.length; i++) {
            out.writeFloat(columns[i]);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int len = Varint.readSignedVarInt(in);
        columns = new float[len];
        for (int i = 0; i < len; i++) {
            columns[i] = in.readFloat();
        }
    }

    @Override
    protected InvestRecordWritable clone() throws CloneNotSupportedException {
        return new InvestRecordWritable(this.columns);
    }

    @Override
    public String toString() {
        return StringUtils.join(columns, ",");
    }
}
