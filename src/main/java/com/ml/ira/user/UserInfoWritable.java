package com.ml.ira.user;

import com.google.common.hash.HashCode;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.classifier.df.DFUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/**
 * Created by yaming_deng on 14-4-14.
 */
public class UserInfoWritable implements Writable, Cloneable  {

    private int[] values;

    public UserInfoWritable(int[] values) {
        this.values = values;
    }

    public int[] getValues() {
        return values;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        DFUtils.writeArray(out, values);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        values = DFUtils.readIntArray(in);
    }

    @Override
    public int hashCode() {
        return HashCode.fromString(this.toString()).asInt();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof UserInfoWritable)) {
            return false;
        }
        UserInfoWritable vo = (UserInfoWritable)obj;
        return Arrays.equals(values, vo.getValues());
    }

    @Override
    protected UserInfoWritable clone() throws CloneNotSupportedException {
        return new UserInfoWritable(values);
    }

    @Override
    public String toString() {
        int size = values.length;
        int i=0;
        StringBuilder b = new StringBuilder();
        for(i=0; i<size-1; i++){
            b.append(values[i]).append(",");
        }
        b.append(values[i]);
        return b.toString();
    }
}
