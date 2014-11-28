package com.ml.ira.match;

import org.apache.hadoop.io.WritableComparable;
import org.apache.mahout.classifier.df.DFUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/**
 * Created by yaming_deng on 14-4-14.
 */
public class UserMatchSocreWritable implements WritableComparable<UserMatchSocreWritable>, Cloneable  {

    private double[] columns;

    public UserMatchSocreWritable() {
        super();
    }

    public UserMatchSocreWritable(double[] columns) {
        this.columns = columns;
    }

    public double[] getColumns() {
        return columns;
    }


    @Override
    public boolean equals(Object obj) {
        if (obj == null){
            return false;
        }
        if (!(obj instanceof UserMatchSocreWritable)){
            return false;
        }
        UserMatchSocreWritable obj1 = (UserMatchSocreWritable) obj;
        if (!Arrays.equals(this.columns, obj1.getColumns())){
            return false;
        }
        return true;
    }

    @Override
    protected UserMatchSocreWritable clone() throws CloneNotSupportedException {
        return new UserMatchSocreWritable(columns);
    }

    @Override
    public String toString() {
        int size = columns.length;
        StringBuilder b = new StringBuilder();
        for(int i=0; i<size-1; i++){
            b.append(columns[i]).append(",");
        }
        b.append(columns[size-1]);
        return b.toString();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        if (columns == null){
            dataOutput.writeBoolean(false);
        }else{
            dataOutput.writeBoolean(true);
            DFUtils.writeArray(dataOutput, columns);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        boolean exists = dataInput.readBoolean();
        if (exists){
            this.columns = DFUtils.readDoubleArray(dataInput);
        }
    }

    @Override
    public int compareTo(UserMatchSocreWritable o) {
        for(int i=0; i<columns.length; i++){
            if (columns[i] != o.getColumns()[i]){
                if (columns[i] > o.getColumns()[i]){
                    return 1;
                }else{
                    return -1;
                }
            }
        }
        return 0;
    }
}
