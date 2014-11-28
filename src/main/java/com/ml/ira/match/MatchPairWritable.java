package com.ml.ira.match;

import org.apache.hadoop.io.Writable;
import org.apache.mahout.classifier.df.DFUtils;
import org.apache.mahout.math.Varint;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/**
 * Created by yaming_deng on 14-4-14.
 */
public class MatchPairWritable implements Writable, Cloneable  {

    private int gender0;
    private int[] group;
    private int gender1;
    private int[] join;
    private int result;

    public MatchPairWritable() {
        super();
    }

    public MatchPairWritable(int gender0, int[] group, int gender1, int[] join, int result) {
        this.gender0 = gender0;
        this.group = group;
        this.gender1 = gender1;
        this.join = join;
        this.result = result;
    }

    public int getGender0() {
        return gender0;
    }

    public int getGender1() {
        return gender1;
    }

    public int[] getGroup() {
        return group;
    }

    public int[] getJoin() {
        return join;
    }

    public int getResult() {
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null){
            return false;
        }
        if (!(obj instanceof MatchPairWritable)){
            return false;
        }
        MatchPairWritable obj1 = (MatchPairWritable) obj;
        if (this.result != obj1.getResult()){
            return false;
        }
        if (!Arrays.equals(this.getGroup(), obj1.getGroup())){
            return false;
        }
        if (!Arrays.equals(this.getJoin(), obj1.getJoin())){
            return false;
        }
        return true;
    }

    @Override
    protected MatchPairWritable clone() throws CloneNotSupportedException {
        return new MatchPairWritable(gender0, group, gender1, join, result);
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        if (group!=null){
            b.append(gender0).append("\t");
            for(int i=0; i<group.length; i++){
                b.append(group[i]).append("\t");
            }
        }else{
            b.append("*").append("\t");
        }
        if (join!=null){
            b.append(gender1).append("\t");
            for(int i=0; i<join.length; i++){
                b.append(join[i]).append("\t");
            }
        }else{
            b.append("*").append("\t");
        }
        b.append(this.result);
        return b.toString();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        if (group == null){
            dataOutput.writeBoolean(false);
        }else{
            dataOutput.writeBoolean(true);
            Varint.writeSignedVarInt(gender0, dataOutput);
            DFUtils.writeArray(dataOutput, this.getGroup());
        }
        if (join == null){
            dataOutput.writeBoolean(false);
        }else{
            dataOutput.writeBoolean(true);
            Varint.writeSignedVarInt(gender1, dataOutput);
            DFUtils.writeArray(dataOutput, this.getJoin());
        }
        Varint.writeSignedVarInt(result, dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        boolean exists = dataInput.readBoolean();
        if (exists){
            this.gender0 = Varint.readSignedVarInt(dataInput);
            this.group = DFUtils.readIntArray(dataInput);
        }
        exists = dataInput.readBoolean();
        if (exists){
            this.gender1 = Varint.readSignedVarInt(dataInput);
            this.join = DFUtils.readIntArray(dataInput);
        }
        result = Varint.readSignedVarInt(dataInput);
    }
}
