package com.ml.ira.payment;

import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.Varint;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by yaming_deng on 14-4-18.
 */
public class PredictValueWritable implements Writable, Cloneable {

    private int userId;
    private int cityId;
    private int gender;
    private int iszat;
    private double score;
    private int target;
    private double likelihood; // [-100, 0], 越接近0越好

    public PredictValueWritable() {
    }

    public PredictValueWritable(int userId, int cityId, int iszat, double score, int target, int gender, double likelihood) {
        this.userId = userId;
        this.cityId = cityId;
        this.iszat = iszat;
        this.score = score;
        this.target = target;
        this.gender = gender;
        this.likelihood = likelihood;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Varint.writeSignedVarInt(userId, out);
        Varint.writeSignedVarInt(cityId, out);
        Varint.writeSignedVarInt(iszat, out);
        Varint.writeSignedVarInt(target, out);
        Varint.writeSignedVarInt(gender, out);
        out.writeDouble(score);
        out.writeDouble(likelihood);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        userId = Varint.readSignedVarInt(in);
        cityId = Varint.readSignedVarInt(in);
        iszat = Varint.readSignedVarInt(in);
        target = Varint.readSignedVarInt(in);
        gender = Varint.readSignedVarInt(in);
        score = in.readDouble();
        likelihood = in.readDouble();
    }

    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null){
            return false;
        }
        if (! (obj instanceof PredictValueWritable)){
            return false;
        }
        PredictValueWritable o = (PredictValueWritable)obj;
        return userId == o.userId;
    }

    @Override
    protected PredictValueWritable clone() throws CloneNotSupportedException {
        return new PredictValueWritable(this.userId, this.cityId, this.iszat, this.score, this.target, this.gender, this.likelihood);
    }

    @Override
    public String toString() {
        StringBuilder s = new StringBuilder(50);
        s.append(userId);
        s.append("\t");
        s.append(cityId);
        s.append("\t");
        s.append(gender);
        s.append("\t");
        s.append(iszat);
        s.append("\t");
        s.append(target);
        s.append("\t");
        s.append(score);
        s.append("\t");
        s.append(likelihood);
        return s.toString();
    }
}
