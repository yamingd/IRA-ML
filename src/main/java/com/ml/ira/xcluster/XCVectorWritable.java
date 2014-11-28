package com.ml.ira.xcluster;

import org.apache.hadoop.io.WritableComparable;
import org.apache.mahout.math.Varint;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by yaming_deng on 14-5-14.
 */
public class XCVectorWritable implements WritableComparable<XCVectorWritable>, Cloneable {

    private int clusterId;
    private double[] vector;

    private double distance;

    public XCVectorWritable() {
    }

    public XCVectorWritable(int clusterId, double[] vector) {
        this.clusterId = clusterId;
        this.vector = vector;
        this.distance = -1;
    }

    public double[] getVector() {
        return vector;
    }

    public void setVector(double[] vector) {
        this.vector = vector;
    }

    public int getClusterId() {
        return clusterId;
    }

    public void setClusterId(int clusterId) {
        this.clusterId = clusterId;
    }

    public double getDistance() {
        return distance;
    }

    public void setDistance(double distance) {
        this.distance = distance;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Varint.writeSignedVarInt(clusterId, out);
        Varint.writeSignedVarInt(vector.length, out);
        for (int i = 0; i < vector.length; i++) {
            out.writeDouble(vector[i]);
        }
        out.writeDouble(distance);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.clusterId = Varint.readSignedVarInt(in);
        int len = Varint.readSignedVarInt(in);
        vector = new double[len];
        for (int i = 0; i < len; i++) {
            vector[i] = in.readDouble();
        }
        distance = in.readDouble();
    }

    @Override
    protected XCVectorWritable clone() throws CloneNotSupportedException {
        XCVectorWritable c = new XCVectorWritable(this.clusterId, this.vector);
        c.setDistance(this.getDistance());
        return c;
    }


    @Override
    public int compareTo(XCVectorWritable o) {
        if (o.getClusterId() > this.getClusterId()){
            return -1;
        }
        if (o.getClusterId() < this.getClusterId()){
            return 1;
        }
        if (o.getDistance() > this.getDistance()){
            return -1;
        }
        if (o.getDistance() < this.getDistance()){
            return 1;
        }
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null){
            return false;
        }
        XCVectorWritable o = (XCVectorWritable)obj;
        return this.compareTo(o) == 0;
    }

    @Override
    public String toString() {
        StringBuffer s = new StringBuffer();
        s.append(this.clusterId).append("\t");
        for (int i = 0; i < vector.length; i++) {
            s.append(vector[i]).append("\t");
        }
        s.append(distance);
        return s.toString();
    }

    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }
}
