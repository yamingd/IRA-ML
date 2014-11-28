package com.ml.ira.xcluster;

import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.Varint;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by yaming_deng on 14-5-14.
 */
public class XClusterWritable implements Writable, Cloneable {

    private int clusterId;
    private double[] center;
    private boolean convered;

    public XClusterWritable() {
    }

    public XClusterWritable(int clusterId, double[] center) {
        this.clusterId = clusterId;
        this.center = center;
        this.convered = false;
    }

    public double[] getCenter() {
        return center;
    }

    public void setCenter(double[] center) {
        this.center = center;
    }

    public int getClusterId() {
        return clusterId;
    }

    public void setClusterId(int clusterId) {
        this.clusterId = clusterId;
    }

    public boolean isConvered() {
        return convered;
    }

    public void setConvered(boolean convered) {
        this.convered = convered;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Varint.writeSignedVarInt(clusterId, out);
        Varint.writeSignedVarInt(center.length, out);
        for (int i = 0; i < center.length; i++) {
            out.writeDouble(center[i]);
        }
        out.writeBoolean(convered);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.clusterId = Varint.readSignedVarInt(in);
        int len = Varint.readSignedVarInt(in);
        center = new double[len];
        for (int i = 0; i < len; i++) {
            center[i] = in.readDouble();
        }
        convered = in.readBoolean();
    }

    @Override
    protected XClusterWritable clone() throws CloneNotSupportedException {
        XClusterWritable c = new XClusterWritable(this.clusterId, this.center);
        return c;
    }

    @Override
    public String toString() {
        StringBuffer s = new StringBuffer();
        s.append(this.clusterId).append("\t");
        s.append( (convered ? "1": "0")).append("\t");
        for (int i = 0; i < center.length-1; i++) {
            s.append(center[i]).append("\t");
        }
        s.append(center[center.length - 1]);
        return s.toString();
    }

    @Override
    public int hashCode() {
        return this.center.hashCode();
    }
}
