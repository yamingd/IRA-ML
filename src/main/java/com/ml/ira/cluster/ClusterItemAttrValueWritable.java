package com.ml.ira.cluster;

import com.google.common.hash.HashCode;
import org.apache.hadoop.io.WritableComparable;
import org.apache.mahout.math.Varint;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by yaming_deng on 14-4-14.
 */
public class ClusterItemAttrValueWritable implements WritableComparable<ClusterItemAttrValueWritable>, Cloneable  {

    private int clusterId;
    private int attr;
    private int value;
    private String name;

    public ClusterItemAttrValueWritable() {
    }

    public ClusterItemAttrValueWritable(int clusterId, int attr, int value, String name) {
        this.clusterId = clusterId;
        this.attr = attr;
        this.value = value;
        this.name = name;
    }

    public void setClusterId(int clusterId) {
        this.clusterId = clusterId;
    }

    public int getClusterId() {
        return clusterId;
    }

    public String getName() {
        return name;
    }

    public void setAttr(int attr) {
        this.attr = attr;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAttr() {
        return attr;
    }

    public int getValue() {
        return value;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Varint.writeSignedVarInt(clusterId, out);
        Varint.writeSignedVarInt(attr, out);
        Varint.writeSignedVarInt(value, out);
        out.writeUTF(name);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        clusterId = Varint.readSignedVarInt(in);
        attr = Varint.readSignedVarInt(in);
        value = Varint.readSignedVarInt(in);
        name = in.readUTF();
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
        if (!(obj instanceof ClusterItemAttrValueWritable)) {
            return false;
        }
        ClusterItemAttrValueWritable vo = (ClusterItemAttrValueWritable)obj;
        return clusterId == vo.getClusterId() && attr == vo.getAttr() && value == vo.getValue();
    }

    @Override
    protected ClusterItemAttrValueWritable clone() throws CloneNotSupportedException {
        return new ClusterItemAttrValueWritable(clusterId, attr, value, name);
    }

    @Override
    public String toString() {
        return clusterId+"\t"+attr+"\t"+value+"\t"+name;
    }

    @Override
    public int compareTo(ClusterItemAttrValueWritable o) {
        if (clusterId > o.getClusterId()){
            return 1;
        }
        if (clusterId < o.getClusterId()){
            return -1;
        }
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
