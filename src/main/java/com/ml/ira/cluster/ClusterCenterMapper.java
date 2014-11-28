package com.ml.ira.cluster;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;

/**
 * 读取Center
 * Created by yaming_deng on 14-4-29.
 */
public class ClusterCenterMapper extends Mapper<VarIntWritable, ClusterWritable, VarIntWritable, VectorWritable> {

    private VectorWritable outval = new VectorWritable();

    public ClusterCenterMapper() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void map(VarIntWritable key, ClusterWritable value, Context context) throws IOException, InterruptedException {
        key.set(value.getValue().getId());
        outval.set(value.getValue().getCenter());
        context.write(key, outval);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
