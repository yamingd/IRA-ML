package com.ml.ira.cluster;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.VarIntWritable;

import java.io.IOException;

/**
 * 聚类中的属性值分布
 * Created by yaming_deng on 14-4-29.
 */
public class ClusterItemInfoPropReducer extends Reducer<ClusterItemAttrValueWritable, VarIntWritable, ClusterItemAttrValueWritable, VarIntWritable> {

    private VarIntWritable outval = new VarIntWritable(1);

    public ClusterItemInfoPropReducer() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void reduce(ClusterItemAttrValueWritable key, Iterable<VarIntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(VarIntWritable i : values){
            sum += i.get();
        }
        outval.set(sum);
        context.write(key, outval);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
