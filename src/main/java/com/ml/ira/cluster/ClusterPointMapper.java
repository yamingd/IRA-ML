package com.ml.ira.cluster;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;

/**
 * 读取向量和聚类的记录
 * Created by yaming_deng on 14-4-29.
 */
public class ClusterPointMapper extends Mapper<IntWritable, WeightedPropertyVectorWritable, IntWritable, WeightedPropertyVectorWritable> {

    private VectorWritable outVal = new VectorWritable();

    public ClusterPointMapper() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void map(IntWritable key, WeightedPropertyVectorWritable value, Context context) throws IOException, InterruptedException {
        context.write(key, value);
        /*int clusterId = key.get();
        double weight = value.getWeight();
        NamedVector vector0 = (NamedVector)value.getVector();
        String itemid = vector0.getName();

        Vector vector = new RandomAccessSparseVector(4);
        vector.setQuick(0, 0);
        vector.setQuick(1, clusterId);
        vector.setQuick(2, weight);
        vector.setQuick(3, Integer.parseInt(itemid));
        outVal.set(vector);
        context.write(key, outVal);*/
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
