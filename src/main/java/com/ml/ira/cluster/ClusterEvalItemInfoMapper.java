package com.ml.ira.cluster;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;

/**
 * 计算分类结果记录和中心的距离来做Scatter Plot.
 * Created by yaming_deng on 14-4-29.
 */
public class ClusterEvalItemInfoMapper extends Mapper<LongWritable, Text, VarIntWritable, VectorWritable> {

    private VarIntWritable outkey = new VarIntWritable();
    private VectorWritable outval = new VectorWritable();

    public ClusterEvalItemInfoMapper() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] temp = value.toString().split("\t");
        outkey.set(Integer.parseInt(temp[0])); //cluster id
        Vector vector = new RandomAccessSparseVector(temp.length - 2);
        for (int i=0; i<temp.length - 2; i++){
            vector.setQuick(i, Double.parseDouble(temp[i+2]));
        }
        context.write(outkey, outval);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
