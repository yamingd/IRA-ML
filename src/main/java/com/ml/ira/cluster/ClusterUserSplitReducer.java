package com.ml.ira.cluster;

import com.ml.ira.Gender;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;

/**
 * Created by yaming_deng on 14-4-29.
 */
public class ClusterUserSplitReducer extends Reducer<VarIntWritable, VectorWritable, NullWritable, VectorWritable> {

    private MultipleOutputs mos;

    public ClusterUserSplitReducer() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        mos = new MultipleOutputs(context);
    }

    @Override
    protected void reduce(VarIntWritable key, Iterable<VectorWritable> values, Context context) throws IOException, InterruptedException {
        String namedOutput = "female";
        if (key.get() == Gender.Male){
            namedOutput = "male";
        }
        for (VectorWritable item : values){
            double itemId = item.get().get(0);
            item.set(this.copy(item.get()));
            mos.write(namedOutput, new Text(String.valueOf(itemId)), item);
        }
    }

    private RandomAccessSparseVector copy(Vector vector){
        int size = vector.size() - 1;
        RandomAccessSparseVector ret = new RandomAccessSparseVector(size);
        for (int i = 0; i < size; i++) {
            ret.setQuick(i, vector.get(i+1));
        }
        return ret;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
        super.cleanup(context);
    }
}
