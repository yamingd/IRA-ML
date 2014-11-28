package com.ml.ira.cluster;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;

/**
 * Created by yaming_deng on 14-4-29.
 */
public class ClusterDataPrepareReducer extends Reducer<VarIntWritable, VectorWritable, Text, VectorWritable> {

    private Text outkey = new Text();

    public ClusterDataPrepareReducer() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void reduce(VarIntWritable key, Iterable<VectorWritable> values, Context context) throws IOException, InterruptedException {
        for (VectorWritable item : values){
            double itemId = item.get().get(0);
            item.set(this.copy(item.get()));
            outkey.set(String.valueOf(itemId));
            context.write(outkey, item);
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
        super.cleanup(context);
    }
}
