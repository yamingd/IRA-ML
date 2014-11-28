package com.ml.ira.user;

import com.ml.ira.Gender;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;

/**
 * Created by yaming_deng on 14-4-29.
 */
public class UserGenderSplitReducer extends Reducer<VarIntWritable, VectorWritable, NullWritable, VectorWritable> {

    private MultipleOutputs mos;

    public UserGenderSplitReducer() {
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
            mos.write(namedOutput, NullWritable.get(), item);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
        super.cleanup(context);
    }
}
