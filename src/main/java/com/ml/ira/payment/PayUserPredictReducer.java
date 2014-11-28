package com.ml.ira.payment;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.mahout.math.VarIntWritable;

import java.io.IOException;

/**
 * Created by yaming_deng on 14-4-17.
 */
public class PayUserPredictReducer extends Reducer<VarIntWritable, PredictValueWritable, VarIntWritable, PredictValueWritable> {

    private MultipleOutputs mos;

    public PayUserPredictReducer() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        mos = new MultipleOutputs(context);
    }

    @Override
    protected void reduce(VarIntWritable key, Iterable<PredictValueWritable> values, Context context) throws IOException, InterruptedException {
        String namedOutput = "pay";
        if (key.get() == 0){
            namedOutput = "nopay";
        }

        for(PredictValueWritable item : values){
            mos.write(namedOutput, NullWritable.get(), item);
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
        super.cleanup(context);
    }
}
