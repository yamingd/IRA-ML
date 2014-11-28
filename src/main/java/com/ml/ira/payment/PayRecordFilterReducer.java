package com.ml.ira.payment;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.mahout.math.VarIntWritable;

import java.io.IOException;

/**
 * Created by yaming_deng on 14-4-17.
 */
public class PayRecordFilterReducer extends Reducer<VarIntWritable, PayRecordWritable, NullWritable, PayRecordWritable> {


    private MultipleOutputs mos;

    public PayRecordFilterReducer() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        mos = new MultipleOutputs(context);
    }

    @Override
    protected void reduce(VarIntWritable key, Iterable<PayRecordWritable> values, Context context) throws IOException, InterruptedException {
        String namedOutput = "test";
        if (key.get() == 1){
            namedOutput = "train";
        }

        for(PayRecordWritable item : values){
            mos.write(namedOutput, NullWritable.get(), item);
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
        super.cleanup(context);
    }
}
