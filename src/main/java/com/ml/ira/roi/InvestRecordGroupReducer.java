package com.ml.ira.roi;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * Created by yaming_deng on 14-4-25.
 */
public class InvestRecordGroupReducer extends Reducer<InvestGroupWritable, InvestRecordWritable, InvestGroupWritable, InvestRecordWritable> {

    private MultipleOutputs mos;

    public InvestRecordGroupReducer() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        mos = new MultipleOutputs(context);
    }

    @Override
    protected void reduce(InvestGroupWritable key, Iterable<InvestRecordWritable> values, Context context) throws IOException, InterruptedException {
        float[] ret = null;
        for (InvestRecordWritable item : values){
            if (ret == null){
                ret = item.getColumns();
            }else{
                ret = addUp(ret, item.getColumns());
            }
        }
        String namedOutput = "test";
        if (key.getType() == 1){
            namedOutput = "train";
        }
        mos.write(namedOutput, key, new InvestRecordWritable(ret));
    }

    private float[] addUp(float[] ret, float[] vals){
        for (int i = 0; i < ret.length; i++) {
            ret[i] += vals[i];
        }
        return ret;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
        super.cleanup(context);
    }
}
