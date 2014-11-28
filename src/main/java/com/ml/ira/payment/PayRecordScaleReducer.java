package com.ml.ira.payment;

import com.ml.ira.AppConfig;
import com.ml.ira.Constants;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.VarIntWritable;

import java.io.IOException;

/**
 * Created by yaming_deng on 14-4-17.
 */
public class PayRecordScaleReducer extends Reducer<PayRecordScaleWritable, VarIntWritable, PayRecordScaleWritable, VarIntWritable> {

    private AppConfig appConfig = null;

    public PayRecordScaleReducer() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        appConfig = new AppConfig(Constants.PAYMENT_CONF);
    }

    @Override
    protected void reduce(PayRecordScaleWritable key, Iterable<VarIntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for(VarIntWritable item : values){
            count += item.get();
        }
        context.write(key, new VarIntWritable(count));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
