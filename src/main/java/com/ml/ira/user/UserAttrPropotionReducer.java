package com.ml.ira.user;

import com.ml.ira.propotion.AttrValueWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.VarIntWritable;

import java.io.IOException;

/**
 * Created by yaming_deng on 14-4-15.
 */
public class UserAttrPropotionReducer extends Reducer<AttrValueWritable, VarIntWritable, AttrValueWritable, VarIntWritable> {

    public UserAttrPropotionReducer() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void reduce(AttrValueWritable key, Iterable<VarIntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(VarIntWritable item : values){
            sum += item.get();
        }
        context.write(key, new VarIntWritable(sum));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
