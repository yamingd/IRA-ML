package com.ml.ira.match;

import com.ml.ira.Gender;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.mahout.math.VarIntWritable;

import java.io.IOException;

/**
 * 分割用户
 * Created by yaming_deng on 14-4-28.
 */
public class MatchPredictionSplitReducer extends Reducer<VarIntWritable, UserIndexWritable, NullWritable, UserIndexWritable> {

    private MultipleOutputs mos;

    public MatchPredictionSplitReducer() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        mos = new MultipleOutputs(context);
    }

    @Override
    protected void reduce(VarIntWritable key, Iterable<UserIndexWritable> values, Context context) throws IOException, InterruptedException {
        String namedOutput = "female";
        if (key.get() == Gender.Female){
        }else{
            namedOutput = "male";
        }
        for (UserIndexWritable item : values) {
            mos.write(namedOutput, NullWritable.get(), item);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
        super.cleanup(context);
    }
}
