package com.ml.ira.xcluster;

import com.ml.ira.Gender;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.mahout.math.VarIntWritable;

import java.io.IOException;

/**
 * Created by yaming_deng on 14-4-29.
 */
public class RDReducer extends Reducer<VarIntWritable, Text, NullWritable, Text> {

    private MultipleOutputs mos;

    public RDReducer() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        mos = new MultipleOutputs(context);
    }

    @Override
    protected void reduce(VarIntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int keyid = key.get() % 10;
        int keyds = key.get() / 10 - 1;
        String namedOutput = "female";
        if (keyid == Gender.Male){
            namedOutput = "male";
        }
        for (Text item : values){
            mos.write(namedOutput + keyds, NullWritable.get(), item);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
        super.cleanup(context);
    }
}
