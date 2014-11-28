package com.ml.ira.match;

import com.ml.ira.IDPairWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.VarIntWritable;

import java.io.IOException;

/**
 * Created by yaming_deng on 14-4-14.
 */
public class ActionTableSplitMapper extends Mapper<IDPairWritable, VarIntWritable, VarIntWritable, MatchPairWritable> {

    public ActionTableSplitMapper() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void map(IDPairWritable key, VarIntWritable value, Context context) throws IOException, InterruptedException {
        int[] join = new int[]{key.getBID()};
        MatchPairWritable retVal = new MatchPairWritable(-1, null, -1, join, value.get());
        context.write(new VarIntWritable(key.getAID()), retVal);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
