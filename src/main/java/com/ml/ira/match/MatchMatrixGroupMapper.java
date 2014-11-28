package com.ml.ira.match;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.VarIntWritable;

import java.io.IOException;

/**
 * 读取Matrix左连接记录.
 * Created by yaming_deng on 14-4-14.
 */
public class MatchMatrixGroupMapper extends Mapper<VarIntWritable, MatchPairWritable, VarIntWritable, MatchPairWritable> {

    public MatchMatrixGroupMapper() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void map(VarIntWritable key, MatchPairWritable value, Context context) throws IOException, InterruptedException {
        context.write(key, value);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
