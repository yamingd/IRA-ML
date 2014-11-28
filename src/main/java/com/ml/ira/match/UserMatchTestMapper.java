package com.ml.ira.match;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 模型测试记录构建
 * Created by yaming_deng on 14-4-14.
 */
public class UserMatchTestMapper extends Mapper<NullWritable, MatchPairWritable, MatchMatrixRowWritable, NullWritable> {

    public UserMatchTestMapper() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void map(NullWritable key, MatchPairWritable value, Context context) throws IOException, InterruptedException {
        int[] columns = new int[value.getGroup().length + value.getJoin().length+1];
        int i = 0, j=0;
        int size = value.getGroup().length;
        for(i=0; i<size; i++){
            columns[i] = value.getGroup()[i];
        }
        size = value.getJoin().length;
        for(j=0; j<size; j++){
            columns[i] = value.getJoin()[j];
            i++;
        }
        columns[i] = value.getResult();
        MatchMatrixRowWritable row = new MatchMatrixRowWritable(columns);
        context.write(row, NullWritable.get());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
