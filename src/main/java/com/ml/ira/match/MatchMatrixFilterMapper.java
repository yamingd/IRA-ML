package com.ml.ira.match;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.VarIntWritable;

import java.io.IOException;

/**
 * 过滤Matrix中相互冲突和重复的记录.
 * Created by yaming_deng on 14-4-14.
 */
public class MatchMatrixFilterMapper extends Mapper<NullWritable, MatchPairWritable, MatchMatrixRowWritable, VarIntWritable> {

    public MatchMatrixFilterMapper() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void map(NullWritable key, MatchPairWritable value, Context context) throws IOException, InterruptedException {
        if (value.getResult() < 0){
            System.err.println("MatchMatrixFilterMapper Negative MatchPairWritable's Result. key=" + key.get());
            return;
        }
        MatchPairWritable value0 = null;
        try {
            value0 = value.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        int[] columns = new int[value0.getGroup().length + value0.getJoin().length];
        int i = 0, j=0;
        int size = value0.getGroup().length;
        for(i=0; i<size; i++){
            columns[i] = value0.getGroup()[i];
        }
        size = value0.getJoin().length;
        for(j=0; j<size; j++){
            columns[i] = value0.getJoin()[j];
            i++;
        }
        MatchMatrixRowWritable row = new MatchMatrixRowWritable(columns);
        context.write(row, new VarIntWritable(value0.getResult()));
        /*if (value0.getResult() == 1){
            context.getCounter(Counters.MATCH_RIGHT).increment(1);
        }else{
            context.getCounter(Counters.MATCH_WRONG).increment(1);
        }*/
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
