package com.ml.ira.match;

import com.ml.ira.Counters;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.VarIntWritable;

import java.io.IOException;

/**
 * 输出Matrix每行，保证每行的唯一和一致性
 * Created by yaming_deng on 14-4-15.
 */
public class MatchMatrixFilterReducer extends Reducer<MatchMatrixRowWritable, VarIntWritable, MatchMatrixRowWritable, NullWritable> {

    public MatchMatrixFilterReducer() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void reduce(MatchMatrixRowWritable key, Iterable<VarIntWritable> values, Context context) throws IOException, InterruptedException {
        int positive = 0, negative = 0;
        for(VarIntWritable item : values){
            if (item.get() == 1){
                positive ++;
            }else {
                negative ++;
            }
        }
        //int r = (int) (10.0f * positive / (positive + negative));
        if (positive > 1 && negative > 1){
            return;
        }
        int r = positive >= 1 ? 1 : 0;
        int[] keyColumns = key.getColumns();
        int size = keyColumns.length;
        int[] columns = new int[size + 1];
        for(int i=0; i<size;i++){
            columns[i] = keyColumns[i];
        }
        columns[size] = r;
        MatchMatrixRowWritable outValue = new MatchMatrixRowWritable(columns);
        context.write(outValue, NullWritable.get());
        context.getCounter(Counters.MATCH_RULES).increment(1);
        if (r == 1){
            context.getCounter(Counters.MATCH_RIGHT).increment(1);
        }else{
            context.getCounter(Counters.MATCH_WRONG).increment(1);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
