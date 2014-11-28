package com.ml.ira.match;

import com.ml.ira.Counters;
import com.ml.ira.IDPairWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.VarIntWritable;

import java.io.IOException;

/**
 * Created by yaming_deng on 14-4-14.
 */
public class ActionRecordReducer extends Reducer<IDPairWritable, VarIntWritable, IDPairWritable, VarIntWritable> {

    public static final VarIntWritable POSITIVE_VALUE = new VarIntWritable(1);
    public static final VarIntWritable NEGATIVE_VALUE = new VarIntWritable(0);

    public ActionRecordReducer() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void reduce(IDPairWritable key, Iterable<VarIntWritable> values, Context context) throws IOException, InterruptedException {
        boolean support = false, neg = false;
        for(VarIntWritable item : values){
            if (item.get() == 1){
                support = true;
            }else{
                neg = true;
            }
            if (support && neg){
                break;
            }
        }
        if (support && neg){
            //confused then do nothing
            System.out.println("Act Confused. key=" + key);
        }else{
            if (support){
                context.write(key, POSITIVE_VALUE);
                context.getCounter(Counters.MATCH_RIGHT).increment(1);
                context.getCounter(Counters.ACTION_PAIRS).increment(1);
            }else if(neg){
                context.write(key, NEGATIVE_VALUE);
                context.getCounter(Counters.MATCH_WRONG).increment(1);
                context.getCounter(Counters.ACTION_PAIRS).increment(1);
            }
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
