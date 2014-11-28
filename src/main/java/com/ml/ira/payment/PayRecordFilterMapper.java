package com.ml.ira.payment;

import com.ml.ira.AppConfig;
import com.ml.ira.Constants;
import com.ml.ira.Counters;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.VarIntWritable;

import java.io.IOException;
import java.util.List;

/**
 * Created by yaming_deng on 14-4-17.
 */
public class PayRecordFilterMapper extends Mapper<LongWritable, Text, VarIntWritable, PayRecordWritable> {

    public static final String NULL = "null";

    private AppConfig appConfig = null;
    private List<Integer> predictors = null;
    private PayMatrix payMatrix = null;

    public PayRecordFilterMapper() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        appConfig = new AppConfig(Constants.PAYMENT_CONF);
        payMatrix = new PayMatrix(appConfig);
        predictors = payMatrix.getPredictors();
    }

    private boolean select(final int[] mods, int mod){
        for (int i=0;i <mods.length; i++){
            if (mods[i] == mod){
                return true;
            }
        }
        return false;
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] vs = value.toString().split("\t");
        if (vs.length < 10){
            return;
        }
        //分割数据为训练和测试集合，训练集合=80%
        int mod = (int) key.get() % 10;
        VarIntWritable outKey;
        if (Integer.parseInt(vs[1]) == 1){ //只选择iszat=1的记录
            float[] user = payMatrix.mapUser(vs, this.predictors);
            if (user == null){
                return;
            }
            if (select(PayMatrix.trainPositiveMods, mod)){
                outKey = new VarIntWritable(1); //train
                context.getCounter(Counters.PAY_TRAIN_1).increment(1);
            }else{
                outKey = new VarIntWritable(0); //test
                context.getCounter(Counters.PAY_TEST_1).increment(1);
            }
            context.write(outKey, new PayRecordWritable(user));
        }else{
            if (select(PayMatrix.testNegativeMods, mod)){
                float[] user = payMatrix.mapUser(vs, this.predictors);
                if (user == null){
                    return;
                }
                outKey = new VarIntWritable(0); //test
                context.write(outKey, new PayRecordWritable(user));
                context.getCounter(Counters.PAY_TEST_0).increment(1);
            }else if (select(PayMatrix.trainNegativeMods, mod)){
                float[] user = payMatrix.mapUser(vs, this.predictors);
                if (user == null){
                    return;
                }
                outKey = new VarIntWritable(1); //train
                context.write(outKey, new PayRecordWritable(user));
                context.getCounter(Counters.PAY_TRAIN_0).increment(1);
            }
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }

}
