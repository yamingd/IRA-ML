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
 * 对记录做统计，形成分布
 * Created by yaming_deng on 14-4-17.
 */
public class PayRecordScaleMapper extends Mapper<LongWritable, Text, PayRecordScaleWritable, VarIntWritable> {

    public static final String NULL = "null";

    private AppConfig appConfig = null;
    private PayMatrix payMatrix = null;

    public PayRecordScaleMapper() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        appConfig = new AppConfig(Constants.PAYMENT_CONF);
        payMatrix = new PayMatrix(appConfig);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] val = value.toString().split("\t");
        if (val.length <= payMatrix.f_birthday){
            System.err.println("Error Line: " + value);
            return;
        }
        int ages = payMatrix.getAges(val[payMatrix.f_birthday]);
        if (ages == 0){
            return;
        }
        int payed = Integer.parseInt(val[payMatrix.f_iszat]);
        val[payMatrix.f_birthday] = ages+"";
        List<Integer> ignores = appConfig.getIgnoreFields();
        for(Integer i=2; i<val.length; i++){
            if (ignores != null && ignores.contains(i)){
                continue;
            }
            if (val[i].equalsIgnoreCase(NULL)){
                continue;
            }
            PayRecordScaleWritable outKey = new PayRecordScaleWritable(payed, i, Integer.parseInt(val[i]));
            context.write(outKey, new VarIntWritable(1));
        }
        context.getCounter(Counters.USERS).increment(1);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
