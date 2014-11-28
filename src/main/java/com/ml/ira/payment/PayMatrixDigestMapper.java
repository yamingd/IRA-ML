package com.ml.ira.payment;

import com.ml.ira.AppConfig;
import com.ml.ira.Constants;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 匹配Matrix的摘要数据构建.
 * Created by yaming_deng on 14-4-14.
 */
public class PayMatrixDigestMapper extends Mapper<LongWritable, Text, PayMatrixDigestWritable, Text> {

    private AppConfig appConfig;
    private PayMatrix payMatrix;
    private List<Integer> cates;
    private List<Integer> types;

    public PayMatrixDigestMapper() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        appConfig = new AppConfig(Constants.PAYMENT_CONF);
        payMatrix = new PayMatrix(appConfig);
        cates = appConfig.getCategoricalFields();
        List<Integer> groups = payMatrix.getPredictors();
        types = new ArrayList<Integer>(groups.size());
        for (Integer a : groups){
            if (cates.contains(a)){
                types.add(1);
            }else{
                types.add(0);
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] vals = value.toString().split(",");
        for(int i=1; i<vals.length; i++){
            String iv = "null";
            if (types.get(i) == 1){
                iv = vals[i];
            }
            PayMatrixDigestWritable row = new PayMatrixDigestWritable(i, types.get(i), false, iv);
            context.write(row, new Text(iv));
        }
        PayMatrixDigestWritable row = new PayMatrixDigestWritable(0, 1, true, vals[0]);
        context.write(row, new Text(vals[0]));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
