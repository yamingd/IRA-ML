package com.ml.ira.propotion;

import com.ml.ira.AppConfig;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.VarIntWritable;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by yaming_deng on 14-4-15.
 */
public class DatasetPropotionMapper extends Mapper<LongWritable, Text, AttrValueWritable, VarIntWritable> {

    public static final String APP_NAME = "APP_NAME";

    public enum Counters {
        TOTAL_RECORD
    }

    public static final String NULL = "null";
    public static final VarIntWritable VALUE = new VarIntWritable(1);
    private AppConfig appConfig;
    private List<String> fields;

    public DatasetPropotionMapper() throws IOException {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        String name = context.getConfiguration().get(APP_NAME);
        appConfig = new AppConfig(name);
        String temp = appConfig.get("fields");
        fields = Arrays.asList(temp.split(","));
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] val = value.toString().split("\t");
        for(int i=1; i<val.length; i++){
            int attrVal = -1;
            if (val[i].equalsIgnoreCase(NULL)){
                attrVal = -1;
            }else{
                attrVal = Integer.parseInt(val[i]);
            }
            AttrValueWritable outKey = new AttrValueWritable(i, attrVal, fields.get(i));
            context.write(outKey, VALUE);
        }
        context.getCounter(Counters.TOTAL_RECORD).increment(1);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
