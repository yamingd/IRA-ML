package com.ml.ira.user;

import com.ml.ira.AppConfig;
import com.ml.ira.Constants;
import com.ml.ira.Counters;
import com.ml.ira.propotion.AttrValueWritable;
import com.ml.ira.standard.ValueRangeChecker;
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
public class UserAttrPropotionMapper extends Mapper<LongWritable, Text, AttrValueWritable, VarIntWritable> {

    public static final String NULL = "null";
    public static final VarIntWritable VALUE = new VarIntWritable(1);
    private AppConfig appConfig;
    private List<String> fields;
    private ValueRangeChecker valueRangeChecker;

    public UserAttrPropotionMapper() throws IOException {
        super();
        appConfig = new AppConfig(Constants.MATCH_CONF);
        valueRangeChecker = new ValueRangeChecker(appConfig);
        String temp = appConfig.get("fields");
        fields = Arrays.asList(temp.split(","));
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] val = value.toString().split("\t");
        if (!valueRangeChecker.valid(val)){
            return;
        }
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
        context.getCounter(Counters.USERS).increment(1);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
