package com.ml.ira.match;

import com.ml.ira.AppConfig;
import com.ml.ira.Constants;
import com.ml.ira.IDPairWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.VarIntWritable;

import java.io.IOException;
import java.util.List;

/**
 * 读取用于训练模型的行为记录.
 * Created by yaming_deng on 14-4-14.
 */
public class ActionRecordMapper extends Mapper<LongWritable, Text, IDPairWritable, VarIntWritable> {

    public static final VarIntWritable POSITIVE_VALUE = new VarIntWritable(1);
    public static final VarIntWritable NEGATIVE_VALUE = new VarIntWritable(0);

    private AppConfig appConfig;
    private String actionName;
    List<String> positives;
    List<String> negatives;

    public ActionRecordMapper() {
        super();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split("\t");
        String aid = values[0];
        String bid = values[1];
        if (aid.equalsIgnoreCase(bid)){
            return;
        }
        String resp = values[values.length - 1];
        IDPairWritable outKey1 = new IDPairWritable(Integer.parseInt(aid), Integer.parseInt(bid));
        if (positives.contains(resp)){
            context.write(outKey1, POSITIVE_VALUE);
        }else if (negatives.contains(resp)){
            context.write(outKey1, NEGATIVE_VALUE);
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        appConfig = new AppConfig(Constants.MATCH_CONF);
        actionName = context.getConfiguration().get("action_name");
        positives = appConfig.getPositiveValues(actionName);
        negatives = appConfig.getNegetivesValues(actionName);
    }
}
