package com.ml.ira.match;

import com.ml.ira.AppConfig;
import com.ml.ira.Constants;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.VarIntWritable;

import java.io.IOException;
import java.util.List;

/**
 * 读取用户行为记录.
 * Created by yaming_deng on 14-4-14.
 */
public class UserActionSplitMapper extends Mapper<LongWritable, Text, VarIntWritable, MatchPairWritable> {

    private AppConfig appConfig;
    private String actionName;

    public UserActionSplitMapper() {
        super();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        List<String> positives = appConfig.getPositiveValues(actionName);
        List<String> negatives = appConfig.getNegetivesValues(actionName);

        String[] values = value.toString().split("\t");
        String aid = values[0];
        String bid = values[1];
        String resp = values[values.length - 1];
        int respValue = 0;
        if (positives.contains(resp)){
            respValue = 1;
        }else if (negatives.contains(resp)){
            respValue = 0;
        }

        int[] join = new int[]{Integer.parseInt(bid)};
        MatchPairWritable retVal = new MatchPairWritable(-1, null, -1, join, respValue);
        context.write(new VarIntWritable(Integer.parseInt(aid)), retVal);
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        appConfig = new AppConfig(Constants.MATCH_CONF);
        actionName = context.getConfiguration().get("action_name");
    }
}
