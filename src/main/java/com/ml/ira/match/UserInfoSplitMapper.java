package com.ml.ira.match;

import com.ml.ira.AppConfig;
import com.ml.ira.Constants;
import com.ml.ira.Values;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.VarIntWritable;

import java.io.IOException;

/**
 * 读取用户基本信息
 * Created by yaming_deng on 14-4-14.
 */
public class UserInfoSplitMapper extends Mapper<LongWritable, Text, VarIntWritable, MatchPairWritable> {

    public static final String NULL = "null";

    private int f_height = -1;
    private int f_age = -1;
    private int f_sex = -1;
    private int f_marriage = -1;

    public UserInfoSplitMapper() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        AppConfig appConfig = new AppConfig(Constants.MATCH_CONF);
        f_height = appConfig.getFieldIndex("height");
        f_age = appConfig.getFieldIndex("age");
        f_sex = appConfig.getFieldIndex("sex");
        f_marriage = appConfig.getFieldIndex("marriage");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] val = value.toString().split("\t");
        if (Values.asInt(val[f_height]) <= 0){
            return;
        }
        int var = Values.asInt(val[f_age]);
        if (var <=0 || var >= 100){
            return;
        }
        var = Values.asInt(val[f_sex]);
        if (var !=0 && var != 1){
            return;
        }
        var = Values.asInt(val[f_marriage]);
        if (var <=0 || var >= 10){
            return;
        }
        int userId = Integer.parseInt(val[0]);
        int[] urow = new int[val.length-1]; //不包含userId
        for(int i=0; i<urow.length; i++){
            if (val[i+1].equalsIgnoreCase(NULL)){
                urow[i] = -1;
            }else{
                urow[i] = Integer.parseInt(val[i+1]);
            }
        }
        MatchPairWritable retVal = new MatchPairWritable(-1, urow, -1, null, -9);
        context.write(new VarIntWritable(userId), retVal);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
