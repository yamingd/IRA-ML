package com.ml.ira.match;

import com.ml.ira.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.VarIntWritable;

import java.io.IOException;
import java.util.List;

/**
 * 分割用户，并过滤不用推荐的
 * Created by yaming_deng on 14-4-28.
 */
public class MatchPredictionSplitMapper extends Mapper<LongWritable, Text, VarIntWritable, UserIndexWritable> {

    private int f_height = -1;
    private int f_age = -1;
    private int f_sex = -1;
    private int f_marriage = -1;
    private List<Integer> f_vectorFields;
    private List<Integer> m_vectorFields;

    public MatchPredictionSplitMapper() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        AppConfig appConfig = new AppConfig(Constants.MATCH_CONF);

        f_vectorFields = appConfig.getModelFields("female_fields");
        m_vectorFields = appConfig.getModelFields("male_fields");

        f_height = appConfig.getFieldIndex("height");
        f_age = appConfig.getFieldIndex("age");
        f_sex = appConfig.getFieldIndex("sex");
        f_marriage = appConfig.getFieldIndex("marriage");

        f_vectorFields.add(0, 0); //userid
        f_vectorFields.add(1, f_sex);

        m_vectorFields.add(0, 0); //userid
        m_vectorFields.add(1, f_sex);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] val = value.toString().split("\t");
        int gender;
        if (Values.asInt(val[f_height]) <= 0){
            return;
        }
        int var = Values.asInt(val[f_age]);
        if (var <=0 || var >= 100){
            return;
        }
        var = Values.asInt(val[f_sex]);
        gender = var;
        if (var !=0 && var != 1){
            return;
        }
        var = Values.asInt(val[f_marriage]);
        if (var <=0 || var >= 10){
            return;
        }
        int[] user = null;
        if (gender == Gender.Female){
            user = MatchVector.copy(val, f_vectorFields);
            context.getCounter(Counters.USERS_FEAMLE).increment(1);
        }else{
            user = MatchVector.copy(val, m_vectorFields);
            context.getCounter(Counters.USERS_MALE).increment(1);
        }
        context.write(new VarIntWritable(gender), new UserIndexWritable(user));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
