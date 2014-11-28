package com.ml.ira.match;

import com.ml.ira.AppConfig;
import com.ml.ira.Constants;
import com.ml.ira.Counters;
import com.ml.ira.Gender;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.VarIntWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 输出Matrix右连接结果记录.
 * Created by yaming_deng on 14-4-14.
 */
public class MatchMatrixJoinReducer extends Reducer<VarIntWritable, MatchPairWritable, NullWritable, MatchPairWritable> {

    private AppConfig appConfig = null;
    private int f_sex = -1;
    private List<Integer> f_vectorFields;
    private List<Integer> m_vectorFields;

    public MatchMatrixJoinReducer() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        appConfig = new AppConfig(Constants.MATCH_CONF);
        f_sex = appConfig.getFieldIndex("sex");
        f_vectorFields = appConfig.getModelFields("female_fields");
        m_vectorFields = appConfig.getModelFields("male_fields");
    }

    static class MatchPair{

        MatchPair(int gender0, int[] group, int gender1, int[] join, int result) {
            this.gender0 = gender0;
            this.group = group;
            this.gender1 = gender1;
            this.join = join;
            this.result = result;
        }

        @Override
        public String toString() {
            return "MatchPair{" +
                    "gender0=" + gender0 +
                    ", group=" + Arrays.toString(group) +
                    ", gender1=" + gender1 +
                    ", join=" + Arrays.toString(join) +
                    ", result=" + result +
                    '}';
        }

        public int gender0;
        public int[] group;
        public int gender1;
        public int[] join;
        public int result;
    }

    @Override
    protected void reduce(VarIntWritable key, Iterable<MatchPairWritable> values, Context context) throws IOException, InterruptedException {
        //the value iterator re-uses the same MatchPairWritable throughout the loop
        int[] user = null;
        List<MatchPair> cacheResults = new ArrayList<MatchPair>();
        for(MatchPairWritable item : values){
            if (item.getResult() == -9){
                user = item.getGroup();
            }else{
                cacheResults.add(new MatchPair(item.getGender0(), item.getGroup(), item.getGender1(), item.getJoin(), item.getResult()));
            }
        }
        if (user == null || cacheResults.size() == 0){
            context.getCounter(Counters.MATCH_RULE_JOINS_ERR).increment(1);
            return;
        }
        int gender = user[f_sex-1];
        int[] groupVals = null, joinVals = null;
        if (gender == Gender.Female){
            joinVals = MatchVector.copy(user, f_vectorFields);
        }else{
            groupVals = MatchVector.copy(user, m_vectorFields);
        }
        for(int i=0; i<cacheResults.size(); i++){
            MatchPair item = cacheResults.get(i);
            if (gender == Gender.Female){
                item.join = joinVals;
                item.gender1 = gender;
                if (item.group.length != m_vectorFields.size()){
                    System.err.println(String.format("FPair Error. %s", item));
                }
            }else{
                item.gender0 = gender;
                item.group = groupVals;
                if (item.join.length != f_vectorFields.size()){
                    System.err.println(String.format("MPair Error. %s", item));
                }
            }
            MatchPairWritable nitem = new MatchPairWritable(item.gender0, item.group, item.gender1, item.join, item.result);
            context.write(NullWritable.get(), nitem);
            if (item.result == 1){
                context.getCounter(Counters.MATCH_RIGHT).increment(1);
            }else{
                context.getCounter(Counters.MATCH_WRONG).increment(1);
            }
        }
        context.getCounter(Counters.MATCH_RULE_JOINS).increment(cacheResults.size());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
