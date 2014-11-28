package com.ml.ira.match;

import com.ml.ira.AppConfig;
import com.ml.ira.Constants;
import com.ml.ira.Counters;
import com.ml.ira.Gender;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.VarIntWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 输出Matrix左连接结果记录.
 * Created by yaming_deng on 14-4-14.
 */
public class MatchMatrixGroupReducer extends Reducer<VarIntWritable, MatchPairWritable, VarIntWritable, MatchPairWritable> {

    private AppConfig appConfig = null;
    private List<Integer> f_vectorFields;
    private List<Integer> m_vectorFields;
    private int f_sex;

    public MatchMatrixGroupReducer() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        appConfig = new AppConfig(Constants.MATCH_CONF);
        f_vectorFields = appConfig.getModelFields("female_fields");
        m_vectorFields = appConfig.getModelFields("male_fields");
        f_sex = appConfig.getFieldIndex("sex");
    }

    @Override
    protected void reduce(VarIntWritable key, Iterable<MatchPairWritable> values, Context context) throws IOException, InterruptedException {
        //the value iterator re-uses the same MatchPairWritable throughout the loop
        int[] user = null;
        List<int[]> cache = new ArrayList<int[]>();
        for(MatchPairWritable item : values){
            if (item.getResult() == -9){
                user = item.getGroup();
            }else{
                cache.add(new int[]{item.getJoin()[0], item.getResult()});
            }
        }
        if (user == null || cache.size() == 0){
            context.getCounter(Counters.MATCH_RULE_GROUPS_ERR).increment(1);
            return;
        }
        int gender = user[f_sex-1];
        int[] groupVals = null, joinVals = null;
        int gender0 = -1, gender1 = -1;
        if (gender == Gender.Female){
            gender1 = gender;
            joinVals = MatchVector.copy(user, f_vectorFields);
        }else{
            gender0 = gender;
            groupVals = MatchVector.copy(user, m_vectorFields);
        }
        for (int[] item : cache){
            MatchPairWritable nitem = new MatchPairWritable(gender0, groupVals, gender1, joinVals, item[1]);
            context.write(new VarIntWritable(item[0]), nitem); // userid->row
            if (item[1] == 1){
                context.getCounter(Counters.MATCH_RIGHT).increment(1);
            }else{
                context.getCounter(Counters.MATCH_WRONG).increment(1);
            }
        }
        context.getCounter(Counters.MATCH_RULE_GROUPS).increment(cache.size());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
