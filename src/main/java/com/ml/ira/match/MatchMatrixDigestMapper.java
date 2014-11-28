package com.ml.ira.match;

import com.ml.ira.AppConfig;
import com.ml.ira.Constants;
import com.ml.ira.Counters;
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
public class MatchMatrixDigestMapper extends Mapper<LongWritable, Text, MatchMatrixDigestWritable, Text> {

    private AppConfig appConfig;
    private List<Integer> cates;
    private List<Integer> types;

    public MatchMatrixDigestMapper() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        appConfig = new AppConfig(Constants.MATCH_CONF);
        cates = appConfig.getCategoricalFields();
        List<Integer> groups = appConfig.getModelFields("female_fields");
        List<Integer> joins = appConfig.getModelFields("male_fields");
        joins.addAll(groups);
        types = new ArrayList<Integer>(joins.size());
        for (Integer a : joins){
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
        if (vals.length-1 > types.size()){
            System.err.println("Digest: " + value.toString() + ", key=" + key);
            return;
        }
        int i = 0;
        for(i=0; i<vals.length-1; i++){
            String iv = "null";
            if (types.get(i) == 1){
                iv = vals[i];
            }
            MatchMatrixDigestWritable row = new MatchMatrixDigestWritable(i, types.get(i), false, iv);
            context.write(row, new Text(iv));
        }
        MatchMatrixDigestWritable row = new MatchMatrixDigestWritable(i, 1, true, vals[i]);
        context.write(row, new Text(vals[i]));

        if ("1".equals(vals[i])){
            context.getCounter(Counters.MATCH_RIGHT).increment(1);
        }else{
            context.getCounter(Counters.MATCH_WRONG).increment(1);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
