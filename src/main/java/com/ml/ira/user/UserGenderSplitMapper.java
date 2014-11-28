package com.ml.ira.user;

import com.google.common.collect.Lists;
import com.ml.ira.AppConfig;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;
import java.util.List;

/**
 * Created by yaming_deng on 14-4-14.
 */
public class UserGenderSplitMapper extends Mapper<LongWritable, Text, VarIntWritable, VectorWritable> {

    public static final String NULL = "null";

    private AppConfig appConfig;
    private VarIntWritable outKey = new VarIntWritable();
    private VectorWritable outVal = new VectorWritable();
    private int f_gender;

    public UserGenderSplitMapper() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        String appName = context.getConfiguration().get("app_name");
        appConfig = new AppConfig(appName);
        f_gender = appConfig.getFieldIndex("sex");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] val = value.toString().split("\t");
        int gender = Integer.parseInt(val[f_gender]);
        outKey.set(gender);

        List<Integer> cols = Lists.newArrayList();
        int i = 0;
        for(i=0; i<val.length; i++){
            if (i == f_gender){
                continue;
            }
            if (val[i].equalsIgnoreCase(NULL)){
                cols.add(-1);
            }else{
                cols.add(Integer.parseInt(val[i]));
            }
        }
        i = 0;
        RandomAccessSparseVector vector = new RandomAccessSparseVector(cols.size());
        for(Integer cv : cols){
            vector.set(i, cv);
            i++;
        }
        outVal.set(vector);
        context.write(outKey, outVal);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
