package com.ml.ira.user;

import com.ml.ira.IDPairWritable;
import com.ml.ira.match.MatchPairWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by yaming_deng on 14-4-14.
 */
public class UserSplitByCityMapper extends Mapper<LongWritable, Text, IDPairWritable, MatchPairWritable> {

    public static final String NULL = "null";

    public UserSplitByCityMapper() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] val = value.toString().split("\t");
        int cityid = Integer.parseInt(val[1]);
        int gender = Integer.parseInt(val[3]);
        int[] urow = new int[val.length-2];
        for(int i=0; i<val.length; i++){
            if (i == 1 || i==3){
                continue;
            }
            if (val[i].equalsIgnoreCase(NULL)){
                urow[i] = -1;
            }else{
                urow[i] = Integer.parseInt(val[i]);
            }
        }
        MatchPairWritable retVal = new MatchPairWritable(-1, urow, -1, null, -9);
        context.write(new IDPairWritable(gender, cityid), retVal);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
