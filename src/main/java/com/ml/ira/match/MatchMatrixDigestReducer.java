package com.ml.ira.match;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * 匹配Matrix的摘要数据合并输出
 * Created by yaming_deng on 14-4-15.
 */
public class MatchMatrixDigestReducer extends Reducer<MatchMatrixDigestWritable, Text, MatchMatrixDigestWritable, Text> {

    private Text empty = new Text("");

    public MatchMatrixDigestReducer() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void reduce(MatchMatrixDigestWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> conts = null;
        if (key.getType() == 1){
            conts = new HashSet<String>();
            for (Text iv : values){
                if (iv.toString().length() > 0) {
                    conts.add(iv.toString());
                }
            }
            if (conts.size() > 0){
                String svals = StringUtils.join(conts, ",");
                MatchMatrixDigestWritable out = new MatchMatrixDigestWritable(key.getAttr(), key.getType(), key.isLabel(), svals);
                context.write(out, empty);
            }else{
                context.write(key, empty);
            }

        }else{
            context.write(key, empty);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
