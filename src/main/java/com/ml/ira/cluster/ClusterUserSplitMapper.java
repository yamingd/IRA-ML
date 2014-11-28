package com.ml.ira.cluster;

import com.google.common.collect.Lists;
import com.ml.ira.AppConfig;
import com.ml.ira.Constants;
import com.ml.ira.Values;
import com.ml.ira.propotion.AttrValueWritable;
import com.ml.ira.standard.ValueRangeChecker;
import com.ml.ira.user.UserInfoPropotionJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by yaming_deng on 14-4-29.
 */
public class ClusterUserSplitMapper extends Mapper<LongWritable, Text, VarIntWritable, VectorWritable> {

    public static final String CLUSTER_NAME = "cluster_name";

    private VarIntWritable outkey = new VarIntWritable();
    private VectorWritable outvec = new VectorWritable();

    private AppConfig appConfig;
    private List<String> fieldNames;
    private List<Integer> fields;
    private List<Integer> cates;
    private int f_gender;
    private Map<Integer, List<AttrValueWritable>> attrs;
    private VectorStdNorm stdNorm = null;
    private ValueRangeChecker valueRangeChecker;

    public ClusterUserSplitMapper() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        String name = context.getConfiguration().get(CLUSTER_NAME);
        Path[] files = HadoopUtil.getCachedFiles(context.getConfiguration());
        if (files.length <= 0){
            throw new InterruptedException();
        }

        appConfig = new AppConfig(Constants.CONF_CLUSTER);
        valueRangeChecker = new ValueRangeChecker(appConfig);
        fieldNames = appConfig.get(name);
        fields = new ArrayList<Integer>(fieldNames.size());
        for (String key : fieldNames){
            fields.add(appConfig.getFieldIndex(key));
        }
        f_gender = appConfig.getFieldIndex("sex");
        attrs = UserInfoPropotionJob.loadAttrsMap(files[0]);
        cates = appConfig.getCategoricalFields();
        stdNorm = new VectorStdNorm(cates, attrs);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] vals = value.toString().split("\t");
        if (!valueRangeChecker.valid(vals)){
            return;
        }
        int gender = Values.asInt(vals[f_gender]);
        List<Double> temp = Lists.newArrayList();
        temp.add(Double.parseDouble(vals[0])); //user id
        for (Integer fi : fields){
            int val = Values.asInt(vals[fi]);
            //temp.add(stdNorm.inRatio(fi, val));
            if (cates.contains(fi)){
                stdNorm.addBinary(temp, fi, val);
            }else{
                temp.add(stdNorm.inStd(fi, val));
            }
        }
        Vector user = new RandomAccessSparseVector(temp.size());
        for (int j = 0; j < temp.size(); j++) {
            user.setQuick(j, temp.get(j));
        }
        outkey.set(gender);
        outvec.set(user);
        context.write(outkey, outvec);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
