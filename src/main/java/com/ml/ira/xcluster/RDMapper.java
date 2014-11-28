package com.ml.ira.xcluster;

import com.google.common.collect.Lists;
import com.ml.ira.AppConfig;
import com.ml.ira.Constants;
import com.ml.ira.Values;
import com.ml.ira.cluster.VectorStdNorm;
import com.ml.ira.propotion.AttrValueWritable;
import com.ml.ira.standard.ValueRangeChecker;
import com.ml.ira.user.UserInfoPropotionJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.VarIntWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 选择需要的数据
 * Created by yaming_deng on 14-5-14.
 */
public class RDMapper extends Mapper<LongWritable, Text, VarIntWritable, Text> {

    public static final String CLUSTER_NAME = "cluster_name";

    private VarIntWritable outkey = new VarIntWritable();
    private Text outvec = new Text();

    private AppConfig appConfig;
    private List<String> fieldNames;
    private List<Integer> fields;
    private List<Integer> cates;
    private int f_gender;
    private Map<Integer, List<AttrValueWritable>> attrs;
    private VectorStdNorm stdNorm = null;
    private ValueRangeChecker valueRangeChecker;

    public RDMapper() {
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
        List<Integer> temp = Lists.newArrayList();
        for (Integer fi : fields){
            int val = Values.asInt(vals[fi]);
            temp.add(val);
        }
        outkey.set(10 + gender);
        outvec.set(StringUtils.join("\t", temp));
        context.write(outkey, outvec);

        //原始数据
        int clusterIdcode = outvec.toString().hashCode();
        temp.add(0, clusterIdcode);
        temp.add(1, Integer.parseInt(vals[0])); //user id
        outkey.set(20 + gender);
        outvec.set(StringUtils.join("\t", temp));
        context.write(outkey, outvec);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
