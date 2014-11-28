package com.ml.ira.cluster;

import com.google.common.collect.Lists;
import com.ml.ira.AppConfig;
import com.ml.ira.Constants;
import com.ml.ira.Values;
import com.ml.ira.propotion.AttrValueWritable;
import com.ml.ira.propotion.PropotionJob;
import com.ml.ira.standard.ValueRangeChecker;
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
public class ClusterDataPrepareMapper extends Mapper<LongWritable, Text, VarIntWritable, VectorWritable> {

    public static final String CLUSTER_NAME = "cluster_name";

    private VarIntWritable outkey = new VarIntWritable();
    private VectorWritable outvec = new VectorWritable();

    private AppConfig appConfig;
    private List<String> fieldNames;
    private List<Integer> fields;
    private List<Integer> cates;
    private Map<Integer, List<AttrValueWritable>> attrs;
    private VectorStdNorm stdNorm = null;
    private ValueRangeChecker valueRangeChecker;

    public ClusterDataPrepareMapper() {
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
        attrs = PropotionJob.loadPropotionMap(files[0]);
        cates = appConfig.getCategoricalFields();
        stdNorm = new VectorStdNorm(cates, attrs);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] vals = value.toString().split("\t");
        if (!valueRangeChecker.valid(vals)){
            return;
        }
        List<Double> temp = Lists.newArrayList();
        temp.add(Double.parseDouble(vals[0])); // item-id
        for (Integer fi : fields){
            int val = Values.asInt(vals[fi]);
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
        outkey.set(0);
        outvec.set(user);
        context.write(outkey, outvec);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
