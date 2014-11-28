package com.ml.ira.cluster;

import com.ml.ira.AppConfig;
import com.ml.ira.Constants;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.VarIntWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 聚类中的用户属性值分布
 * Created by yaming_deng on 14-4-29.
 */
public class ClusterItemInfoPropMapper extends Mapper<LongWritable, Text, ClusterItemAttrValueWritable, VarIntWritable> {

    public static final String CLUSTER_NAME = "cluster_name";

    private ClusterItemAttrValueWritable outkey = new ClusterItemAttrValueWritable();
    private VarIntWritable outval = new VarIntWritable(1);

    private AppConfig appConfig;
    private List<Integer> fields;
    private List<String> fieldNames;

    public ClusterItemInfoPropMapper() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        String name = context.getConfiguration().get(CLUSTER_NAME);
        appConfig = new AppConfig(Constants.CONF_CLUSTER);
        fieldNames = appConfig.get(name);
        fields = new ArrayList<Integer>(fieldNames.size());
        for (String key : fieldNames){
            fields.add(appConfig.getFieldIndex(key));
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] vals = value.toString().split("\t"); //cluster-id,userid,a1,a2,a3...
        Integer clusterId = Integer.parseInt(vals[0]);
        for(int i=2; i<vals.length; i++){
            Float f = Float.parseFloat(vals[i]);
            outkey.setClusterId(clusterId);
            outkey.setAttr(fields.get(i-2));
            outkey.setName(fieldNames.get(i-2));
            outkey.setValue(f.intValue());
            context.write(outkey, outval);
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
