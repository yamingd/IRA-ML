package com.ml.ira.distance;

import com.google.common.collect.Maps;
import com.ml.ira.AppConfig;
import com.ml.ira.Constants;
import com.ml.ira.cluster.ClusterUserSplitMapper;
import com.ml.ira.propotion.AttrValueWritable;
import com.ml.ira.user.UserInfoPropotionJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.parameters.Parameter;
import org.apache.mahout.math.Vector;

import java.io.IOException;
import java.util.*;

/**
 * Created by yaming_deng on 14-5-13.
 */
public class GowerSimilarity implements DistanceMeasure {

    private Map<Integer, Integer> vmaps;
    private AppConfig appConfig;
    private List<Integer> fields;
    private List<Integer> cates;

    public double distance(double[] v1, double[] v2){
        if (fields == null){
            return Integer.MAX_VALUE;
        }
        double sum = 0.0;
        for (int i = 0; i < fields.size(); i++) {
            Integer fid = fields.get(i);
            double x1 = v1[i];
            double x2 = v2[i];
            if (cates.contains(fid)){
                if (x1 == x2){
                    sum += 1;
                }else{
                    sum += 0;
                }
            }else{
                int dom = vmaps.get(fid);
                if (dom != 0){
                    double y = 1 - Math.abs(x1 - x2) / dom;
                    sum += y;
                }else{
                    sum += 1;
                }
            }
        }
        sum = sum / fields.size();
        return sum;
    }

    @Override
    public double distance(Vector v1, Vector v2) {
        if (fields == null){
            return Integer.MAX_VALUE;
        }
        double sum = 0.0;
        for (int i = 0; i < fields.size(); i++) {
            Integer fid = fields.get(i);
            double x1 = v1.get(i);
            double x2 = v2.get(i);
            if (cates.contains(fid)){
                if (x1 == x2){
                    sum += 1;
                }else{
                    sum += 0;
                }
            }else{
                int dom = vmaps.get(fid);
                if (dom != 0){
                    double y = 1 - Math.abs(x1 - x2) / dom;
                    sum += y;
                }else{
                    sum += 1;
                }
            }
        }
        sum = sum / fields.size();
        return sum;
    }

    @Override
    public double distance(double centroidLengthSquare, Vector centroid, Vector v) {
        // 计算向量和中心的距离, 用于检验覆盖值
        return distance(centroid, v);
    }

    @Override
    public Collection<Parameter<?>> getParameters() {
        return Collections.EMPTY_LIST;
    }

    @Override
    public void createParameters(String prefix, Configuration jobConf) {

    }

    @Override
    public void configure(Configuration config) {
        String name = config.get(ClusterUserSplitMapper.CLUSTER_NAME);
        try {
            appConfig = new AppConfig(Constants.CONF_CLUSTER);
            List<String> fieldNames = appConfig.get(name);
            fields = new ArrayList<Integer>(fieldNames.size());
            cates = appConfig.getCategoricalFields();
            boolean hasNumerical = false;
            for (String key : fieldNames){
                Integer index = appConfig.getFieldIndex(key);
                fields.add(index);
                if (cates.contains(index)){
                    hasNumerical = true;
                }
            }
            vmaps = Maps.newHashMap();
            if (!hasNumerical){
                return;
            }
            Path[] files = HadoopUtil.getCachedFiles(config);
            Map<Integer, List<AttrValueWritable>> attrs = UserInfoPropotionJob.loadAttrsMap(files[0]);
            for(Integer fid : fields){
                if (cates.contains(fid)){
                    continue;
                }
                List<AttrValueWritable> items = attrs.get(fid);
                Integer[] mms = new Integer[]{Integer.MAX_VALUE, Integer.MIN_VALUE};
                for(AttrValueWritable attr : items){
                    if (attr.getValue() > mms[1]){
                        mms[1] = attr.getValue();
                    }
                    if (attr.getValue() < mms[0]){
                        mms[0] = attr.getValue();
                    }
                }
                vmaps.put(fid, mms[1] - mms[0]);
                mms = null;
            }
            System.out.println(vmaps);
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("GlowerDistanceMeasure Configure Done.");
    }
}
