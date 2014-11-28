package com.ml.ira.xcluster;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.ml.ira.AppConfig;
import com.ml.ira.Constants;
import com.ml.ira.cluster.ClusterUserSplitMapper;
import com.ml.ira.distance.GowerDistanceMeasure;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.VarIntWritable;

import java.io.IOException;
import java.util.*;

/**
 * Created by yaming_deng on 14-5-14.
 */
public class CCenterTrainReducer extends Reducer<VarIntWritable, XCVectorWritable, NullWritable, XClusterWritable> {

    private List<XClusterWritable> centers;
    private AppConfig appConfig;
    private List<Integer> fields;
    private List<Integer> cates;
    private GowerDistanceMeasure measure;
    private float distanceDelta = 0.01f;

    public CCenterTrainReducer() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        String str = context.getConfiguration().get(XClusterJob.PRIOR_PATH_KEY);
        Path centerPath = new Path(str);
        try {
            centers = XCenterSeedGenerator.loadCenters(context.getConfiguration(), centerPath);
        } catch (Exception e) {
            e.printStackTrace();
            throw new InterruptedException();
        }
        distanceDelta = 0.01f;
        String name = context.getConfiguration().get(ClusterUserSplitMapper.CLUSTER_NAME);
        appConfig = new AppConfig(Constants.CONF_CLUSTER);
        List<String> fieldNames = appConfig.get(name);
        fields = new ArrayList<Integer>(fieldNames.size());
        for (String key : fieldNames){
            fields.add(appConfig.getFieldIndex(key));
        }
        cates = appConfig.getCategoricalFields();
        measure = new GowerDistanceMeasure();
        measure.configure(context.getConfiguration());
    }

    @Override
    protected void reduce(VarIntWritable key, Iterable<XCVectorWritable> values, Context context) throws IOException, InterruptedException {
        XClusterWritable writable = null;
        for (XClusterWritable c : centers){
            if (c.getClusterId() == key.get()){
                writable = c;
                break;
            }
        }
        List<XCVectorWritable> list = Lists.newArrayList();
        try {
            for (XCVectorWritable v : values){
                list.add(v.clone());
            }
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        Collections.sort(list);
        double min = list.get(0).getDistance();
        double max = min + distanceDelta;
        boolean cut = false;
        for (int i = 1; i < list.size(); i++) {
            double d = list.get(i).getDistance();
            if (d > max){
                list = list.subList(i, list.size());
                cut = true;
                break;
            }
        }
        if (!cut){
            writable.setConvered(true);
            writable.setClusterId(writable.getCenter().hashCode());
            context.write(NullWritable.get(), writable);
            System.out.println(Arrays.toString(writable.getCenter()));
            return;
        }
        //输出中心
        writable.setConvered(false);
        writable.setClusterId(writable.getCenter().hashCode());
        context.write(NullWritable.get(), writable);
        System.out.println(Arrays.toString(writable.getCenter()));
        //分裂新中心
        Map<Double, Integer> map = Maps.newHashMap();
        while (list.size() > 0){
            min = list.get(0).getDistance();
            max = min + distanceDelta;
            List<XCVectorWritable> subList = null;
            for (int i = 1; i < list.size(); i++) {
                double d = list.get(i).getDistance();
                if (d > max){
                    subList = list.subList(0, i);
                    list = list.subList(i, list.size());
                    break;
                }
            }
            if (subList == null){
                subList = list;
                list = Collections.EMPTY_LIST;
            }
            if (subList != null){
                System.out.println("SubList: " + subList.size());
                double[] vector = new double[fields.size()];
                for (int i = 0; i < fields.size(); i++) {
                    if (cates.contains(fields.get(i))){
                        for(XCVectorWritable item : subList){
                            double v = item.getVector()[i];
                            Integer c = map.get(v);
                            if (c == null){
                                c = 1;
                            }else{
                                c ++;
                            }
                            map.put(v, c);
                        }
                        int mcount = 0;
                        double mvalue = -1;
                        for (Double mc : map.keySet()){
                            if (map.get(mc) >= mcount){
                                mcount = map.get(mc);
                                mvalue = mc;
                            }
                        }
                        XCVectorWritable maxitem = null;
                        for (XCVectorWritable item : subList){
                            if (item.getVector()[i] == mvalue){
                                if (maxitem == null){
                                    maxitem = item;
                                }else{
                                    if (maxitem.getDistance() > item.getDistance()){
                                        maxitem = item;
                                    }
                                }
                            }
                        }
                        vector[i] = maxitem.getVector()[i];
                        map.clear();
                    }else{
                        double sum = 0.0;
                        for(XCVectorWritable item : subList){
                            sum += item.getVector()[i];
                        }
                        vector[i] = sum / subList.size();
                    }
                }
                subList = null;
                writable.setConvered(false);
                writable.setCenter(vector);
                writable.setClusterId(vector.hashCode());
                //输出中心
                context.write(NullWritable.get(), writable);
                System.out.println("New Center:" + Arrays.toString(writable.getCenter()));
            }
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
