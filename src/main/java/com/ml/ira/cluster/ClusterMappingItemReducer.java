package com.ml.ira.cluster;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.Vector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 建立用户和聚类的关系
 * Created by yaming_deng on 14-4-29.
 */
public class ClusterMappingItemReducer extends Reducer<IntWritable, WeightedPropertyVectorWritable, VarIntWritable, Text> {

    public static final String CENTER_PATH = "CENTER_PATH";

    public enum Counter{
        CLUSTERS
    }

    private Text out = new Text();
    private VarIntWritable outkey = new VarIntWritable();

    private List<ClusterWritable> clusterWritableList;
    private DistanceMeasure measure;

    public ClusterMappingItemReducer() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        String url = context.getConfiguration().get(CENTER_PATH);
        clusterWritableList = new KMeansJob().getCenters(new Path(url));
        measure = new SquaredEuclideanDistanceMeasure();
    }

    @Override
    protected void reduce(IntWritable key, Iterable<WeightedPropertyVectorWritable> values, Context context) throws IOException, InterruptedException {
        int cluseterId = key.get();
        List<Vector> list = new ArrayList<Vector>();
        for (WeightedPropertyVectorWritable item : values){
            list.add(item.getVector().clone());
        }
        Vector centerVector = null;
        for(ClusterWritable item : clusterWritableList){
            if (item.getValue().getId() == cluseterId){
                centerVector = item.getValue().getCenter();
                break;
            }
        }
        if (centerVector == null){
            System.err.println(key);
            return;
        }
        context.getCounter(Counter.CLUSTERS).increment(1);
        outkey.set(cluseterId);
        StringBuffer s = new StringBuffer();
        for(Vector item : list) {
            NamedVector namedVector = (NamedVector)item;
            double distance = measure.distance(namedVector.getDelegate(), centerVector);
            Double itemid = Double.parseDouble(namedVector.getName());
            s.append(itemid.intValue());
            s.append("\t");
            s.append(distance);
            out.set(s.toString());
            context.write(outkey, out);
            s.setLength(0);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
