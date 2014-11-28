package com.ml.ira.xcluster;

import com.ml.ira.distance.GowerDistanceMeasure;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.VarIntWritable;

import java.io.IOException;
import java.util.List;

/**
 *
 * Created by yaming_deng on 14-5-14.
 */
public class CCenterTrainMapper extends Mapper<LongWritable, Text, VarIntWritable, XCVectorWritable> {

    private List<XClusterWritable> centers;
    private GowerDistanceMeasure measure;

    private VarIntWritable outkey = new VarIntWritable();
    private XCVectorWritable outval = new XCVectorWritable();

    public CCenterTrainMapper() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        String str = context.getConfiguration().get(XClusterJob.PRIOR_PATH_KEY);
        Path centerPath = new Path(str);
        try {
            centers = XCenterSeedGenerator.loadCenters(context.getConfiguration(), centerPath);
            measure = new GowerDistanceMeasure();
            measure.configure(context.getConfiguration());
        } catch (Exception e) {
            e.printStackTrace();
            throw new InterruptedException();
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        double[] vector = XCenterSeedGenerator.parseText(value.toString().split("\t"));
        double[] distances = new double[centers.size()];
        int i = 0;
        for (XClusterWritable center : centers){
            distances[i] = measure.distance(vector, center.getCenter());
            i++;
        }
        double min = distances[0];
        for (int j = 1; j < distances.length; j++) {
            if (distances[j] <= min){
               min = distances[j];
            }
        }
        for (int j=0; j < distances.length; j++){
            if (distances[j] == min){
                outkey.set(centers.get(j).getClusterId());
                outval.setVector(vector);
                outval.setClusterId(centers.get(j).getClusterId());
                outval.setDistance(min);
                context.write(outkey, outval);
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
