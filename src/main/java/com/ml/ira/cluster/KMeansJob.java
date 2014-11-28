package com.ml.ira.cluster;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.clustering.conversion.InputDriver;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.ClassUtils;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirValueIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by yaming_deng on 14-4-25.
 */
public class KMeansJob extends AbstractJob{

    private static final Logger log = LoggerFactory.getLogger(KMeansJob.class);
    private static final String DIRECTORY_CONTAINING_CONVERTED_INPUT = "data";

    public static void main(String[] args) throws Exception {
        log.info("Running with only user-supplied arguments");
        ToolRunner.run(new Configuration(), new KMeansJob(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        addInputOption();
        addOutputOption();
        addOption(DefaultOptionCreator.distanceMeasureOption().create());
        addOption(DefaultOptionCreator.numClustersOption().create());
        //addOption(DefaultOptionCreator.t1Option().create());
        //addOption(DefaultOptionCreator.t2Option().create());
        addOption(DefaultOptionCreator.convergenceOption().create());
        addOption(DefaultOptionCreator.maxIterationsOption().create());
        addOption(DefaultOptionCreator.overwriteOption().create());
        addOption("vecf", "vf", "to convert data into vector format first.", "1");

        Map<String,List<String>> argMap = parseArguments(args);
        if (argMap == null) {
            return -1;
        }

        Path input = getInputPath();
        Path output = getOutputPath();
        String measureClass = getOption(DefaultOptionCreator.DISTANCE_MEASURE_OPTION);
        if (measureClass == null) {
            measureClass = SquaredEuclideanDistanceMeasure.class.getName();
        }
        double convergenceDelta = Double.parseDouble(getOption(DefaultOptionCreator.CONVERGENCE_DELTA_OPTION));
        int maxIterations = Integer.parseInt(getOption(DefaultOptionCreator.MAX_ITERATIONS_OPTION));
        if (hasOption(DefaultOptionCreator.OVERWRITE_OPTION)) {
            HadoopUtil.delete(getConf(), output);
        }
        DistanceMeasure measure = ClassUtils.instantiateAs(measureClass, DistanceMeasure.class);
        int k = Integer.parseInt(getOption(DefaultOptionCreator.NUM_CLUSTERS_OPTION));

        boolean vecf = getOption("vecf").equals("1");
        run(getConf(), input, output, measure, k, convergenceDelta, maxIterations, vecf);

        return 0;
    }

    /**
     * Run the kmeans clustering job on an input dataset using the given the number of clusters k and iteration
     * parameters. All output data will be written to the output directory, which will be initially deleted if it exists.
     * The clustered points will reside in the path <output>/clustered-points. By default, the job expects a file
     * containing equal length space delimited data that resides in a directory named "testdata", and writes output to a
     * directory named "output".
     *
     * @param conf
     *          the Configuration to use
     * @param input
     *          the String denoting the input directory path
     * @param output
     *          the String denoting the output directory path
     * @param measure
     *          the DistanceMeasure to use
     * @param k
     *          the number of clusters in Kmeans
     * @param convergenceDelta
     *          the double convergence criteria for iterations
     * @param maxIterations
     *          the int maximum number of iterations
     * @param vecformat
     *          convert data to vector format.
     */
    public List<Integer> run(Configuration conf, Path input, Path output, DistanceMeasure measure, int k,
                           double convergenceDelta, int maxIterations, boolean vecformat) throws Exception {

        System.out.println("input:"+input);
        System.out.println("output:"+output);
        System.out.println("k:"+k);
        System.out.println("maxIterations:"+maxIterations);

        Path directoryContainingConvertedInput = null;
        if (vecformat) {
            directoryContainingConvertedInput = new Path(output, DIRECTORY_CONTAINING_CONVERTED_INPUT);
            log.info("Preparing Input");
            InputDriver.runJob(input, directoryContainingConvertedInput, "org.apache.mahout.math.RandomAccessSparseVector");
        }else{
            directoryContainingConvertedInput = input;
        }
        log.info("Running random seed to get initial clusters");
        Path clusters = new Path(output, "random-seeds");
        clusters = RandomSeedGenerator.buildRandom(conf, directoryContainingConvertedInput, clusters, k, measure);
        log.info("Running KMeans with k = {}", k);
        KMeansDriver.run(conf, directoryContainingConvertedInput, clusters, output, convergenceDelta,
                maxIterations, true, 0.0, false);
        // run ClusterDumper
        Path outGlob = new Path(output, "clusters-*-final");
        Path clusteredPoints = new Path(output,"clusteredPoints");
        log.info("Dumping out clusters from clusters: {} and clusteredPoints: {}", outGlob, clusteredPoints);

        List<Integer> clusterIds = this.getClusterIds(outGlob);
        return clusterIds;
    }

    public void dumpCluster(Configuration conf, Path output) throws Exception {
        // run ClusterDumper
        Path outGlob = new Path(output, "clusters-*-final");
        Path clusteredPoints = new Path(output,"clusteredPoints");

        log.info("Dumping out clusters from clusters: {} and clusteredPoints: {}", outGlob, clusteredPoints);
        //ClusterDumper clusterDumper = new ClusterDumper(outGlob, clusteredPoints);
        //clusterDumper.printClusters(null);

        List<Integer> clusterIds = this.getClusterIds(outGlob);
        System.out.println("Cluster No. " + clusterIds.size());
    }

    private Map<Integer, List<WeightedPropertyVectorWritable>> clusterIdToPoints;

    private Map<Integer, List<WeightedPropertyVectorWritable>> readPoints(Path pointsPathDir) {
        Configuration conf = this.getConf();
        Map<Integer, List<WeightedPropertyVectorWritable>> result = Maps.newTreeMap();
        for (Pair<IntWritable, WeightedPropertyVectorWritable> record
                : new SequenceFileDirIterable<IntWritable, WeightedPropertyVectorWritable>(pointsPathDir, PathType.LIST,
                PathFilters.logsCRCFilter(), conf)) {
            // value is the cluster id as an int, key is the name/id of the
            // vector, but that doesn't matter because we only care about printing it
            //String clusterId = value.toString();
            int keyValue = record.getFirst().get();
            List<WeightedPropertyVectorWritable> pointList = result.get(keyValue);
            if (pointList == null) {
                pointList = Lists.newArrayList();
                result.put(keyValue, pointList);
            }
            pointList.add(record.getSecond());
        }
        return result;
    }

    public List<Integer> getClusterIds(Path seqFileDir) throws IOException {
        Configuration conf = this.getConf();
        Iterable<ClusterWritable> iterable = new SequenceFileDirValueIterable<ClusterWritable>(new Path(seqFileDir,
                "part-*"), PathType.GLOB, conf);
        List<Integer> clusters = Lists.newArrayList();
        Iterator<ClusterWritable> iterator = iterable.iterator();
        while (iterator.hasNext()) {
            ClusterWritable item = iterator.next();
            clusters.add(item.getValue().getId());
        }
        Collections.sort(clusters);
        return clusters;
    }

    public List<ClusterWritable> getCenters(Path seqFileDir) throws IOException {
        Configuration conf = this.getConf();
        Iterable<ClusterWritable> iterable = new SequenceFileDirValueIterable<ClusterWritable>(new Path(seqFileDir,
                "part-*"), PathType.GLOB, conf);
        List<ClusterWritable> clusters = Lists.newArrayList();
        Iterator<ClusterWritable> iterator = iterable.iterator();
        while (iterator.hasNext()) {
            ClusterWritable item = iterator.next();
            clusters.add(item);
        }
        return clusters;
    }
}
