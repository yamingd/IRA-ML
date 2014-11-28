package com.ml.ira.cluster;

import com.ml.ira.AppConfig;
import com.ml.ira.Constants;
import com.ml.ira.JobCreator;
import com.ml.ira.Paths;
import com.ml.ira.propotion.PropotionJob;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * 提供一个\t分割的txt文件来做聚类
 * Created by yaming_deng on 14-4-25.
 */
public class ClusterItemRunJob extends AbstractJob {

    private static final Logger log = LoggerFactory.getLogger(ClusterItemRunJob.class);
    public static final String CLUSTERED_ITEMS = "clusteredItems";
    public static final String CLUSTERED_POINTS = "clusteredPoints";
    public static final String CLUSTER_ITEM_INFO_PROP = "clusterItemInfoProp";

    private AppConfig appConfig;
    private String clusterName;
    private int kOfClusters;
    private int roundOfClusters;
    private double convergenceDelta;

    public ClusterItemRunJob() throws IOException {

    }

    @Override
    public int run(String[] args) throws Exception {
        this.addInputOption();
        this.addOption("app", "p", "the name of app to run", true);
        this.addOption("cluster", "c", "the name of cluster to run", "");
        this.addOption("steps", "sp", "steps to run", "0,1,2,3");
        this.addOption("knum", "k", "total number of clusters", "10");
        this.addOption("round", "r", "total number of iteration to run", "100");
        this.addOption("cd", "cd", "convergenceDelta", "0.001");

        Map<String,List<String>> argMap = parseArguments(args);
        if (argMap == null) {
            return -1;
        }

        this.appConfig = new AppConfig(this.getOption("app"));
        JobCreator.wrapConf(appConfig, getConf());

        clusterName = this.getOption("app");
        if(StringUtils.isNotBlank(this.getOption("cluster"))){
            clusterName = this.getOption("cluster");
        }
        kOfClusters = this.getInt("knum", 10);
        roundOfClusters = this.getInt("round", 100);
        this.convergenceDelta = this.getFloat("cd");

        String[] tmp = this.getOption("steps").split(",");
        List<String> steps = Arrays.asList(tmp);

        this.outputPath = new Path(appConfig.getString("output"));
        this.tempPath = this.getOutputPath("temp");

        this.outputPath = this.getOutputPath(clusterName);
        this.tempPath = this.getTempPath(clusterName);
        this.inputPath = new Path(appConfig.getDataFile(clusterName));

        if (steps.contains("0")){
            PropotionJob job = new PropotionJob();
            int ret = job.run(new String[]{
                "-i", this.inputPath.toString(),
                "-p", clusterName
            });
            if (ret == -1) {
                return -1;
            }
        }

        if (steps.contains("1") && !prepareMatrix()){
            return -1;
        }

        if (steps.contains("2")){
            boolean flag1 = this.runKmeans();
            if (!flag1)
                return -1;
            this.dumpKmeans();
        }
        if (steps.contains("3")){
            boolean flag1 = this.mapClusterToItem();
            if (!flag1)
                return -1;
        }
        if (steps.contains("4")){
            this.dumpClusterItemAttrProp();
        }
        if (steps.contains("99")){
            Paths.deleteIfExists(this.tempPath, this.getConf());
        }

        return 0;
    }

    private boolean prepareMatrix() throws IOException, ClassNotFoundException, InterruptedException {
        Path infoPath = this.getTempPath("matrix");
        Paths.deleteIfExists(infoPath, this.getConf());

        String jobName = "Cluster-" + clusterName;

        Job splitJob = new Job(getConf(), jobName);
        Configuration configuration = splitJob.getConfiguration();
        configuration.setBoolean(Constants.MAPREDUCE_MAP_OUTPUT_COMPRESS, true);
        configuration.set(ClusterUserSplitMapper.CLUSTER_NAME, clusterName);
        configuration.set(Constants.MAPREDUCE_OUTPUTDIR, infoPath.toString());
        configuration.set(Constants.MAPREDUCE_INPUTDIR, inputPath.toString());
        configuration.set(NLineInputFormat.LINES_PER_MAP, Constants.USER_LINES_PER_MAP);

        splitJob.setJarByClass(ClusterDataPrepareMapper.class);
        splitJob.setInputFormatClass(NLineInputFormat.class);

        splitJob.setMapperClass(ClusterDataPrepareMapper.class);
        splitJob.setMapOutputKeyClass(VarIntWritable.class);
        splitJob.setMapOutputValueClass(VectorWritable.class);

        splitJob.setReducerClass(ClusterDataPrepareReducer.class);
        splitJob.setOutputKeyClass(Text.class);
        splitJob.setOutputValueClass(VectorWritable.class);

        splitJob.addCacheFile(PropotionJob.getPropPath(inputPath).toUri());

        //{namedOutput}-(m|r)-{part-number}

        boolean succeeded = splitJob.waitForCompletion(true);
        if (!succeeded) {
            log.error("Job Failed. " + jobName);
            return false;
        }

        return true;
    }

    private boolean runKmeans() throws Exception {
        //聚类
        Path infoPath = this.getTempPath("matrix");
        Path dataPath = new Path(infoPath + "/part-r-00000");
        Path resultPath = this.getOutputPath();
        Paths.deleteIfExists(resultPath, this.getConf());

        KMeansJob job = new KMeansJob();
        job.run(this.getConf(), dataPath, resultPath, new SquaredEuclideanDistanceMeasure(), kOfClusters, 0.5, roundOfClusters, false);
        return true;
    }

    private boolean dumpKmeans() throws Exception {
        //聚类
        Path resultPath = this.getOutputPath();
        KMeansJob job = new KMeansJob();
        job.dumpCluster(this.getConf(), resultPath);
        return true;
    }

    private boolean mapClusterToItem() throws IOException, ClassNotFoundException, InterruptedException {
        Path resultPath = this.getOutputPath();
        Path pointPath = new Path(resultPath, CLUSTERED_POINTS);
        Path infoPath = this.getTempPath("matrix");
        Path itemInfoPath = new Path(infoPath + "/part-r-00000");

        return doClusterItemMapping(this.getConf(), resultPath, itemInfoPath);
    }

    public static boolean doClusterItemMapping(Configuration conf, Path outputPath, Path itemInfoPath) throws IOException, InterruptedException, ClassNotFoundException {
        System.out.println("doClusterItemMapping, outputPath: " + outputPath);
        System.out.println("doClusterItemMapping, itemInfoPath: " + itemInfoPath);

        Path pointPath = new Path(outputPath, CLUSTERED_POINTS);
        Path clusteredItemsPath = new Path(outputPath, CLUSTERED_ITEMS);
        Paths.deleteIfExists(clusteredItemsPath, conf);

        Job mapJob = new Job(conf, "Cluster-mapClusterToItem");

        Path path = new Path(outputPath, "clusters-*-final");
        Configuration jobConf = mapJob.getConfiguration();

        jobConf.set(ClusterMappingItemReducer.CENTER_PATH, path.toString());

        mapJob.setJarByClass(ClusterPointMapper.class);
        mapJob.setInputFormatClass(SequenceFileInputFormat.class);
        mapJob.setMapperClass(ClusterPointMapper.class);
        mapJob.setMapOutputKeyClass(IntWritable.class);
        mapJob.setMapOutputValueClass(WeightedPropertyVectorWritable.class);

        mapJob.setReducerClass(ClusterMappingItemReducer.class);
        mapJob.setOutputFormatClass(TextOutputFormat.class);
        mapJob.setOutputKeyClass(VarIntWritable.class);
        mapJob.setOutputValueClass(Text.class);

        jobConf.setBoolean(Constants.MAPREDUCE_MAP_OUTPUT_COMPRESS, true);
        jobConf.set(Constants.MAPREDUCE_OUTPUTDIR, clusteredItemsPath.toString());
        jobConf.set(Constants.MAPREDUCE_INPUTDIR, pointPath.toString());

        JobCreator.setIOSort(mapJob);
        boolean succeeded = mapJob.waitForCompletion(true);
        if (!succeeded) {
            log.error("Job Cluster-mapClusterToItem Failed.");
            return false;
        }
        long total = mapJob.getCounters().findCounter(ClusterMappingItemReducer.Counter.CLUSTERS).getValue();
        System.out.println("Map Cluster: " + total);
        return true;
    }

    private boolean dumpClusterItemAttrProp() throws IOException, ClassNotFoundException, InterruptedException {
        Path resultPath = this.getOutputPath();
        return doDumpClusterItemProp(this.getConf(), clusterName, resultPath);
    }

    public static boolean doDumpClusterItemProp(Configuration conf, String clusterName, Path outputPath) throws IOException, InterruptedException, ClassNotFoundException {
        System.out.println("doClusterItemMapping, outputPath: " + outputPath);
        System.out.println("doClusterItemMapping, clusterName: " + clusterName);

        Path clusteredItemPath = new Path(outputPath, CLUSTERED_ITEMS);
        Path joboutPath = new Path(outputPath, CLUSTER_ITEM_INFO_PROP);
        Paths.deleteIfExists(joboutPath, conf);

        Job mapJob = new Job(conf, "Cluster-dumpClusterItemAttrProp-" + clusteredItemPath);
        Configuration jobConf = mapJob.getConfiguration();

        mapJob.setMapperClass(ClusterItemInfoPropMapper.class);
        mapJob.setJarByClass(ClusterItemInfoPropMapper.class);
        mapJob.setInputFormatClass(NLineInputFormat.class);

        mapJob.setMapOutputKeyClass(ClusterItemAttrValueWritable.class);
        mapJob.setMapOutputValueClass(VarIntWritable.class);

        mapJob.setCombinerClass(ClusterItemInfoPropReducer.class);
        mapJob.setReducerClass(ClusterItemInfoPropReducer.class);

        mapJob.setOutputFormatClass(TextOutputFormat.class);

        mapJob.setOutputKeyClass(ClusterItemAttrValueWritable.class);
        mapJob.setOutputValueClass(VarIntWritable.class);

        jobConf.set(ClusterUserSplitMapper.CLUSTER_NAME, clusterName);
        jobConf.set(NLineInputFormat.LINES_PER_MAP, Constants.LINES_PER_MAP);
        jobConf.setBoolean(Constants.MAPREDUCE_MAP_OUTPUT_COMPRESS, true);

        jobConf.set(Constants.MAPREDUCE_INPUTDIR, clusteredItemPath.toString());
        jobConf.set(Constants.MAPREDUCE_OUTPUTDIR, joboutPath.toString());

        JobCreator.setIOSort(mapJob);
        boolean succeeded = mapJob.waitForCompletion(true);
        if (!succeeded) {
            log.error("Job Cluster-dumpClusterItemAttrProp Failed.");
            return false;
        }

        return true;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new ClusterItemRunJob(), args);
    }
}
