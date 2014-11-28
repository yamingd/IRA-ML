package com.ml.ira.cluster;

import com.ml.ira.AppConfig;
import com.ml.ira.Constants;
import com.ml.ira.JobCreator;
import com.ml.ira.Paths;
import com.ml.ira.user.UserInfoPropotionJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by yaming_deng on 14-4-25.
 */
public class ClusterUserRunJob extends AbstractJob {

    private static final Logger log = LoggerFactory.getLogger(ClusterUserRunJob.class);

    private AppConfig appConfig;
    private String clusterName;
    private int kOfClusters;
    private int roundOfClusters;
    private double convergenceDelta;

    public ClusterUserRunJob() throws IOException {
        this.appConfig = new AppConfig(Constants.CONF_CLUSTER);
    }

    @Override
    public int run(String[] args) throws Exception {
        JobCreator.wrapConf(appConfig, getConf());
        this.addOption("cluster", "c", "the name of cluster to run", true);
        this.addOption("steps", "sp", "steps to run", "0,1,2,3");
        this.addOption("knum", "k", "total number of clusters", "10");
        this.addOption("round", "r", "total number of iteration to run", "100");
        this.addOption("cd", "cd", "convergenceDelta", "0.001");

        Map<String,List<String>> argMap = parseArguments(args);
        if (argMap == null) {
            return -1;
        }

        clusterName = this.getOption("cluster");
        kOfClusters = this.getInt("knum", 10);
        roundOfClusters = this.getInt("round", 100);
        convergenceDelta = this.getFloat("cd");

        String[] tmp = this.getOption("steps").split(",");
        List<String> steps = Arrays.asList(tmp);

        this.outputPath = new Path(appConfig.getString("output"));
        this.inputPath = new Path(appConfig.getDataFile("user"));
        this.tempPath = this.getOutputPath("temp");

        this.outputPath = this.getOutputPath(clusterName);
        this.tempPath = this.getTempPath(clusterName);

        this.getConf().set(ClusterUserSplitMapper.CLUSTER_NAME, clusterName);

        if (steps.contains("0")){
            UserInfoPropotionJob job = new UserInfoPropotionJob();
            int ret = job.run(new String[]{
                "-i", this.inputPath.toString()
            });
            if (ret == -1){
                return -1;
            }
        }

        if (steps.contains("1") && !prepareMatrix()){
            return -1;
        }

        if (steps.contains("2")){
            boolean flag1 = this.runKmeans("female");
            if (!flag1)
                return -1;
        }
        if (steps.contains("3")){
            boolean flag1 = this.mapClusterToUser("female");
            if (!flag1)
                return -1;

        }
        if (steps.contains("31")){
            this.dumpCluster("female");
        }
        if (steps.contains("4")){
            boolean flag1 = this.runKmeans("male");
            if (!flag1)
                return -1;
        }
        if (steps.contains("5")){
            boolean flag1 = this.mapClusterToUser("male");
            if (!flag1)
                return -1;
        }
        if (steps.contains("51")){
            this.dumpCluster("male");
        }
        if (steps.contains("99")){
            Paths.deleteIfExists(this.tempPath, this.getConf());
        }

        return 0;
    }

    private boolean prepareMatrix() throws IOException, ClassNotFoundException, InterruptedException {
        Path infoPath = this.getTempPath("matrix");
        Paths.deleteIfExists(infoPath, this.getConf());

        String jobName = "Cluster-" + clusterName + " - prepareMatrix";

        Job splitJob = new Job(getConf(), jobName);
        Configuration configuration = splitJob.getConfiguration();
        configuration.setBoolean(Constants.MAPREDUCE_MAP_OUTPUT_COMPRESS, true);
        configuration.set(Constants.MAPREDUCE_OUTPUTDIR, infoPath.toString());
        configuration.set(Constants.MAPREDUCE_INPUTDIR, inputPath.toString());
        configuration.set(NLineInputFormat.LINES_PER_MAP, Constants.USER_LINES_PER_MAP);

        splitJob.setJarByClass(ClusterUserSplitMapper.class);
        splitJob.setInputFormatClass(NLineInputFormat.class);

        splitJob.setMapperClass(ClusterUserSplitMapper.class);
        splitJob.setMapOutputKeyClass(VarIntWritable.class);
        splitJob.setMapOutputValueClass(VectorWritable.class);

        splitJob.setReducerClass(ClusterUserSplitReducer.class);

        splitJob.addCacheFile(new Path(this.inputPath.getParent(), UserInfoPropotionJob.USER_PROP).toUri());

        //{namedOutput}-(m|r)-{part-number}
        MultipleOutputs.addNamedOutput(splitJob, "female", SequenceFileOutputFormat.class, Text.class, VectorWritable.class);
        MultipleOutputs.addNamedOutput(splitJob, "male", SequenceFileOutputFormat.class, Text.class, VectorWritable.class);

        boolean succeeded = splitJob.waitForCompletion(true);
        if (!succeeded) {
            log.error("Job Failed. " + jobName);
            return false;
        }

        return true;
    }

    private boolean runKmeans(String fileName) throws Exception {
        //聚类
        Path infoPath = this.getTempPath("matrix");
        Path dataPath = new Path(infoPath + "/" + fileName + "-r-00000");
        Path resultPath = this.getOutputPath(fileName);
        Paths.deleteIfExists(resultPath, this.getConf());

        KMeansJob job = new KMeansJob();
        DistanceMeasure measure = new SquaredEuclideanDistanceMeasure();

        URI uri = new Path(this.inputPath.getParent(), UserInfoPropotionJob.USER_PROP).toUri();
        DistributedCache.addCacheFile(uri, this.getConf());

        job.run(this.getConf(), dataPath, resultPath, measure, kOfClusters, convergenceDelta, roundOfClusters, false);

        return true;
    }

    private void dumpCluster(String fileName) throws Exception {
        //聚类
        Path infoPath = this.getTempPath("matrix");
        Path resultPath = this.getOutputPath(fileName);
        KMeansJob job = new KMeansJob();
        job.dumpCluster(this.getConf(), resultPath);
    }

    private boolean mapClusterToUser(String fileName) throws Exception {
        Path infoPath = this.getTempPath("matrix");
        Path resultPath = this.getOutputPath(fileName);
        Path itemInfoPath = new Path(infoPath + "/"+ fileName +"-r-00000");
        boolean result = ClusterItemRunJob.doClusterItemMapping(this.getConf(), resultPath, itemInfoPath);
        return result;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new ClusterUserRunJob(), args);
    }
}
