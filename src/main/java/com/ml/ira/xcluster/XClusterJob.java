package com.ml.ira.xcluster;

import com.ml.ira.AppConfig;
import com.ml.ira.Constants;
import com.ml.ira.JobCreator;
import com.ml.ira.Paths;
import com.ml.ira.cluster.ClusterUserSplitMapper;
import com.ml.ira.user.UserInfoPropotionJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.VarIntWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by yaming_deng on 14-5-14.
 */
public class XClusterJob extends AbstractJob {

    public static final String PRIOR_PATH_KEY = "xclustering.prior.path";

    private static final Logger log = LoggerFactory.getLogger(XClusterJob.class);
    public static final String CLUSTERED_USERS = "clusteredUsers";

    private AppConfig appConfig;
    private String clusterName;
    private int kOfClusters;
    private int numIterations;
    private double convergenceDelta;
    private boolean dump;

    public XClusterJob() throws IOException {
        this.appConfig = new AppConfig(Constants.CONF_CLUSTER);
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new XClusterJob(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        JobCreator.wrapConf(appConfig, getConf());
        this.addOption("cluster", "c", "the name of cluster to run", true);
        this.addOption("steps", "sp", "steps to run", "0,1,2,3");
        this.addOption("knum", "k", "total number of clusters", "10");
        this.addOption("round", "r", "total number of iteration to run", "100");
        this.addOption("cd", "cd", "convergenceDelta", "0.001");
        this.addOption("dump", "dump", "dump mapper data to text", "0");
        this.addOption("itc", "itc", "iteration's center", "0");

        Map<String,List<String>> argMap = parseArguments(args);
        if (argMap == null) {
            return -1;
        }

        clusterName = this.getOption("cluster");
        kOfClusters = this.getInt("knum", 10);
        numIterations = this.getInt("round", 100);
        convergenceDelta = this.getFloat("cd");
        dump = this.getInt("dump") == 1;

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

        if (steps.contains("2") && !this.trainCenter("female")){
            return -1;
        }

        if (steps.contains("21")){
            this.dumpCenter("female");
        }

        if (steps.contains("99")){
            Paths.deleteIfExists(this.tempPath, this.getConf());
        }

        return 0;
    }

    private boolean prepareMatrix() throws Exception {
        Path infoPath = this.getTempPath("matrix2");
        Paths.deleteIfExists(infoPath, this.getConf());

        String jobName = "Cluster-" + clusterName + " - prepareMatrix";

        Job splitJob = new Job(getConf(), jobName);
        Configuration configuration = splitJob.getConfiguration();
        configuration.setBoolean(Constants.MAPREDUCE_MAP_OUTPUT_COMPRESS, true);
        configuration.set(Constants.MAPREDUCE_OUTPUTDIR, infoPath.toString());
        configuration.set(Constants.MAPREDUCE_INPUTDIR, inputPath.toString());
        configuration.set(NLineInputFormat.LINES_PER_MAP, Constants.USER_LINES_PER_MAP);

        splitJob.setJarByClass(RDMapper.class);
        splitJob.setInputFormatClass(NLineInputFormat.class);

        splitJob.setMapperClass(RDMapper.class);
        splitJob.setMapOutputKeyClass(VarIntWritable.class);
        splitJob.setMapOutputValueClass(Text.class);

        splitJob.setReducerClass(RDReducer.class);

        splitJob.addCacheFile(new Path(this.inputPath.getParent(), UserInfoPropotionJob.USER_PROP).toUri());

        //{namedOutput}-(m|r)-{part-number}
        //包含userId
        MultipleOutputs.addNamedOutput(splitJob, "female1", TextOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(splitJob, "male1", TextOutputFormat.class, NullWritable.class, Text.class);

        //不包含userId
        MultipleOutputs.addNamedOutput(splitJob, "female0", TextOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(splitJob, "male0", TextOutputFormat.class, NullWritable.class, Text.class);

        boolean succeeded = splitJob.waitForCompletion(true);
        if (!succeeded) {
            log.error("Job Failed. " + jobName);
            return false;
        }

        return true;
    }

    private void dumpCenter(String fileName) throws Exception {
        Path resultPath = this.getOutputPath(fileName+"2");

        String itc = this.getOption("itc");
        if (itc.equals("0")) {
            Path clusters = new Path(resultPath, "random-seeds");
            XCenterSeedGenerator.dumpCenter(this.getConf(), clusters);
        }else {
            Path clustersFinal = new Path(resultPath, "clusters-*-final");
            XCenterSeedGenerator.dumpCenter(this.getConf(), clustersFinal);
        }
    }

    private boolean trainCenter(String fileName) throws Exception {
        this.getConf().set(ClusterUserSplitMapper.CLUSTER_NAME, clusterName);
        this.getConf().set(NLineInputFormat.LINES_PER_MAP, Constants.USER_LINES_PER_MAP);
        URI uri = new Path(this.inputPath.getParent(), UserInfoPropotionJob.USER_PROP).toUri();
        DistributedCache.addCacheFile(uri, this.getConf());

        Path infoPath = this.getTempPath("matrix2");
        Path dataPath = new Path(infoPath + "/" + fileName + "0-r-00000");
        Path resultPath = this.getOutputPath(fileName+"2");
        Paths.deleteIfExists(resultPath, this.getConf());

        Path clusters = new Path(resultPath, "random-seeds");
        Paths.deleteIfExists(clusters, this.getConf());
        clusters = XCenterSeedGenerator.buildRandom(this.getConf(), dataPath, clusters, kOfClusters, null, null);

        Path clustersOut = null;
        int iteration = 0;
        Configuration conf = this.getConf();

        Path priorPath = clusters.getParent();

        while (iteration < numIterations) {
            iteration ++;
            conf.set(PRIOR_PATH_KEY, priorPath.toString());
            String jobName = "Cluster Iterator running iteration " + iteration + " over priorPath: " + priorPath;
            Job job = new Job(conf, jobName);
            job.setMapOutputKeyClass(VarIntWritable.class);
            job.setMapOutputValueClass(XCVectorWritable.class);

            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(XClusterWritable.class);

            job.setInputFormatClass(NLineInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            job.setMapperClass(CCenterTrainMapper.class);
            job.setReducerClass(CCenterTrainReducer.class);

            FileInputFormat.addInputPath(job, dataPath);
            clustersOut = new Path(resultPath, Cluster.CLUSTERS_DIR + iteration);
            FileOutputFormat.setOutputPath(job, clustersOut);
            job.setJarByClass(CCenterTrainMapper.class);
            if (!job.waitForCompletion(true)) {
                throw new InterruptedException("Cluster Iteration " + iteration + " failed processing " + priorPath);
            }
            //检查稳定性
            priorPath = clustersOut;
            boolean convered = true;
            List<XClusterWritable> clusterWritableList = XCenterSeedGenerator.loadCenters(this.getConf(), clustersOut);
            for (XClusterWritable cluster : clusterWritableList){
                System.out.println(cluster.toString());
                if (!cluster.isConvered()){
                    convered = false;
                    break;
                }
            }
            if (convered){
                break;
            }
        }

        Path finalClustersIn = new Path(resultPath, Cluster.CLUSTERS_DIR + iteration + Cluster.FINAL_ITERATION_SUFFIX);
        FileSystem.get(clustersOut.toUri(), conf).rename(clustersOut, finalClustersIn);

        return false;
    }
}
