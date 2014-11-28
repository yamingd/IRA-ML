package com.ml.ira.match;

import com.ml.ira.*;
import com.ml.ira.algos.ForestBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.VarIntWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by yaming_deng on 14-4-21.
 */
public class DForestTestJob extends AbstractJob {

    private static final Logger log = LoggerFactory.getLogger(DForestTestJob.class);
    public static final String TEMP_GROUP = "step1";
    public static final String TEMP_JOIN = "step2";

    private AppConfig appConfig;
    private List<String> steps;

    public DForestTestJob() throws IOException {
        appConfig = new AppConfig(Constants.MATCH_CONF);
    }

    @Override
    public int run(String[] args) throws Exception {
        JobCreator.wrapConf(appConfig, getConf());
        this.addOption("steps", "steps", "running steps", "1,2,3,4");

        Map<String,List<String>> parsedArgs = parseArguments(args);
        if (parsedArgs == null) {
            return -1;
        }

        String temp = this.getOption("steps");
        steps = Arrays.asList(temp.split(","));

        this.outputPath = new Path(appConfig.get("output") + "", "testdata");
        if (!prepareTestData())
            return -1;

        //4. 加载模型并测试
        if (steps.contains("4")){
            this.startTest();
        }
        if (steps.contains("5")){
            Path oGroupPath1 = this.getOutputPath(TEMP_GROUP);
            Paths.deleteIfExists(oGroupPath1, this.getConf());
            Path oGroupPath2 = this.getOutputPath(TEMP_JOIN);
            Paths.deleteIfExists(oGroupPath2, this.getConf());
        }
        return 0;
    }

    private void startTest() throws InterruptedException, IOException, ClassNotFoundException {
        Path rootPath = new Path(appConfig.get("output") + "");
        ForestBuilder forestBuilder = new ForestBuilder(this.getConf(), rootPath);

        Path dataPath = this.getOutputPath("data");
        FileSystem fs = dataPath.getFileSystem(this.getConf());
        FileStatus[] files = fs.listStatus(dataPath);
        for (FileStatus file : files){
            String name = file.getPath().getName();
            if (!name.equalsIgnoreCase("_SUCCESS")){
                System.out.println("Found Test File: " + file.getPath());
                forestBuilder.startTest(file.getPath());
            }
        }
    }

    private boolean prepareTestData() throws IOException, InterruptedException, ClassNotFoundException {
        Path uactionPath = new Path(appConfig.getDataFile("qingyuan"));
        Path uinfoPath = new Path(appConfig.getDataFile("user"));
        //1. Group主动方资料
        if (steps.contains("1") && !step1(uactionPath, uinfoPath)){
            return false;
        }
        //2. Join目标用户资料
        if (steps.contains("2") && !step2(uinfoPath)){
            return false;
        }
        //3. 输出匹配对矩阵
        if (steps.contains("3") && !step3()){
            return false;
        }
        return true;
    }

    private boolean step1(Path uactionPath, Path uinfoPath) throws IOException, ClassNotFoundException, InterruptedException {
        Path oGroupPath = this.getOutputPath(TEMP_GROUP);
        Paths.deleteIfExists(oGroupPath, this.getConf());

        Job partialMultiply0 = new Job(getConf(), "DForestTestJob-Group");
        Configuration partialMultiplyConf = partialMultiply0.getConfiguration();
        partialMultiplyConf.set("action_name", "qingyuan");

        MultipleInputs.addInputPath(partialMultiply0, uactionPath,
                NLineInputFormat.class, UserActionSplitMapper.class);
        MultipleInputs.addInputPath(partialMultiply0, uinfoPath,
                NLineInputFormat.class, UserInfoSplitMapper.class);

        partialMultiply0.setJarByClass(UserActionSplitMapper.class);

        partialMultiply0.setMapOutputKeyClass(VarIntWritable.class);
        partialMultiply0.setMapOutputValueClass(MatchPairWritable.class);

        partialMultiply0.setReducerClass(MatchMatrixGroupReducer.class);
        partialMultiply0.setOutputFormatClass(SequenceFileOutputFormat.class);
        partialMultiply0.setOutputKeyClass(VarIntWritable.class);
        partialMultiply0.setOutputValueClass(MatchPairWritable.class);

        partialMultiplyConf.set(NLineInputFormat.LINES_PER_MAP, Constants.USER_LINES_PER_MAP);
        partialMultiplyConf.setBoolean(Constants.MAPREDUCE_MAP_OUTPUT_COMPRESS, true);
        partialMultiplyConf.set(Constants.MAPREDUCE_OUTPUTDIR, oGroupPath.toString());

        JobCreator.setIOSort(partialMultiply0);
        boolean succeeded = partialMultiply0.waitForCompletion(true);
        if (!succeeded) {
            log.error("Job DForestTestJob-Group Failed.");
            return false;
        }
        long totalGroup = partialMultiply0.getCounters().findCounter(Counters.MATCH_RULE_GROUPS).getValue();
        long totalGroupErr = partialMultiply0.getCounters().findCounter(Counters.MATCH_RULE_GROUPS_ERR).getValue();
        if (totalGroup <= 0){
            log.error("Job DForestTestJob-Group Failed. totalGroupErr=" + totalGroupErr);
            return false;
        }
        System.out.println("MATCH_PAIR_GROUPS: " + totalGroup);
        return true;
    }

    private boolean step2(Path uinfoPath) throws IOException, ClassNotFoundException, InterruptedException {
        Path oGroupPath = this.getOutputPath(TEMP_GROUP);
        Path tempJoin = this.getOutputPath(TEMP_JOIN);
        Paths.deleteIfExists(tempJoin, this.getConf());

        Job partialMultiply1 = new Job(getConf(), "DForestTestJob-Join");
        Configuration partialMultiplyConf = partialMultiply1.getConfiguration();

        MultipleInputs.addInputPath(partialMultiply1, oGroupPath,
                SequenceFileInputFormat.class,MatchMatrixGroupMapper.class);
        MultipleInputs.addInputPath(partialMultiply1, uinfoPath,
                NLineInputFormat.class, UserInfoSplitMapper.class);

        partialMultiply1.setJarByClass(MatchMatrixJoinReducer.class);

        partialMultiply1.setMapOutputKeyClass(VarIntWritable.class);
        partialMultiply1.setMapOutputValueClass(MatchPairWritable.class);

        partialMultiply1.setReducerClass(MatchMatrixJoinReducer.class);
        partialMultiply1.setOutputFormatClass(SequenceFileOutputFormat.class);
        partialMultiply1.setOutputKeyClass(NullWritable.class);
        partialMultiply1.setOutputValueClass(MatchPairWritable.class);

        partialMultiplyConf.set(NLineInputFormat.LINES_PER_MAP, Constants.USER_LINES_PER_MAP);
        partialMultiplyConf.setBoolean(Constants.MAPREDUCE_MAP_OUTPUT_COMPRESS, true);
        partialMultiplyConf.set(Constants.MAPREDUCE_OUTPUTDIR, tempJoin.toString());

        JobCreator.setIOSort(partialMultiply1);
        boolean succeeded = partialMultiply1.waitForCompletion(true);
        if (!succeeded) {
            log.error("Job DForestTestJob-Join Failed.");
            return false;
        }
        long totalJoin = partialMultiply1.getCounters().findCounter(Counters.MATCH_RULE_JOINS).getValue();
        long totalJoinErr = partialMultiply1.getCounters().findCounter(Counters.MATCH_RULE_JOINS_ERR).getValue();
        if (totalJoin <= 0){
            log.error("Job DForestTestJob-Join Failed. totalJoinErr=" + totalJoinErr);
            return false;
        }
        System.out.println("MATCH_PAIR_JOINS: " + totalJoin);
        return true;
    }

    private boolean step3() throws InterruptedException, IOException, ClassNotFoundException {
        Path tempJoin = this.getOutputPath(TEMP_JOIN);
        Path dataPath = this.getOutputPath("data");
        Job filterJob = new Job(getConf(), "DForestTestJob-Mapper");
        Paths.deleteIfExists(dataPath, this.getConf());

        filterJob.getConfiguration().setBoolean(Constants.MAPREDUCE_MAP_OUTPUT_COMPRESS, true);
        filterJob.getConfiguration().set(Constants.MAPREDUCE_OUTPUTDIR, dataPath.toString());
        filterJob.getConfiguration().set(Constants.MAPREDUCE_INPUTDIR, tempJoin.toString());

        filterJob.setJarByClass(UserMatchTestMapper.class);
        filterJob.setInputFormatClass(SequenceFileInputFormat.class);

        filterJob.setMapperClass(UserMatchTestMapper.class);
        filterJob.setOutputFormatClass(TextOutputFormat.class);
        filterJob.setOutputKeyClass(MatchMatrixRowWritable.class);
        filterJob.setOutputValueClass(NullWritable.class);

        filterJob.setNumReduceTasks(0);

        JobCreator.setIOSort(filterJob);
        boolean succeeded = filterJob.waitForCompletion(true);
        if (!succeeded) {
            log.error("Job DForestTestJob-Mapper Failed.");
            return false;
        }

        return true;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new DForestTestJob(), args);
    }
}
