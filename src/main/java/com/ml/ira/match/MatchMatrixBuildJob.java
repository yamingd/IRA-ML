package com.ml.ira.match;

import com.ml.ira.*;
import com.ml.ira.algos.ForestBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.classifier.df.DFUtils;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.VarIntWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * 计算矩阵相乘 ActionTable X Users
 * Created by yaming_deng on 14-4-14.
 */
public class MatchMatrixBuildJob extends AbstractJob {

    private static final Logger log = LoggerFactory.getLogger(MatchMatrixBuildJob.class);

    public static final String MATCH_MATRIX = "match_matrix";

    private AppConfig appConfig = null;

    public MatchMatrixBuildJob() throws IOException {
        appConfig = new AppConfig(Constants.MATCH_CONF);
    }

    @Override
    public int run(String[] args) throws Exception {
        JobCreator.wrapConf(appConfig, getConf());

        this.addOption("steps", "steps", "running steps", "1,2,3,4,5");
        Map<String,List<String>> parsedArgs = parseArguments(args);
        if (parsedArgs == null) {
            return -1;
        }
        String temp = this.getOption("steps");
        List<String> steps = Arrays.asList(temp.split(","));

        this.outputPath = new Path(appConfig.get("output") + "");

        Path uinfoPath = new Path(appConfig.getDataFile("user"));

        Path actionTblPath = this.getOutputPath(ActionTablePrepareJob.ACTION_TABLE);
        if (!Paths.ifExists(actionTblPath, this.getConf())){
            log.error("Path does not exists. path=" + actionTblPath.toString());
            return -1;
        }
        Path maxtrixPath = this.getOutputPath(MATCH_MATRIX);
        if (steps.contains("1") && this.step1(uinfoPath, actionTblPath) == null)
            return -1;
        if (steps.contains("2") && this.step2(uinfoPath) == null)
            return -1;
        if (steps.contains("3") && !this.step3(maxtrixPath))
            return -1;
        //4. 生成Matrix描述数据
        if (steps.contains("4") && !this.step4(new Path(maxtrixPath.toString() + "/part-r-00000"))){
            return -1;
        }
        if (steps.contains("5")){
            Paths.deleteIfExists(this.getOutputPath("match_matrix0"), this.getConf());
            Paths.deleteIfExists(this.getOutputPath("match_matrix1"), this.getConf());
        }
        return 0;
    }

    private Path step1(Path uinfoPath, Path actionTblPath) throws IOException, ClassNotFoundException, InterruptedException {
        Path matchMatrix0 = this.getOutputPath("match_matrix0");
        Paths.deleteIfExists(matchMatrix0, this.getConf());

        Job partialMultiply0 = new Job(getConf(), "MatchMatrix-Group");
        Configuration partialMultiplyConf = partialMultiply0.getConfiguration();

        MultipleInputs.addInputPath(partialMultiply0, actionTblPath, SequenceFileInputFormat.class,
                ActionTableSplitMapper.class);
        MultipleInputs.addInputPath(partialMultiply0, uinfoPath,
                NLineInputFormat.class, UserInfoSplitMapper.class);

        partialMultiply0.setJarByClass(MatchMatrixGroupReducer.class);

        partialMultiply0.setMapOutputKeyClass(VarIntWritable.class);
        partialMultiply0.setMapOutputValueClass(MatchPairWritable.class);

        partialMultiply0.setReducerClass(MatchMatrixGroupReducer.class);
        partialMultiply0.setOutputFormatClass(SequenceFileOutputFormat.class);
        partialMultiply0.setOutputKeyClass(VarIntWritable.class);
        partialMultiply0.setOutputValueClass(MatchPairWritable.class);

        partialMultiplyConf.set(NLineInputFormat.LINES_PER_MAP, Constants.USER_LINES_PER_MAP);
        partialMultiplyConf.setBoolean(Constants.MAPREDUCE_MAP_OUTPUT_COMPRESS, true);
        partialMultiplyConf.set(Constants.MAPREDUCE_OUTPUTDIR, matchMatrix0.toString());

        JobCreator.setIOSort(partialMultiply0);
        boolean succeeded = partialMultiply0.waitForCompletion(true);
        if (!succeeded) {
            log.error("Job MatchMatrix-Group Failed.");
            return null;
        }
        long totalGroup = partialMultiply0.getCounters().findCounter(Counters.MATCH_RULE_GROUPS).getValue();
        long totalRight = partialMultiply0.getCounters().findCounter(Counters.MATCH_RIGHT).getValue();
        long totalWrong = partialMultiply0.getCounters().findCounter(Counters.MATCH_WRONG).getValue();
        System.out.println("totalGroup: " + totalGroup);
        System.out.println("totalRight: " + totalRight);
        System.out.println("totalWrong: " + totalWrong);

        StringBuilder s = new StringBuilder(100);
        s.append("totalGroup").append("\t").append(totalGroup).append("\n");
        s.append("totalRight").append("\t").append(totalRight).append("\n");
        s.append("totalWrong").append("\t").append(totalWrong).append("\n");

        Path path = new Path(this.outputPath+"/matrix_step1.info");
        DFUtils.storeString(this.getConf(), path, s.toString());

        return matchMatrix0;
    }

    private Path step2(Path uinfoPath) throws IOException, ClassNotFoundException, InterruptedException {
        Path matchMatrix1 = this.getOutputPath("match_matrix1");
        Path matchMatrix0 = this.getOutputPath("match_matrix0");
        Paths.deleteIfExists(matchMatrix1, this.getConf());

        Job partialMultiply1 = new Job(getConf(), "MatchMatrix-Join");
        Configuration partialMultiplyConf = partialMultiply1.getConfiguration();

        MultipleInputs.addInputPath(partialMultiply1, matchMatrix0, SequenceFileInputFormat.class,
                MatchMatrixGroupMapper.class);
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
        partialMultiplyConf.set(Constants.MAPREDUCE_OUTPUTDIR, matchMatrix1.toString());

        JobCreator.setIOSort(partialMultiply1);
        boolean succeeded = partialMultiply1.waitForCompletion(true);
        if (!succeeded) {
            log.error("Job MatchMatrix-Join Failed.");
            return null;
        }
        long totalJoin = partialMultiply1.getCounters().findCounter(Counters.MATCH_RULE_JOINS).getValue();
        long totalRight = partialMultiply1.getCounters().findCounter(Counters.MATCH_RIGHT).getValue();
        long totalWrong = partialMultiply1.getCounters().findCounter(Counters.MATCH_WRONG).getValue();
        System.out.println("totalJoin: " + totalJoin);
        System.out.println("totalRight: " + totalRight);
        System.out.println("totalWrong: " + totalWrong);

        StringBuilder s = new StringBuilder(100);
        s.append("totalJoin").append("\t").append(totalJoin).append("\n");
        s.append("totalRight").append("\t").append(totalRight).append("\n");
        s.append("totalWrong").append("\t").append(totalWrong).append("\n");

        Path path = new Path(this.outputPath+"/matrix_step2.info");
        DFUtils.storeString(this.getConf(), path, s.toString());

        return matchMatrix1;
    }

    private boolean step3(Path maxtrixPath) throws IOException, ClassNotFoundException, InterruptedException {
        Paths.deleteIfExists(maxtrixPath, this.getConf());
        Path matchMatrix1 = this.getOutputPath("match_matrix1");
        Job filterJob = new Job(getConf(), "MatchMatrix-Filter");
        filterJob.getConfiguration().setBoolean(Constants.MAPREDUCE_MAP_OUTPUT_COMPRESS, true);
        filterJob.getConfiguration().set(Constants.MAPREDUCE_OUTPUTDIR, maxtrixPath.toString());
        filterJob.getConfiguration().set(Constants.MAPREDUCE_INPUTDIR, matchMatrix1.toString());

        filterJob.setJarByClass(MatchMatrixFilterMapper.class);
        filterJob.setInputFormatClass(SequenceFileInputFormat.class);

        filterJob.setMapperClass(MatchMatrixFilterMapper.class);
        filterJob.setMapOutputKeyClass(MatchMatrixRowWritable.class);
        filterJob.setMapOutputValueClass(VarIntWritable.class);

        filterJob.setReducerClass(MatchMatrixFilterReducer.class);
        filterJob.setOutputFormatClass(TextOutputFormat.class);
        filterJob.setOutputKeyClass(MatchMatrixRowWritable.class);
        filterJob.setOutputValueClass(NullWritable.class);

        JobCreator.setIOSort(filterJob);
        boolean succeeded = filterJob.waitForCompletion(true);
        if (!succeeded) {
            log.error("Job MatchMatrix-Filter Failed.");
            return false;
        }

        int total0 = (int) filterJob.getCounters().findCounter(Counters.MATCH_RULES).getValue();
        long totalRight = filterJob.getCounters().findCounter(Counters.MATCH_RIGHT).getValue();
        long totalWrong = filterJob.getCounters().findCounter(Counters.MATCH_WRONG).getValue();
        System.out.println("totalRule: " + total0);
        System.out.println("totalRight: " + totalRight);
        System.out.println("totalWrong: " + totalWrong);

        StringBuilder s = new StringBuilder(100);
        s.append("totalRule").append("\t").append(total0).append("\n");
        s.append("totalRight").append("\t").append(totalRight).append("\n");
        s.append("totalWrong").append("\t").append(totalWrong).append("\n");

        Path path = new Path(this.outputPath+"/matrix_rule.info");
        DFUtils.storeString(this.getConf(), path, s.toString());

        return true;
    }

    private boolean step4(Path matrixPath) throws Exception {
        Path infoPath = new Path(matrixPath.getParent(), "tmp");
        Paths.deleteIfExists(infoPath, this.getConf());
        String jobName = "MatchMatrix-Digest";
        Job digestJob = JobCreator.prepareJob(jobName, matrixPath, infoPath,
                NLineInputFormat.class,
                MatchMatrixDigestMapper.class, MatchMatrixDigestWritable.class, Text.class,
                MatchMatrixDigestReducer.class, MatchMatrixDigestWritable.class, Text.class,
                TextOutputFormat.class,
                getConf());
        digestJob.getConfiguration().set(NLineInputFormat.LINES_PER_MAP, Constants.USER_LINES_PER_MAP);

        JobCreator.setIOSort(digestJob);
        boolean succeeded = digestJob.waitForCompletion(true);
        if (!succeeded) {
            log.error("Job " + jobName +" Failed.");
            return false;
        }

        long totalRight = digestJob.getCounters().findCounter(Counters.MATCH_RIGHT).getValue();
        long totalWrong = digestJob.getCounters().findCounter(Counters.MATCH_WRONG).getValue();
        System.out.println("totalRight: " + totalRight);
        System.out.println("totalWrong: " + totalWrong);

        ForestBuilder forestBuilder = new ForestBuilder(this.getConf(), outputPath);
        forestBuilder.buildDataset(infoPath);

        return true;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new MatchMatrixBuildJob(), args);
    }
}
