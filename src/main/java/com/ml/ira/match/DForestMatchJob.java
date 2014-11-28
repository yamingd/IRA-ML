package com.ml.ira.match;

import com.ml.ira.*;
import com.ml.ira.algos.ForestBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.VarIntWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by yaming_deng on 14-4-24.
 */
public class DForestMatchJob extends AbstractJob {

    private static final Logger log = LoggerFactory.getLogger(DForestMatchJob.class);

    private AppConfig appConfig;
    private List<String> steps;
    private Path indexRootPath;

    public DForestMatchJob() throws IOException {
        appConfig = new AppConfig(Constants.MATCH_CONF);
    }

    @Override
    public int run(String[] args) throws Exception {
        JobCreator.wrapConf(appConfig, getConf());
        this.addOption("steps", "steps", "running steps", "1,2");
        this.addOption("path", "path", "user info path", true);

        Map<String,List<String>> parsedArgs = parseArguments(args);
        if (parsedArgs == null) {
            return -1;
        }

        String temp = this.getOption("steps");
        steps = Arrays.asList(temp.split(","));
        String output = appConfig.get("output");
        this.outputPath = new Path(output, "predict");
        this.indexRootPath = new Path(output, "index");

        temp = this.getOption("path");
        if (StringUtils.isBlank(temp)){
            return -1;
        }

        Path uinfoPath = new Path(temp);
        System.out.println("Match Using: " + uinfoPath);
        if (steps.contains("1") && !this.indexUser(uinfoPath)){
            return -1;
        }
        if (steps.contains("2") && !this.startMatch(uinfoPath)){
            return -1;
        }
        return 0;
    }

    private boolean indexUser(Path uinfoPath) throws IOException, ClassNotFoundException, InterruptedException {
        String name = uinfoPath.getName();
        Path indexPath = new Path(indexRootPath, name);
        Paths.deleteIfExists(indexPath, this.getConf());

        Job filterJob = new Job(getConf(), "MatchPredict-indexUser-" + name);
        filterJob.getConfiguration().setBoolean(Constants.MAPREDUCE_MAP_OUTPUT_COMPRESS, true);
        filterJob.getConfiguration().set(Constants.MAPREDUCE_OUTPUTDIR, indexPath.toString());
        filterJob.getConfiguration().set(Constants.MAPREDUCE_INPUTDIR, uinfoPath.toString());
        filterJob.getConfiguration().set(NLineInputFormat.LINES_PER_MAP, Constants.USER_LINES_PER_MAP);

        filterJob.setJarByClass(MatchPredictionSplitMapper.class);
        filterJob.setInputFormatClass(NLineInputFormat.class);

        filterJob.setMapperClass(MatchPredictionSplitMapper.class);
        filterJob.setMapOutputKeyClass(VarIntWritable.class);
        filterJob.setMapOutputValueClass(UserIndexWritable.class);

        filterJob.setReducerClass(MatchPredictionSplitReducer.class);

        //{namedOutput}-(m|r)-{part-number}
        MultipleOutputs.addNamedOutput(filterJob, "female", TextOutputFormat.class, NullWritable.class, UserIndexWritable.class);
        MultipleOutputs.addNamedOutput(filterJob, "male", TextOutputFormat.class, NullWritable.class, UserIndexWritable.class);

        boolean succeeded = filterJob.waitForCompletion(true);
        if (!succeeded) {
            log.error("Job MatchPredict-indexUser Failed.");
            return false;
        }

        int totalFemale = (int) filterJob.getCounters().findCounter(Counters.USERS_FEAMLE).getValue();
        int totalMale = (int) filterJob.getCounters().findCounter(Counters.USERS_MALE).getValue();

        HadoopUtil.writeInt(totalFemale, new Path(indexPath + "/totalFemale.bin"), getConf());
        HadoopUtil.writeInt(totalMale, new Path(indexPath + "/totalMale.bin"), getConf());

        return true;
    }

    private boolean startMatch(Path uinfoPath) throws IOException, ClassNotFoundException, InterruptedException {
        String output = appConfig.get("output");
        ForestBuilder builder = new ForestBuilder(this.getConf(), new Path(output));

        String name = uinfoPath.getName();
        Path indexPath = new Path(indexRootPath, name);
        int totalFemale = HadoopUtil.readInt(new Path(indexPath + "/totalFemale.bin"), getConf());
        int totalMale = HadoopUtil.readInt(new Path(indexPath + "/totalMale.bin"), getConf());

        Path femalePath = new Path(indexPath + "/female-r-00000");
        Path malePath = new Path(indexPath + "/male-r-00000");
        Path datasetPath = builder.getDatasetPath();
        Path forestPath = builder.getModelPath();

        Path matchOutput = this.getOutputPath(name);
        Paths.deleteIfExists(matchOutput, this.getConf());

        Job matchJob = new Job(getConf(), "MatchPredict-classify-" + name);
        Configuration jobConf = matchJob.getConfiguration();

        // put the dataset into the DistributedCache
        matchJob.addCacheFile(datasetPath.toUri());
        log.info("Adding the decision forest to the DistributedCache");
        matchJob.addCacheFile(forestPath.toUri());

        if (totalFemale > totalMale){
            matchJob.addCacheFile(malePath.toUri());
            jobConf.set(Constants.MAPREDUCE_INPUTDIR, femalePath.toString());
            jobConf.set("total_cands", totalMale + "");
        }else{
            matchJob.addCacheFile(femalePath.toUri());
            jobConf.set(Constants.MAPREDUCE_INPUTDIR, malePath.toString());
            jobConf.set("total_cands", totalFemale + "");
        }

        matchJob.setInputFormatClass(NLineInputFormat.class);
        matchJob.setJarByClass(MatchPredictionClassifyMapper.class);
        matchJob.setMapperClass(MatchPredictionClassifyMapper.class);
        matchJob.setMapOutputKeyClass(UserMatchKeyWritable.class);
        matchJob.setMapOutputValueClass(UserMatchSocreWritable.class);

        matchJob.setNumReduceTasks(0);

        //{namedOutput}-(m|r)-{part-number}
        MultipleOutputs.addNamedOutput(matchJob, "female", TextOutputFormat.class, UserMatchKeyWritable.class, UserMatchSocreWritable.class);
        MultipleOutputs.addNamedOutput(matchJob, "male", TextOutputFormat.class, UserMatchKeyWritable.class, UserMatchSocreWritable.class);

        int lines = MatchPredictionClassifyMapper.usersPerMatch * 5;
        jobConf.set(NLineInputFormat.LINES_PER_MAP, lines + "");
        jobConf.setBoolean(Constants.MAPREDUCE_MAP_OUTPUT_COMPRESS, true);
        jobConf.set(Constants.MAPREDUCE_OUTPUTDIR, matchOutput.toString());

        JobCreator.setIOSort(matchJob);
        boolean succeeded = matchJob.waitForCompletion(true);
        if (!succeeded) {
            log.error("Job Failed. MatchPredict-shuffle-" + name);
            return false;
        }

        long iterateNo = matchJob.getCounters().findCounter(MatchPredictionClassifyMapper.Counters.IterateNo).getValue();
        long matchYes = matchJob.getCounters().findCounter(MatchPredictionClassifyMapper.Counters.MatchYes).getValue();
        long matchNo = matchJob.getCounters().findCounter(MatchPredictionClassifyMapper.Counters.MatchNo).getValue();
        long duration = matchJob.getCounters().findCounter(MatchPredictionClassifyMapper.Counters.Duration).getValue();

        System.out.println("iterateNo: " + iterateNo);
        System.out.println("matchYes: " + matchYes);
        System.out.println("matchNo: " + matchNo);
        System.out.println("duration: " + duration);

        return true;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new DForestMatchJob(), args);
    }
}
