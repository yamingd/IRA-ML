package com.ml.ira.match;

import com.ml.ira.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.classifier.df.DFUtils;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.VarIntWritable;

/**
 * 从单向行为记录生成双向行为矩阵, 并过滤记录相冲的行为
 * Created by yaming_deng on 14-4-14.
 */
public class ActionTablePrepareJob extends AbstractJob {

    public static final String ACTION_TABLE = "action_table";

    @Override
    public int run(String[] strings) throws Exception {
        AppConfig appConfig = new AppConfig(Constants.MATCH_CONF);
        JobCreator.wrapConf(appConfig, getConf());
        this.outputPath = new Path(appConfig.get("output") + "");

        String actions = appConfig.get("actions") + "";
        String actionName = "leer";
        Path dataPath = new Path(appConfig.getDataFile(actions));
        Path actionTablePath = getOutputPath(ACTION_TABLE);
        Paths.deleteIfExists(actionTablePath, this.getConf());

        Job filterAction = JobCreator.prepareJob(
                "ActionTablePrepareJob",
                dataPath,
                actionTablePath,
                NLineInputFormat.class,
                ActionRecordMapper.class,
                IDPairWritable.class,
                VarIntWritable.class,
                ActionRecordReducer.class,
                IDPairWritable.class,
                VarIntWritable.class,
                SequenceFileOutputFormat.class,
                this.getConf());
        JobCreator.setIOSort(filterAction);
        JobConf conf = (JobConf)filterAction.getConfiguration();
        conf.set("action_name", actionName);
        conf.set(NLineInputFormat.LINES_PER_MAP, Constants.ACT_LINES_PER_MAP);

        boolean succeeded = filterAction.waitForCompletion(true);
        if (!succeeded) {
            return -1;
        }

        int total = (int) filterAction.getCounters().findCounter(Counters.ACTION_PAIRS).getValue();
        long totalRight = filterAction.getCounters().findCounter(Counters.MATCH_RIGHT).getValue();
        long totalWrong = filterAction.getCounters().findCounter(Counters.MATCH_WRONG).getValue();

        StringBuilder s = new StringBuilder(100);
        s.append("total").append("\t").append(total).append("\n");
        s.append("totalRight").append("\t").append(totalRight).append("\n");
        s.append("totalWrong").append("\t").append(totalWrong).append("\n");

        Path path = new Path(this.outputPath+"/action_table.info");
        DFUtils.storeString(this.getConf(), path, s.toString());

        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new ActionTablePrepareJob(), args);
    }
}
