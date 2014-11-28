package com.ml.ira.propotion;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.ml.ira.JobCreator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.math.VarIntWritable;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * 统计资料的分布
 * hadoop jar xx.jar com.ml.ira.propotion.PropotionJob -i hdfs://10.10.10.x:9000/yamingd/ml/uinfo/user.txt -p match
 * Created by yaming_deng on 14-4-14.
 */
public class PropotionJob extends AbstractJob {

    public static final String NUM_RECORDS = "num%s.bin";
    public static final String FOLDER_PROP = "%s_prop";

    public static String LINES_PER_MAP = "50000";

    private String appName;

    private void deleteIfExists(Path path) throws IOException {
        FileSystem ofs = path.getFileSystem(getConf());
        if (ofs.exists(path)) {
            ofs.delete(path, true);
        }
    }

    public static Path getPropPath(Path inPath){
        Path propPath = new Path(String.format(FOLDER_PROP, inPath.getName()));
        return propPath;
    }

    @Override
    public int run(String[] args) throws Exception {
        addInputOption();
        this.addOption("app", "p", "the name of app to run", true);

        Map<String,List<String>> parsedArgs = parseArguments(args);
        if (parsedArgs == null) {
            return -1;
        }

        this.outputPath = getInputPath().getParent();
        this.appName = this.getOption("app");

        Path propPath = this.getOutputPath(String.format(FOLDER_PROP, getInputPath().getName()));
        this.deleteIfExists(propPath);

        Job propJob = JobCreator.prepareJob(
                "DatasetPropotion - " + inputPath,
                getInputPath(),
                propPath,
                NLineInputFormat.class,
                DatasetPropotionMapper.class,
                AttrValueWritable.class,
                VarIntWritable.class,
                DatasetPropotionReducer.class,
                AttrValueWritable.class,
                VarIntWritable.class,
                SequenceFileOutputFormat.class,
                this.getConf());

        JobConf conf = (JobConf)propJob.getConfiguration();
        conf.set(NLineInputFormat.LINES_PER_MAP, LINES_PER_MAP);
        conf.set(DatasetPropotionMapper.APP_NAME, this.appName);

        long ts1 = System.currentTimeMillis();
        boolean succeeded = propJob.waitForCompletion(true);
        if (!succeeded) {
            return -1;
        }
        long ts = System.currentTimeMillis() - ts1;
        System.out.println("Job DatasetPropotionMapper. duration = " + ts);

        Path path = getOutputPath(String.format(NUM_RECORDS, this.getInputPath().getName()));
        int totalOfUsers = (int) propJob.getCounters().findCounter(DatasetPropotionMapper.Counters.TOTAL_RECORD).getValue();
        HadoopUtil.writeInt(totalOfUsers, path, getConf());

        return 0;
    }

    public static Map<Integer, List<AttrValueWritable>> loadPropotionMap(Path inputPath){
        Path propPath = new Path(inputPath.getParent(), String.format(FOLDER_PROP, inputPath.getName()));
        Configuration conf = new Configuration();
        Map<Integer, List<AttrValueWritable>> result = Maps.newTreeMap();
        for (Pair<AttrValueWritable, VarIntWritable> record
                : new SequenceFileDirIterable<AttrValueWritable, VarIntWritable>(propPath, PathType.LIST,
                PathFilters.logsCRCFilter(), conf)) {

            int id = record.getFirst().getAttr();
            List<AttrValueWritable> list = result.get(id);
            if (list == null){
                list = Lists.newArrayList();
                result.put(id, list);
            }
            record.getFirst().setCount(record.getSecond().get());
            list.add(record.getFirst());
        }
        return result;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new PropotionJob(), args);
    }
}
