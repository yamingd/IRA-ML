package com.ml.ira.user;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.ml.ira.Constants;
import com.ml.ira.Counters;
import com.ml.ira.JobCreator;
import com.ml.ira.propotion.AttrValueWritable;
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
 * 统计用户资料的分布
 * hadoop jar xx.jar com.ml.ira.data.PropotionJob -i hdfs://10.10.10.x:9000/yamingd/ml/uinfo/user.txt
 * Created by yaming_deng on 14-4-14.
 */
public class UserInfoPropotionJob extends AbstractJob {

    public static final String NUM_USERS = "numUsers.bin";
    public static final String USER_PROP = "user_prop";

    private void deleteIfExists(Path path) throws IOException {
        FileSystem ofs = path.getFileSystem(getConf());
        if (ofs.exists(path)) {
            ofs.delete(path, true);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        addInputOption();

        Map<String,List<String>> parsedArgs = parseArguments(args);
        if (parsedArgs == null) {
            return -1;
        }

        Path uinfoPath = this.getInputPath();
        this.outputPath = uinfoPath.getParent();

        Path propPath = this.getOutputPath(USER_PROP);
        this.deleteIfExists(propPath);

        Job propJob = JobCreator.prepareJob(
                "UserAttrPropotion",
                uinfoPath,
                propPath,
                NLineInputFormat.class,
                UserAttrPropotionMapper.class,
                AttrValueWritable.class,
                VarIntWritable.class,
                UserAttrPropotionReducer.class,
                AttrValueWritable.class,
                VarIntWritable.class,
                SequenceFileOutputFormat.class,
                this.getConf());

        JobConf conf = (JobConf)propJob.getConfiguration();
        conf.set(NLineInputFormat.LINES_PER_MAP, Constants.USER_LINES_PER_MAP);

        long ts1 = System.currentTimeMillis();
        boolean succeeded = propJob.waitForCompletion(true);
        if (!succeeded) {
            return -1;
        }
        long ts = System.currentTimeMillis() - ts1;
        System.out.println("Job DatasetPropotionMapper. duration = " + ts);

        int totalOfUsers = (int) propJob.getCounters().findCounter(Counters.USERS).getValue();
        HadoopUtil.writeInt(totalOfUsers, getOutputPath(NUM_USERS), getConf());

        return 0;
    }

    public static Map<Integer, List<AttrValueWritable>> loadAttrsMap(Path userPath){
        Path propPath = new Path(userPath.getParent(), "user_prop");
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
        ToolRunner.run(new UserInfoPropotionJob(), args);
    }
}
