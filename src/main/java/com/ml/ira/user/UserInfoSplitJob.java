package com.ml.ira.user;

import com.ml.ira.Constants;
import com.ml.ira.Paths;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VectorWritable;

import java.util.List;
import java.util.Map;

/**
 * 统计用户资料的分割
 * Created by yaming_deng on 14-4-14.
 */
public class UserInfoSplitJob extends AbstractJob {

    @Override
    public int run(String[] args) throws Exception {
        addInputOption();

        Map<String,List<String>> parsedArgs = parseArguments(args);
        if (parsedArgs == null) {
            return -1;
        }

        Path uinfoPath = this.getInputPath();
        this.outputPath = uinfoPath.getParent();

        Path propPath = this.getOutputPath(uinfoPath.getName()+"_gender");
        Paths.deleteIfExists(propPath, this.getConf());

        Job splitJob = new Job(getConf(), "UserInfoSplitJob");
        Configuration configuration = splitJob.getConfiguration();
        configuration.setBoolean(Constants.MAPREDUCE_MAP_OUTPUT_COMPRESS, true);
        configuration.set(Constants.MAPREDUCE_OUTPUTDIR, propPath.toString());
        configuration.set(Constants.MAPREDUCE_INPUTDIR, inputPath.toString());
        configuration.set(NLineInputFormat.LINES_PER_MAP, Constants.USER_LINES_PER_MAP);

        splitJob.setJarByClass(UserGenderSplitMapper.class);
        splitJob.setInputFormatClass(NLineInputFormat.class);

        splitJob.setMapperClass(UserGenderSplitMapper.class);
        splitJob.setMapOutputKeyClass(VarIntWritable.class);
        splitJob.setMapOutputValueClass(VectorWritable.class);

        splitJob.setReducerClass(UserGenderSplitReducer.class);

        //{namedOutput}-(m|r)-{part-number}
        //包含userId
        MultipleOutputs.addNamedOutput(splitJob, "female", SequenceFileOutputFormat.class, VarIntWritable.class, VectorWritable.class);
        MultipleOutputs.addNamedOutput(splitJob, "male", SequenceFileOutputFormat.class, VarIntWritable.class, VectorWritable.class);

        boolean succeeded = splitJob.waitForCompletion(true);
        if (!succeeded) {
            return -1;
        }

        return 0;
    }

    public static Path[] getResultPaths(Path inpath){
        Path outputPath = inpath.getParent();
        Path propPath = new Path(outputPath, inpath.getName().replace(".txt", "")+"_gender");
        return new Path[]{new Path(propPath+"/female-r-00000", propPath+"/male-r-00000")};
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new UserInfoSplitJob(), args);
    }
}
