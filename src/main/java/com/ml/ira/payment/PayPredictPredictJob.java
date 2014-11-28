package com.ml.ira.payment;

import com.ml.ira.AppConfig;
import com.ml.ira.Constants;
import com.ml.ira.Counters;
import com.ml.ira.JobCreator;
import org.apache.hadoop.fs.FileSystem;
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

/**
 * Created by yaming_deng on 14-4-17.
 */
public class PayPredictPredictJob extends AbstractJob {

    private static final Logger log = LoggerFactory.getLogger(PayPredictPredictJob.class);

    private Path uinfoPath;
    private AppConfig appConfig;

    public PayPredictPredictJob() throws IOException {
        this.appConfig = new AppConfig(Constants.PAYMENT_CONF);
    }

    private void deleteIfExists(Path path) throws IOException {
        FileSystem ofs = path.getFileSystem(getConf());
        if (ofs.exists(path)) {
            ofs.delete(path, true);
        }
        ofs.close();
    }

    @Override
    public int run(String[] args) throws Exception {
        JobCreator.wrapConf(appConfig, getConf());
        this.uinfoPath = new Path(appConfig.getDataFile("preduser"));
        this.step3();
        return 0;
    }

    private boolean step3() throws IOException, ClassNotFoundException, InterruptedException {
        String str = appConfig.get("output");
        this.outputPath = new Path(str);
        Path propPath = this.getOutputPath("predict");
        this.deleteIfExists(propPath);

        Job filterJob = new Job(getConf(), "3.PayRecord-Predict");
        filterJob.getConfiguration().setBoolean(Constants.MAPREDUCE_MAP_OUTPUT_COMPRESS, true);
        filterJob.getConfiguration().set(Constants.MAPREDUCE_OUTPUTDIR, propPath.toString());
        filterJob.getConfiguration().set(Constants.MAPREDUCE_INPUTDIR, uinfoPath.toString());
        filterJob.getConfiguration().set(NLineInputFormat.LINES_PER_MAP, Constants.USER_LINES_PER_MAP);

        filterJob.setJarByClass(PayUserPredictMapper.class);
        filterJob.setInputFormatClass(NLineInputFormat.class);

        filterJob.setMapperClass(PayUserPredictMapper.class);
        filterJob.setMapOutputKeyClass(VarIntWritable.class);
        filterJob.setMapOutputValueClass(PredictValueWritable.class);

        filterJob.setReducerClass(PayUserPredictReducer.class);

        //{namedOutput}-(m|r)-{part-number}
        MultipleOutputs.addNamedOutput(filterJob, "pay", TextOutputFormat.class, NullWritable.class, PredictValueWritable.class);
        MultipleOutputs.addNamedOutput(filterJob, "nopay", TextOutputFormat.class, NullWritable.class, PredictValueWritable.class);

        boolean succeeded = filterJob.waitForCompletion(true);
        if (!succeeded) {
            log.error("Job PayRecord-Split Failed.");
            return false;
        }

        int total0 = (int) filterJob.getCounters().findCounter(Counters.PREDICT_PAY).getValue();
        HadoopUtil.writeInt(total0, getOutputPath("predictPay.bin"), getConf());

        int total1 = (int) filterJob.getCounters().findCounter(Counters.PREDICT_NOPAY).getValue();
        HadoopUtil.writeInt(total1, getOutputPath("predictNoPay.bin"), getConf());

        System.out.println("PREDICT_PAY: " + total0);
        System.out.println("PREDICT_NOPAY: " + total1);

        return true;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new PayPredictPredictJob(), args);
    }
}
