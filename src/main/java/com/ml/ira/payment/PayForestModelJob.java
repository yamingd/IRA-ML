package com.ml.ira.payment;

import com.ml.ira.*;
import com.ml.ira.algos.ForestBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.classifier.df.data.DescriptorException;
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
 * Created by yaming_deng on 14-4-17.
 */
public class PayForestModelJob extends AbstractJob {

    private static final Logger log = LoggerFactory.getLogger(PayForestModelJob.class);

    private AppConfig appConfig;
    private PayMatrix payMatrix;
    private ForestBuilder forestBuilder;

    private Path uinfoPath;
    private Path dataPath;
    private int trainSize;
    private int testSize;

    private Path trainDataSetPath;
    private Path testDataSetPath;

    private Integer nbtrees;

    public PayForestModelJob() throws IOException {
        this.appConfig = new AppConfig(Constants.PAYMENT_CONF);
        this.payMatrix = new PayMatrix(appConfig);

        String tmp = appConfig.get("output");
        this.forestBuilder = new ForestBuilder(this.getConf(), new Path(tmp));
    }


    @Override
    public int run(String[] args) throws Exception {

        this.addOption("round", "r", "total round of model training", "10");
        this.addOption("steps", "steps", "steps to run", "1,2,3,4");
        this.addOption("nbtrees", "nt", "total number of trees to build", "0");

        Map<String,List<String>> parsedArgs = parseArguments(args);
        if (parsedArgs == null) {
            return -1;
        }

        String[] str = this.getOption("steps").split(",");
        List<String> steps = Arrays.asList(str);

        JobCreator.wrapConf(appConfig, getConf());
        this.uinfoPath = new Path(appConfig.getDataFile("pay"));
        //1. 标准化属性的值
        String tmp = appConfig.get("output");
        this.outputPath = new Path(tmp);
        this.dataPath = this.getOutputPath("data");
        if (steps.contains("1") && !splitData())
            return -1;
        //2. 生成数据集描述文件
        this.trainDataSetPath = new Path(dataPath, "train-r-00000");
        this.testDataSetPath = new Path(dataPath, "test-r-00000");
        if (steps.contains("2")){
            if (!this.genDeigestMatrix("pay", trainDataSetPath, trainSize))
                return -1;
        }
        //3. 生成模型
        if (steps.contains("3")){
            this.startBuilder();
        }
        //4. 测试
        if(steps.contains("4")){
            this.startTest();
        }
        return 0;
    }

    private boolean splitData() throws IOException, ClassNotFoundException, InterruptedException {
        Paths.deleteIfExists(this.dataPath, this.getConf());

        Job filterJob = new Job(getConf(), "2.PayRecord-Split");
        filterJob.getConfiguration().setBoolean(Constants.MAPREDUCE_MAP_OUTPUT_COMPRESS, true);
        filterJob.getConfiguration().set(Constants.MAPREDUCE_OUTPUTDIR, dataPath.toString());
        filterJob.getConfiguration().set(Constants.MAPREDUCE_INPUTDIR, uinfoPath.toString());
        filterJob.getConfiguration().set(NLineInputFormat.LINES_PER_MAP, Constants.USER_LINES_PER_MAP);

        filterJob.setJarByClass(PayRecordFilterMapper.class);
        filterJob.setInputFormatClass(NLineInputFormat.class);

        filterJob.setMapperClass(PayRecordFilterMapper.class);
        filterJob.setMapOutputKeyClass(VarIntWritable.class);
        filterJob.setMapOutputValueClass(PayRecordWritable.class);

        filterJob.setReducerClass(PayRecordFilterReducer.class);

        //{namedOutput}-(m|r)-{part-number}
        MultipleOutputs.addNamedOutput(filterJob, "train", TextOutputFormat.class, NullWritable.class, PayRecordWritable.class);
        MultipleOutputs.addNamedOutput(filterJob, "test", TextOutputFormat.class, NullWritable.class, PayRecordWritable.class);

        boolean succeeded = filterJob.waitForCompletion(true);
        if (!succeeded) {
            log.error("Job PayRecord-Split Failed.");
            return false;
        }

        int total10 = (int) filterJob.getCounters().findCounter(Counters.PAY_TRAIN_1).getValue();
        int total11 = (int) filterJob.getCounters().findCounter(Counters.PAY_TRAIN_0).getValue();
        HadoopUtil.writeInt(total10 + total11, getOutputPath("numTrain.bin"), getConf());
        trainSize = total10 + total11;

        int total00 = (int) filterJob.getCounters().findCounter(Counters.PAY_TEST_0).getValue();
        int total01 = (int) filterJob.getCounters().findCounter(Counters.PAY_TEST_1).getValue();
        HadoopUtil.writeInt(total01 + total00, getOutputPath("numTest.bin"), getConf());
        testSize = total00 + total01;

        System.out.println("PAY_TRAIN: " + (total10 + total11) + "/" + total10 + "/" + total11);
        System.out.println("PAY_TEST: " + (total00 + total01) + "/" + total00 + "/" + total01);

        return true;
    }

    private boolean genDeigestMatrix(String name, final Path matrixPath, int size) throws Exception {
        Path infoPath = new Path(matrixPath.getParent(), "tmp");
        Paths.deleteIfExists(infoPath, this.getConf());
        String jobName = "PayMatrix-Digest-" + name;
        Job digestJob = JobCreator.prepareJob(jobName, matrixPath, infoPath,
                NLineInputFormat.class,
                PayMatrixDigestMapper.class, PayMatrixDigestWritable.class, Text.class,
                PayMatrixDigestReducer.class, PayMatrixDigestWritable.class, Text.class,
                TextOutputFormat.class,
                getConf());

        digestJob.getConfiguration().set("model_name", name);
        digestJob.getConfiguration().set(NLineInputFormat.LINES_PER_MAP, Constants.USER_LINES_PER_MAP);

        boolean succeeded = digestJob.waitForCompletion(true);
        if (!succeeded) {
            log.error("Job " + jobName +" Failed.");
            return false;
        }

        succeeded = forestBuilder.buildDataset(infoPath);
        if (!succeeded) {
            log.error("Job " + jobName +" Failed.");
            return false;
        }

        return true;
    }

    public void startBuilder() throws IOException, DescriptorException, ClassNotFoundException, InterruptedException {
        Path outputPath0 = new Path(appConfig.get("output") + "");
        this.outputPath = outputPath0;

        Map treeConf = appConfig.get("forest");
        int treeNb = Integer.parseInt(treeConf.get("nbTrees").toString());
        if (nbtrees>0){
            treeNb = nbtrees;
        }
        boolean partial = Boolean.parseBoolean(treeConf.get("partial")+"");
        forestBuilder.setPartial(partial);
        forestBuilder.setNbTrees(treeNb);
        forestBuilder.setComplemented(true);
        forestBuilder.setMinSplitNum(2);
        forestBuilder.setMinVarianceProportion(0.001);
        forestBuilder.setSeed(null);

        forestBuilder.buildForest(trainDataSetPath);
    }

    private void startTest() throws InterruptedException, IOException, ClassNotFoundException {
        Path outputPath0 = new Path(appConfig.get("output") + "");
        this.outputPath = outputPath0;
        forestBuilder.startTest(testDataSetPath);
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new PayForestModelJob(), args);
    }
}
