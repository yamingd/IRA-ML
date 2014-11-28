package com.ml.ira.payment;

import com.google.common.io.Closeables;
import com.ml.ira.*;
import com.ml.ira.algos.RunLogistic;
import com.ml.ira.algos.TrainLogistic;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
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
import java.util.*;

/**
 * Created by yaming_deng on 14-4-17.
 */
public class PayLogisticModelJob extends AbstractJob {

    private static final Logger log = LoggerFactory.getLogger(PayLogisticModelJob.class);

    public static final String NUM_USERS = "numUsers.bin";

    private AppConfig appConfig;
    private PayMatrix payMatrix;

    private Path uinfoPath;
    private Path dataPath;

    private Path trainDataSetPath;
    private Path trainDataSetLocalPath;
    private Path trainModelPath;
    private Path testDataSetLocalPath;
    private Path testDataSetPath;
    private String trainDSFields;
    private String trainPredictors;
    private String trainPredictorTypes;
    private int argRound;

    public PayLogisticModelJob() throws IOException {
        this.appConfig = new AppConfig(Constants.PAYMENT_CONF);
        this.payMatrix = new PayMatrix(appConfig);
    }

    private void deleteIfExists(Path path) throws IOException {
        FileSystem ofs = path.getFileSystem(getConf());
        if (ofs.exists(path)) {
            ofs.delete(path, true);
        }
        ofs.close();
    }

    private void exportTrainDS() throws IOException {
        FileSystem ofs = this.trainDataSetPath.getFileSystem(getConf());
        if (ofs.exists(trainDataSetPath)) {
            String str = appConfig.get("local") + "/train";
            long timeMillis = System.currentTimeMillis();
            trainDataSetLocalPath = new Path(str+"_"+ timeMillis + ".csv");
            ofs.copyToLocalFile(false, trainDataSetPath, trainDataSetLocalPath, true);
        }
    }

    private void exportTestDS() throws IOException {
        FileSystem ofs = this.testDataSetPath.getFileSystem(getConf());
        if (ofs.exists(this.testDataSetPath)) {
            String str = appConfig.get("local") + "/test";
            long timeMillis = System.currentTimeMillis();
            testDataSetLocalPath = new Path(str+"_"+ timeMillis + ".csv");
            ofs.copyToLocalFile(false, this.testDataSetPath, this.testDataSetLocalPath, true);
        }
    }

    /**
     * 用于训练模型的字段
     */
    public void setTrainPredictorsAndFields(){
        if (trainPredictors != null){
            return;
        }
        Map<String, Integer> fidx = this.appConfig.getFieldIndex();
        List<Integer> ignores = this.appConfig.getIgnoreFields();
        List<Integer> cates = this.appConfig.getCategoricalFields();
        String str = this.appConfig.get("f_predictors");
        String[] temp = str.split(",");
        List<String> fnds = new ArrayList<String>();
        List<String> types = new ArrayList<String>();
        for(int i=0; i<temp.length; i++){
            String key = temp[i].trim().toLowerCase();
            Integer idx = fidx.get(key);
            if (ignores.contains(idx)){
                continue;
            }
            fnds.add(temp[i]);
            if (cates.contains(idx)){
                types.add("w");
            }else{
                types.add("n");
            }
        }
        trainPredictors = StringUtils.join(fnds, ",");
        trainPredictorTypes = StringUtils.join(types, ",");
        String target = this.appConfig.get("f_target");
        fnds.add(0, target);
        trainDSFields = StringUtils.join(fnds, ","); //第1列为是否购珍爱通标记
    }

    @Override
    public int run(String[] args) throws Exception {

        this.addOption("round", "r", "total round of model training", "10");
        this.addOption("steps", "steps", "steps to run", "1,2,3,4");

        Map<String,List<String>> parsedArgs = parseArguments(args);
        if (parsedArgs == null) {
            return -1;
        }

        argRound = this.getInt("round", 10);
        String[] str = this.getOption("steps").split(",");
        List<String> steps = Arrays.asList(str);

        JobCreator.wrapConf(appConfig, getConf());
        this.uinfoPath = new Path(appConfig.getDataFile("pay"));
        //1. 统计属性值的分布
        if (steps.contains("1") && !step1())
            return -1;
        //2. 标准化属性的值
        String tmp = appConfig.get("output");
        this.outputPath = new Path(tmp);
        this.dataPath = this.getOutputPath("data");
        if (steps.contains("2") && !step2())
            return -1;
        //3. 导出训练数据集
        this.trainDataSetPath = new Path(dataPath, "train-r-00000");
        this.testDataSetPath = new Path(dataPath, "test-r-00000");
        if (steps.contains("3")){
            this.exportTrainDS();
            this.setTrainPredictorsAndFields();
            //4. 训练回归模型
            if(!this.trainLogModel()){
                return -1;
            }
            //保存模型参数到Hadoop
            this.saveLogModel();
        }
        //5. 测试回归模式
        if(steps.contains("4")){
            tmp = appConfig.get("output");
            trainModelPath = new Path(tmp + "/model.bin");
            this.exportTestDS();
            this.setTrainPredictorsAndFields();
            if (!this.testLogModel())
                return -1;
        }
        return 0;
    }

    private boolean step1() throws IOException, InterruptedException, ClassNotFoundException {
        this.outputPath = uinfoPath.getParent();
        Path propPath = this.getOutputPath("pay_scale");
        this.deleteIfExists(propPath);

        Job propJob = JobCreator.prepareJob(
                "1.PayRecordScaleMapper",
                uinfoPath,
                propPath,
                NLineInputFormat.class,
                PayRecordScaleMapper.class,
                PayRecordScaleWritable.class,
                VarIntWritable.class,
                PayRecordScaleReducer.class,
                PayRecordScaleWritable.class,
                VarIntWritable.class,
                TextOutputFormat.class,
                this.getConf());

        JobConf conf = (JobConf)propJob.getConfiguration();
        conf.set(NLineInputFormat.LINES_PER_MAP, Constants.USER_LINES_PER_MAP);

        long ts1 = System.currentTimeMillis();
        boolean succeeded = propJob.waitForCompletion(true);
        if (!succeeded) {
            return false;
        }
        long ts = System.currentTimeMillis() - ts1;
        System.out.println("Job PayRecordScaleMapper. duration = " + ts);

        int totalOfUsers = (int) propJob.getCounters().findCounter(Counters.USERS).getValue();
        HadoopUtil.writeInt(totalOfUsers, getOutputPath(NUM_USERS), getConf());
        return true;
    }

    private boolean step2() throws IOException, ClassNotFoundException, InterruptedException {
        this.deleteIfExists(this.dataPath);

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

        int total00 = (int) filterJob.getCounters().findCounter(Counters.PAY_TEST_0).getValue();
        int total01 = (int) filterJob.getCounters().findCounter(Counters.PAY_TEST_1).getValue();
        HadoopUtil.writeInt(total01 + total00, getOutputPath("numTest.bin"), getConf());

        System.out.println("PAY_TRAIN: " + (total10 + total11) + "/" + total10 + "/" + total11);
        System.out.println("PAY_TEST: " + (total00 + total01) + "/" + total00 + "/" + total01);

        return true;
    }

    private boolean trainLogModel() throws Exception {
        String str = appConfig.get("output");
        trainModelPath = new Path(str + "/model.bin");

        //--input donut.csv --output donut.model --target color --categories 2 --predictors x y a b c --types numeric --features 50 --passes 100 --rate 50
        System.out.println("trainDataSetLocalPath: " + trainDataSetLocalPath);
        System.out.println("trainModelPath: " + trainModelPath);
        System.out.println("trainPredictors: " + trainPredictors);
        System.out.println("trainPredictorTypes: " + trainPredictorTypes);
        System.out.println("trainDSFields: " + trainDSFields);

        Map map = this.appConfig.get("logistic");
        String target = this.appConfig.get("f_target");

        String[] args = new String[]{
                "--input", this.trainDataSetLocalPath.toString(),
                "--output", this.trainModelPath.toString(),
                "--target", target,
                "--categories", "2",
                "--predictors", this.trainPredictors,
                "--types", this.trainPredictorTypes,
                "--features", map.get("features")+"",
                "--passes", argRound+"",
                "--rate", map.get("rate")+"",
                "--fdnames", this.trainDSFields
        };
        try {
            TrainLogistic.main(args);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    private void saveLogModel() throws IOException {
        String str = appConfig.get("output");
        this.outputPath = new Path(str);

        Map map = new HashMap();
        map.put("fields", this.trainDSFields);
        map.put("predictors", this.trainPredictors);
        map.put("types", this.trainPredictorTypes);

        String json = JacksonUtils.objectToJson(map);
        this.saveString(new Path(str, "/model.json"), json);
    }

    private void saveString(Path path, String value) throws IOException {
        FileSystem fs = FileSystem.get(path.toUri(), this.getConf());
        FSDataOutputStream out = fs.create(path);
        try {
            out.writeBytes(value);
        } finally {
            Closeables.close(out, false);
        }
    }

    private boolean testLogModel() throws Exception {
        //runlogistic --input donut-test.csv --model donut.model --scores --auc --confusion

        String[] args = new String[]{
                "--input", this.testDataSetLocalPath.toString(),
                "--model", this.trainModelPath.toString(),
                "--scores",
                "--auc",
                "--confusion",
                "--fdnames", "internal"
        };
        try {
            RunLogistic.main(args);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new PayLogisticModelJob(), args);
    }
}
