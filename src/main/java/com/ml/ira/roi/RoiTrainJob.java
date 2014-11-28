package com.ml.ira.roi;

import com.ml.ira.AppConfig;
import com.ml.ira.Constants;
import com.ml.ira.JobCreator;
import com.ml.ira.Paths;
import com.ml.ira.algos.RunLogistic;
import com.ml.ira.algos.TrainLogistic;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by yaming_deng on 14-4-25.
 */
public class RoiTrainJob extends AbstractJob {

    private static final Logger log = LoggerFactory.getLogger(RoiTrainJob.class);

    private AppConfig appConfig;
    private List<String> steps;
    private String group;
    private String modelName;

    private Path dataPath;
    private Path modelPath;
    private Path testPath;

    private Path trainDataPath;
    private Path testDataPath;

    private String datasetFields;
    private String predictors;
    private String predictorTypes;

    public RoiTrainJob() throws IOException {
        this.appConfig = new AppConfig("roi");
    }

    @Override
    public int run(String[] args) throws Exception {
        JobCreator.wrapConf(appConfig, getConf());

        this.addOption("steps", "steps", "running steps", "1,2,3,4");
        this.addOption("group", "g", "group field index", "1,2,3,4");
        this.addOption("model", "m", "model's name", "model");
        this.addOption("round", "r", "total round of training", "10");

        Map<String,List<String>> parsedArgs = parseArguments(args);
        if (parsedArgs == null) {
            return -1;
        }

        String temp = this.getOption("steps");
        steps = Arrays.asList(temp.split(","));
        group = this.getOption("group");
        modelName = this.getOption("model");

        this.outputPath = new Path(appConfig.getString("output"));
        this.inputPath = new Path(appConfig.getDataFile("roi"));
        this.dataPath = this.getOutputPath("data");
        this.modelPath = this.getOutputPath(modelName);
        this.testPath = this.getOutputPath(modelName+"-test");

        //1. group data by group option
        if (steps.contains("1") && !prepareData()){
            return -1;
        }
        this.trainDataPath = new Path(this.dataPath + "/train-r-00000");
        this.testDataPath = new Path(this.dataPath + "/test-r-00000");
        //2. train model
        this.setTrainPredictorsAndFields();
        if (steps.contains("2") && !trainModel()){
            return -1;
        }
        //3. test model
        if (steps.contains("3") && !testModel()){
            return -1;
        }
        //4. predict

        //5. find strongest source

        return 0;
    }

    private boolean prepareData() throws IOException, ClassNotFoundException, InterruptedException {
        Paths.deleteIfExists(this.dataPath, this.getConf());

        Job filterJob = new Job(getConf(), "Roi-DataSplit");
        filterJob.getConfiguration().setBoolean(Constants.MAPREDUCE_MAP_OUTPUT_COMPRESS, true);
        filterJob.getConfiguration().set(Constants.MAPREDUCE_OUTPUTDIR, dataPath.toString());
        filterJob.getConfiguration().set(Constants.MAPREDUCE_INPUTDIR, this.inputPath.toString());
        filterJob.getConfiguration().set(NLineInputFormat.LINES_PER_MAP, Constants.USER_LINES_PER_MAP);

        filterJob.setJarByClass(InvestRecordGroupMapper.class);
        filterJob.setInputFormatClass(NLineInputFormat.class);

        filterJob.setMapperClass(InvestRecordGroupMapper.class);
        filterJob.setMapOutputKeyClass(InvestGroupWritable.class);
        filterJob.setMapOutputValueClass(InvestRecordWritable.class);

        filterJob.setReducerClass(InvestRecordGroupReducer.class);

        //{namedOutput}-(m|r)-{part-number}
        MultipleOutputs.addNamedOutput(filterJob, "train", TextOutputFormat.class, InvestGroupWritable.class, InvestRecordWritable.class);
        MultipleOutputs.addNamedOutput(filterJob, "test", TextOutputFormat.class, InvestGroupWritable.class, InvestRecordWritable.class);

        boolean succeeded = filterJob.waitForCompletion(true);
        if (!succeeded) {
            log.error("Job Roi-DataSplit Failed.");
            return false;
        }

        return succeeded;
    }

    /**
     * 用于训练模型的字段
     */
    public void setTrainPredictorsAndFields(){
        if (predictors != null){
            return;
        }
        List<String> fnds = new ArrayList<String>();
        List<String> types = new ArrayList<String>();

        Map<String, Integer> fidx = this.appConfig.getFieldIndex();
        List<Integer> ignores = this.appConfig.getIgnoreFields();
        List<Integer> cates = this.appConfig.getCategoricalFields();

        String[] temp = this.appConfig.getString("f_predictors").split(",");
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
        predictors = StringUtils.join(fnds, ",");
        predictorTypes = StringUtils.join(types, ",");

        temp = group.split(",");
        for (int i = 0; i < temp.length; i++) {
            fnds.add(i, "column" + i);
            types.add(i, "w");
        }

        Map target = this.appConfig.get("target");
        fnds.add(target.get("name") + "");

        datasetFields = StringUtils.join(fnds, ","); //最后一列为Label
    }

    private boolean trainModel() throws IOException {
        Path path = new Path(this.modelPath + "/model.bin");
        Paths.deleteIfExists(path, this.getConf());

        //--input donut.csv --output donut.model --target color --categories 2 --predictors x y a b c --types numeric --features 50 --passes 100 --rate 50
        System.out.println("trainDataSetLocalPath: " + trainDataPath);
        System.out.println("trainModelPath: " + path);
        System.out.println("trainPredictors: " + predictors);
        System.out.println("trainPredictorTypes: " + predictorTypes);
        System.out.println("trainDSFields: " + datasetFields);

        Map map = this.appConfig.get("logistic");
        Map target = this.appConfig.get("target");
        int argRound = this.getInt("round");

        String[] args = new String[]{
                "--input", this.trainDataPath.toString(),
                "--output", path.toString(),
                "--target", target.get("name") + "",
                "--categories", map.get("categories")+"",
                "--predictors", predictors,
                "--types", predictorTypes,
                "--features", map.get("features")+"",
                "--passes", argRound+"",
                "--rate", map.get("rate")+"",
                "--fdnames", datasetFields
        };
        try {
            TrainLogistic.main(args);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    private boolean testModel() throws Exception {
        //runlogistic --input donut-test.csv --model donut.model --scores --auc --confusion
        Path path = new Path(this.modelPath + "/model.bin");
        String[] args = new String[]{
                "--input", this.testDataPath.toString(),
                "--model", path.toString(),
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
        ToolRunner.run(new RoiTrainJob(), args);
    }
}
