package com.ml.ira.match;

import com.ml.ira.AppConfig;
import com.ml.ira.Constants;
import com.ml.ira.JobCreator;
import com.ml.ira.algos.ForestBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.classifier.df.data.DescriptorException;
import org.apache.mahout.common.AbstractJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by yaming_deng on 14-4-14.
 */
public class DForestBuildJob extends AbstractJob {

    private static final Logger log = LoggerFactory.getLogger(DForestBuildJob.class);
    private AppConfig appConfig;
    private Integer nbtrees;

    @Override
    public int run(String[] args) throws Exception {
        appConfig = new AppConfig(Constants.MATCH_CONF);
        JobCreator.wrapConf(appConfig, getConf());

        this.addOption("steps", "steps", "running steps", "1,2,3,4");
        this.addOption("nbtrees", "nt", "total number of trees to build", "0");

        Map<String,List<String>> parsedArgs = parseArguments(args);
        if (parsedArgs == null) {
            return -1;
        }

        String temp = this.getOption("steps");
        nbtrees = this.getInt("nbtrees");
        List<String> steps = Arrays.asList(temp.split(","));

        //1.
        if (steps.contains("1")){
            int runResult = ToolRunner.run(this.getConf(), new ActionTablePrepareJob(), new String[0]);
            if (runResult == -1){
                log.error("ActionTablePrepareJob Failed.");
                return -1;
            }
        }

        //2.
        if (steps.contains("2")){
            int runResult = ToolRunner.run(this.getConf(), new MatchMatrixBuildJob(), new String[0]);
            if (runResult == -1){
                log.error("MatchMatrixBuildJob Failed.");
                return -1;
            }
        }

        //3.
        if (steps.contains("3")) {
            startBuilder();
        }

        //4.
        if (steps.contains("4")) {
            int runResult = ToolRunner.run(this.getConf(), new DForestTestJob(), new String[0]);
            if (runResult == -1){
                log.error("DForestTestJob Failed.");
                return -1;
            }
        }
        return 0;
    }

    public void startBuilder() throws IOException, DescriptorException, ClassNotFoundException, InterruptedException {
        Path outputPath0 = new Path(appConfig.get("output") + "");
        Map treeConf = appConfig.get("forest");
        int treeNb = Integer.parseInt(treeConf.get("nbTrees").toString());
        if (nbtrees>0){
            treeNb = nbtrees;
        }
        boolean partial = Boolean.parseBoolean(treeConf.get("partial")+"");
        Path dataPath = new Path(outputPath0, MatchMatrixBuildJob.MATCH_MATRIX);
        ForestBuilder forestBuilder = new ForestBuilder(this.getConf(), outputPath0);
        forestBuilder.setPartial(partial);
        forestBuilder.setNbTrees(treeNb);
        forestBuilder.setComplemented(true);
        forestBuilder.setMinSplitNum(2);
        forestBuilder.setMinVarianceProportion(0.001);
        forestBuilder.setSeed(null);
        forestBuilder.buildForest(new Path(dataPath.toString()+"/part-r-00000"));
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new DForestBuildJob(), args);
    }
}
