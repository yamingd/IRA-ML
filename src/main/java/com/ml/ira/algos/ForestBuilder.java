package com.ml.ira.algos;

import com.google.common.io.Closeables;
import com.ml.ira.JacksonUtils;
import com.ml.ira.Paths;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.classifier.ClassifierResult;
import org.apache.mahout.classifier.RegressionResultAnalyzer;
import org.apache.mahout.classifier.ResultAnalyzer;
import org.apache.mahout.classifier.df.DFUtils;
import org.apache.mahout.classifier.df.DecisionForest;
import org.apache.mahout.classifier.df.builder.DecisionTreeBuilder;
import org.apache.mahout.classifier.df.data.Dataset;
import org.apache.mahout.classifier.df.mapreduce.Builder;
import org.apache.mahout.common.iterator.FileLineIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Random Forest生成.
 * Created by yaming_deng on 14-4-23.
 */
public class ForestBuilder {

    private static final Logger log = LoggerFactory.getLogger(ForestBuilder.class);

    private Path outputPath;
    private Path datasetPath;
    private Path modelPath;

    private String datasetName = "forest.info";
    private String modelName = "forest.seq";

    private Configuration conf;
    private Integer m; // Number of variables to select at each tree-node
    private boolean complemented; // tree is complemented
    private Integer minSplitNum; // minimum number for split
    private Double minVarianceProportion; // minimum proportion of the total variance for split
    private int nbTrees; // Number of trees to grow
    private Long seed; // Random seed
    private boolean isPartial; // use partial data implementation

    public ForestBuilder(Configuration conf, Path outputPath) {
        this.conf = conf;
        this.outputPath = outputPath;
        this.modelPath = new Path(outputPath, "model");
        this.datasetPath = new Path(outputPath, "dataset");
        this.datasetPath = new Path(datasetPath+"/"+datasetName);
    }

    public void setM(Integer m) {
        this.m = m;
    }

    public void setComplemented(boolean complemented) {
        this.complemented = complemented;
    }

    public void setMinSplitNum(Integer minSplitNum) {
        this.minSplitNum = minSplitNum;
    }

    public void setMinVarianceProportion(Double minVarianceProportion) {
        this.minVarianceProportion = minVarianceProportion;
    }

    public void setNbTrees(int nbTrees) {
        this.nbTrees = nbTrees;
    }

    public void setSeed(Long seed) {
        this.seed = seed;
    }

    public void setPartial(boolean isPartial) {
        this.isPartial = isPartial;
    }

    public Path getDatasetPath() {
        return datasetPath;
    }

    public Path getModelPath() {
        return modelPath;
    }

    public Path getModelFullPath() {
        return new Path(modelPath, modelName);
    }

    public boolean buildDataset(Path infoPath) throws IOException {
        if (!Paths.ifExists(infoPath, conf)){
            throw new IOException("infoPath does not exissts. path=" + infoPath);
        }
        Paths.deleteIfExists(this.datasetPath, conf);

        String json = null;
        FSDataInputStream in = null;
        try {
            FileSystem fs = FileSystem.get(infoPath.toUri(), conf);
            in = fs.open(new Path(infoPath+"/part-r-00000"));
            List<Map> dataset = new ArrayList<Map>();
            for (String line : new FileLineIterable(in)) {
                String[] tmp = line.trim().split("\t");
                int index = Integer.parseInt(tmp[0]);
                Map item = new HashMap();
                item.put("label", tmp[2].equals("1"));
                item.put("type", getTypeName(tmp[1]));
                item.put("values", filterValue(tmp[3]));
                dataset.add(index, item);
            }
            json = JacksonUtils.objectToJson(dataset);
        } finally {
            Closeables.close(in, true);
        }

        if (json == null || json.length() == 0){
            return false;
        }

        System.out.println("storing the dataset description");
        DFUtils.storeString(conf, datasetPath, json);
        System.out.println(json);

        return true;
    }

    private String getTypeName(String val){
        if ("1".equals(val)){
            return "categorical";
        }else{
            return "numerical";
        }
    }

    private String[] filterValue(String val){
        if (val == null || "null".equalsIgnoreCase(val)){
            return null;
        }
        return val.split(",");
    }

    public Path buildForest(Path dataPath) throws IOException, ClassNotFoundException, InterruptedException {
        if (!Paths.ifExists(datasetPath, conf)){
            throw new IOException("datasetPath does not exists. path=" + datasetPath);
        }
        Paths.deleteIfExists(modelPath, conf);

        DecisionTreeBuilder treeBuilder = new DecisionTreeBuilder();
        if (m != null) {
            treeBuilder.setM(m);
        }
        treeBuilder.setComplemented(complemented);
        if (minSplitNum != null) {
            treeBuilder.setMinSplitNum(minSplitNum);
        }
        if (minVarianceProportion != null) {
            treeBuilder.setMinVarianceProportion(minVarianceProportion);
        }

        Builder forestBuilder;

        if (isPartial) {
            log.info("Partial Mapred implementation");
            forestBuilder = new ForestPartialBuilder(treeBuilder, dataPath, datasetPath, seed, conf, modelPath);
        } else {
            log.info("InMem Mapred implementation");
            forestBuilder = new ForestInMemBuilder(treeBuilder, dataPath, datasetPath, seed, conf, modelPath);
        }

        forestBuilder.setOutputDirName(modelPath.getName());

        log.info("Building the forest...");
        long time = System.currentTimeMillis();

        DecisionForest forest = forestBuilder.build(nbTrees);

        time = System.currentTimeMillis() - time;
        log.info("Build Time: {}", DFUtils.elapsedTime(time));
        log.info("Forest num Nodes: {}", forest.nbNodes());
        log.info("Forest mean num Nodes: {}", forest.meanNbNodes());
        log.info("Forest mean max Depth: {}", forest.meanMaxDepth());

        // store the decision forest in the output path
        Path forestPath = new Path(modelPath, modelName);
        log.info("Storing the forest in: {}", forestPath);
        DFUtils.storeWritable(conf, forestPath, forest);
        return forestPath;
    }

    public void startTest(Path testDataPath) throws InterruptedException, IOException, ClassNotFoundException {
        if (!Paths.ifExists(datasetPath, conf)){
            throw new IOException("datasetPath does not exists. path=" + datasetPath);
        }
        if (!Paths.ifExists(modelPath, conf)){
            throw new IOException("modelPath does not exists. path=" + modelPath);
        }
        Path testOutPath = new Path(outputPath, "test-result");
        this.runMRTest(modelPath, testDataPath, datasetPath, testOutPath);
    }

    private void runMRTest(Path modelPath, Path dataPath, Path datasetPath, Path testOutPath) throws ClassNotFoundException, IOException, InterruptedException {
        Paths.deleteIfExists(testOutPath, conf);

        DFClassifier classifier = new DFClassifier(modelPath, dataPath, datasetPath, testOutPath, conf);

        classifier.run();

        double[][] results = classifier.getResults();
        if (results != null) {
            Dataset dataset = Dataset.load(conf, datasetPath);
            if (dataset.isNumerical(dataset.getLabelId())) {
                RegressionResultAnalyzer regressionAnalyzer = new RegressionResultAnalyzer();
                regressionAnalyzer.setInstances(results);
                log.info("{}", regressionAnalyzer);
            } else {
                ResultAnalyzer analyzer = new ResultAnalyzer(Arrays.asList(dataset.labels()), "unknown");
                for (double[] res : results) {
                    analyzer.addInstance(dataset.getLabelString(res[0]),
                            new ClassifierResult(dataset.getLabelString(res[1]), 1.0));
                }
                log.info("{}", analyzer);
            }
        }
    }

}
