package com.ml.ira.algos;

import com.google.common.base.Preconditions;
import com.ml.ira.JobCreator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.classifier.df.DFUtils;
import org.apache.mahout.classifier.df.DecisionForest;
import org.apache.mahout.classifier.df.builder.TreeBuilder;
import org.apache.mahout.classifier.df.mapreduce.Builder;
import org.apache.mahout.classifier.df.mapreduce.MapredOutput;
import org.apache.mahout.classifier.df.mapreduce.partial.PartialBuilder;
import org.apache.mahout.classifier.df.mapreduce.partial.Step1Mapper;
import org.apache.mahout.classifier.df.mapreduce.partial.TreeID;
import org.apache.mahout.classifier.df.node.Node;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by yaming_deng on 14-4-29.
 */
public class ForestPartialBuilder extends PartialBuilder {

    private Path outputPath;

    public ForestPartialBuilder(TreeBuilder treeBuilder, Path dataPath, Path datasetPath, Long seed, Path outputPath) {
        super(treeBuilder, dataPath, datasetPath, seed);
        this.outputPath = outputPath;
    }

    public ForestPartialBuilder(TreeBuilder treeBuilder, Path dataPath, Path datasetPath, Long seed, Configuration conf, Path outputPath) {
        super(treeBuilder, dataPath, datasetPath, seed, conf);
        this.outputPath = outputPath;
    }

    @Override
    protected void configureJob(Job job) throws IOException {

        Configuration conf = job.getConfiguration();

        job.setJarByClass(ForestPartialBuilder.class);

        FileInputFormat.setInputPaths(job, getDataPath());
        FileOutputFormat.setOutputPath(job, getOutputPath(conf));

        job.setOutputKeyClass(TreeID.class);
        job.setOutputValueClass(MapredOutput.class);

        job.setMapperClass(Step1Mapper.class);
        job.setNumReduceTasks(0); // no reducers

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        JobCreator.setIOSort(job);
    }

    @Override
    protected Path getOutputPath(Configuration conf) throws IOException {
        return outputPath;
    }

    @Override
    protected DecisionForest parseOutput(Job job) throws IOException {
        Configuration conf = job.getConfiguration();

        int numTrees = Builder.getNbTrees(conf);

        Path outputPath = getOutputPath(conf);

        TreeID[] keys = new TreeID[numTrees];
        Node[] trees = new Node[numTrees];

        processOutput(job, outputPath, keys, trees);

        return new DecisionForest(Arrays.asList(trees));
    }

    /**
     * Processes the output from the output path.<br>
     *
     * @param outputPath
     *          directory that contains the output of the job
     * @param keys
     *          can be null
     * @param trees
     *          can be null
     * @throws java.io.IOException
     */
    protected static void processOutput(JobContext job,
                                        Path outputPath,
                                        TreeID[] keys,
                                        Node[] trees) throws IOException {
        Preconditions.checkArgument(keys == null && trees == null || keys != null && trees != null,
                "if keys is null, trees should also be null");
        Preconditions.checkArgument(keys == null || keys.length == trees.length, "keys.length != trees.length");

        Configuration conf = job.getConfiguration();

        FileSystem fs = outputPath.getFileSystem(conf);

        Path[] outfiles = DFUtils.listOutputFiles(fs, outputPath);

        // read all the outputs
        int index = 0;
        for (Path path : outfiles) {
            for (Pair<TreeID,MapredOutput> record : new SequenceFileIterable<TreeID, MapredOutput>(path, conf)) {
                TreeID key = record.getFirst();
                MapredOutput value = record.getSecond();
                if (keys != null) {
                    keys[index] = key;
                }
                if (trees != null) {
                    trees[index] = value.getTree();
                }
                index++;
            }
        }

        // make sure we got all the keys/values
        if (keys != null && index != keys.length) {
            throw new IllegalStateException("Some key/values are missing from the output");
        }
    }
}
