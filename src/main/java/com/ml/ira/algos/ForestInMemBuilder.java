package com.ml.ira.algos;

import com.ml.ira.JobCreator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.classifier.df.builder.TreeBuilder;
import org.apache.mahout.classifier.df.mapreduce.MapredOutput;
import org.apache.mahout.classifier.df.mapreduce.inmem.InMemBuilder;
import org.apache.mahout.classifier.df.mapreduce.inmem.InMemMapper;

import java.io.IOException;

/**
 * Created by yaming_deng on 14-4-17.
 */
public class ForestInMemBuilder extends InMemBuilder {

    private Path outputPath;

    public ForestInMemBuilder(TreeBuilder treeBuilder, Path dataPath, Path datasetPath, Long seed, Configuration conf, Path outputPath) {
        super(treeBuilder, dataPath, datasetPath, seed, conf);
        this.outputPath = outputPath;
    }

    public ForestInMemBuilder(TreeBuilder treeBuilder, Path dataPath, Path datasetPath, Path outputPath) {
        super(treeBuilder, dataPath, datasetPath);
        this.outputPath = outputPath;
    }

    @Override
    protected void configureJob(Job job) throws IOException {

        Configuration conf = job.getConfiguration();

        job.setJarByClass(ForestInMemBuilder.class);

        FileOutputFormat.setOutputPath(job, getOutputPath(conf));

        // put the data in the DistributedCache
        DistributedCache.addCacheFile(getDataPath().toUri(), conf);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(MapredOutput.class);

        job.setMapperClass(InMemMapper.class);
        job.setNumReduceTasks(0); // no reducers

        job.setInputFormatClass(ForestInMemInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        JobCreator.setIOSort(job);
    }

    protected Path getOutputPath(Configuration conf) throws IOException {
        return outputPath;
    }

}
