package com.ml.ira.algos;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.mahout.classifier.df.mapreduce.inmem.InMemInputFormat;

import java.io.IOException;
import java.util.List;

/**
 * Created by yaming_deng on 14-4-17.
 */
public class ForestInMemInputFormat extends InMemInputFormat {

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        int numSplits = conf.getInt("mapred.map.tasks", -1);

        return getSplits(conf, numSplits);
    }
}
