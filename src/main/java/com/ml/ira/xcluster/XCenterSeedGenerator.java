package com.ml.ira.xcluster;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.iterator.FileLineIterable;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirValueIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * Created by yaming_deng on 14-5-14.
 */
public class XCenterSeedGenerator {

    private static final Logger log = LoggerFactory.getLogger(XCenterSeedGenerator.class);

    public static Path buildRandom(Configuration conf,
                                   Path input,
                                   Path output,
                                   int k,
                                   DistanceMeasure measure,
                                   Long seed) throws IOException {

        Preconditions.checkArgument(k > 0, "Must be: k > 0, but k = " + k);
        // delete the output directory
        FileSystem fs = FileSystem.get(output.toUri(), conf);
        HadoopUtil.delete(conf, output);
        Path outFile = new Path(output, "part-randomSeed");
        boolean newFile = fs.createNewFile(outFile);
        if (newFile) {
            Path inputPathPattern;

            if (fs.getFileStatus(input).isDir()) {
                inputPathPattern = new Path(input, "*");
            } else {
                inputPathPattern = input;
            }

            FileStatus[] inputFiles = fs.globStatus(inputPathPattern, PathFilters.logsCRCFilter());
            SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, outFile, NullWritable.class, XClusterWritable.class);

            Random random = (seed != null) ? RandomUtils.getRandom(seed) : RandomUtils.getRandom();

            List<XClusterWritable> chosenClusters = Lists.newArrayListWithCapacity(k);
            int nextClusterId = 0;
            int index = 0;
            for (FileStatus fileStatus : inputFiles) {
                if (fileStatus.isDir()) {
                    continue;
                }
                /*
                for (Pair<VarIntWritable, Text> record
                        : new SequenceFileIterable<VarIntWritable, Text>(fileStatus.getPath(), true, conf)) {
                    VarIntWritable key = record.getFirst();
                    Text value = record.getSecond();
                    XClusterWritable newCluster = new XClusterWritable(nextClusterId++, parseText(value.toString().split("\t")));
                    int currentSize = chosenClusters.size();
                    if (currentSize < k) {
                        chosenTexts.add(key);
                        chosenClusters.add(newCluster);
                    } else {
                        int j = random.nextInt(index);
                        if (j < k) {
                            chosenTexts.set(j, key);
                            chosenClusters.set(j, newCluster);
                        }
                    }
                    index++;
                }*/
                InputStream is = fs.open(fileStatus.getPath());
                for (String record
                        : new FileLineIterable(is)) {
                    XClusterWritable newCluster = new XClusterWritable(nextClusterId++, parseText(record.split("\t")));
                    int currentSize = chosenClusters.size();
                    if (currentSize < k) {
                        chosenClusters.add(newCluster);
                    } else {
                        int j = random.nextInt(index);
                        if (j < k) {
                            chosenClusters.set(j, newCluster);
                        }
                    }
                    index++;
                }
                is.close();
            }

            try {
                for (int i = 0; i < chosenClusters.size(); i++) {
                    writer.append(NullWritable.get(), chosenClusters.get(i));
                }
                log.info("Wrote {} xclusters to {}", k, outFile);
            } finally {
                Closeables.close(writer, false);
            }
        }

        return outFile;
    }

    public static List<XClusterWritable> loadCenters(Configuration conf, Path path) throws Exception {
        Set<XClusterWritable> chosenClusters = Sets.newHashSet();
        Iterable<XClusterWritable> iterable = new SequenceFileDirValueIterable<XClusterWritable>(new Path(path,
                "part-*"), PathType.GLOB, conf);
        Iterator<XClusterWritable> iterator = iterable.iterator();
        while (iterator.hasNext()) {
            XClusterWritable item = iterator.next();
            chosenClusters.add(item.clone());
        }
        return Lists.newArrayList(chosenClusters);
    }

    public static void dumpCenter(Configuration conf, Path path)throws Exception{
        List<XClusterWritable> centers = loadCenters(conf, path);
        for (XClusterWritable center : centers){
            System.out.println(center.toString());
        }
        System.out.println("Center Count: " + centers.size());
    }

    public static double[] parseText(String[] val){
        double[] ret = new double[val.length];
        for (int i = 0; i < val.length; i++) {
            ret[i] = Integer.parseInt(val[i]);
        }
        return ret;
    }

}
