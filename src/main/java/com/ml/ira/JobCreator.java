package com.ml.ira;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by yaming_deng on 14-4-15.
 */
public final class JobCreator {

    public static final String MAPREDUCE_INPUTDIR = "mapreduce.input.fileinputformat.inputdir";
    public static final String MAPREDUCE_MAP_OUTPUT_COMPRESS = "mapreduce.map.output.compress";
    public static final String MAPREDUCE_OUTPUTDIR = "mapreduce.output.fileoutputformat.outputdir";

    public static Job prepareJob(String jobName,
                                 Path inputPath,
                                 Path outputPath,
                                 Class<? extends InputFormat> inputFormat,
                                 Class<? extends Mapper> mapper,
                                 Class<? extends Writable> mapperKey,
                                 Class<? extends Writable> mapperValue,
                                 Class<? extends Reducer> reducer,
                                 Class<? extends Writable> reducerKey,
                                 Class<? extends Writable> reducerValue,
                                 Class<? extends OutputFormat> outputFormat,
                                 Configuration conf) throws IOException {

        return prepareJob(jobName,
                inputPath, outputPath, inputFormat,
                mapper, mapperKey, mapperValue,
                reducer, reducerKey,reducerValue, outputFormat, conf, true);
    }

    public static Job prepareJob(String jobName,
                                 Path inputPath,
                                 Path outputPath,
                                 Class<? extends InputFormat> inputFormat,
                                 Class<? extends Mapper> mapper,
                                 Class<? extends Writable> mapperKey,
                                 Class<? extends Writable> mapperValue,
                                 Class<? extends Reducer> reducer,
                                 Class<? extends Writable> reducerKey,
                                 Class<? extends Writable> reducerValue,
                                 Class<? extends OutputFormat> outputFormat,
                                 Configuration conf, boolean combiner) throws IOException {

        Job job = new Job(new Configuration(conf));
        Configuration jobConf = job.getConfiguration();

        if (reducer.equals(Reducer.class)) {
            if (mapper.equals(Mapper.class)) {
                throw new IllegalStateException("Can't figure out the user class jar file from mapper/reducer");
            }
            job.setJarByClass(mapper);
        } else {
            job.setJarByClass(reducer);
        }

        job.setInputFormatClass(inputFormat);
        jobConf.set(MAPREDUCE_INPUTDIR, inputPath.toString());

        job.setMapperClass(mapper);
        if (mapperKey != null) {
            job.setMapOutputKeyClass(mapperKey);
        }
        if (mapperValue != null) {
            job.setMapOutputValueClass(mapperValue);
        }

        jobConf.setBoolean(MAPREDUCE_MAP_OUTPUT_COMPRESS, true);

        if (combiner) {
            job.setCombinerClass(reducer);
        }

        job.setReducerClass(reducer);
        job.setOutputKeyClass(reducerKey);
        job.setOutputValueClass(reducerValue);

        job.setOutputFormatClass(outputFormat);
        jobConf.set(MAPREDUCE_OUTPUTDIR, outputPath.toString());

        job.setJobName(jobName);

        return job;
    }

    public static void wrapConf(AppConfig appConfig, Configuration conf){
        Map map = appConfig.get("job");
        if (map!= null){
            Iterator<Object> itor = map.keySet().iterator();
            while (itor.hasNext()){
                String key = itor.next().toString();
                String value = map.get(key).toString();
                conf.set(key, value);
            }
        }
    }

    public static void setIOSort(JobContext job) {
        Configuration conf = job.getConfiguration();
        String factorKey = "mapreduce.task.io.sort.factor";
        conf.setInt(factorKey, 100);
        String javaOpts = conf.get("mapred.map.child.java.opts"); // new arg name
        if (javaOpts == null) {
            javaOpts = conf.get("mapred.child.java.opts"); // old arg name
        }
        int assumedHeapSize = 512;
        if (javaOpts != null) {
            Matcher m = Pattern.compile("-Xmx([0-9]+)([mMgG])").matcher(javaOpts);
            if (m.find()) {
                assumedHeapSize = Integer.parseInt(m.group(1));
                String megabyteOrGigabyte = m.group(2);
                if ("g".equalsIgnoreCase(megabyteOrGigabyte)) {
                    assumedHeapSize *= 1024;
                }
            }
        }
        // Cap this at 1024MB now; see https://issues.apache.org/jira/browse/MAPREDUCE-2308
        conf.setInt(factorKey, Math.min(assumedHeapSize / 2, 1024));
        conf.setInt("mapreduce.task.io.sort.mb", Math.max(assumedHeapSize / 10, 100));
        // For some reason the Merger doesn't report status for a long time; increase
        // timeout when running these jobs
        conf.setInt("mapreduce.task.timeout", 60 * 60 * 1000);

        System.out.println(factorKey+":"+conf.getInt(factorKey, 0));

    }
}
