package com.ml.ira;

/**
 * Created by yaming_deng on 14-4-24.
 */
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;

public class MultiLinesInputFormat extends NLineInputFormat {

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(
            InputSplit split, TaskAttemptContext context) {
        return getRecordReader(context);
    }

    public NLinesRecordReader getRecordReader(TaskAttemptContext context) {
        NLinesRecordReader reader = new NLinesRecordReader();
        String number = context.getConfiguration().get(NLineInputFormat.LINES_PER_MAP);
        NLinesRecordReader.setNumLinesPerMap(Integer.valueOf(number));
        return reader;
    }

}
