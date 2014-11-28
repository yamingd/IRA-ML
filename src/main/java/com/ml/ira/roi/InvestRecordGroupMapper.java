package com.ml.ira.roi;

import com.ml.ira.AppConfig;
import com.ml.ira.Constants;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import javax.xml.soap.Text;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by yaming_deng on 14-4-25.
 */
public class InvestRecordGroupMapper extends Mapper<LongWritable, Text, InvestGroupWritable, InvestRecordWritable> {

    private AppConfig appConfig = null;
    private List<Integer> columns = null;
    private List<Integer> groups = null;

    public InvestRecordGroupMapper() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        appConfig = new AppConfig(Constants.CONF_ROI);
        columns = appConfig.getModelFields("f_predictors");
        Map map = appConfig.get("target");
        int index = appConfig.getFieldIndex(map.get("name")+"");
        columns.add(index);

        String[] groupstr = context.getConfiguration().get("invest.group").split(",");
        groups = new ArrayList<Integer>(groupstr.length);
        for (int i = 0; i < groupstr.length; i++) {
            groups.add(Integer.parseInt(groupstr[i]));
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int mod = (int) key.get() % 10;
        String[] temp = value.toString().split("\t");
        String[] keys = new String[groups.size()];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = temp[groups.get(i)];
        }
        float[] values = new float[columns.size() + 1];
        for (int i = 0; i < values.length; i++) {
            values[i] = Float.parseFloat(temp[i]);
        }
        int type = 1; //train
        if (mod == 0 || mod == 2 || mod == 4){
            type = 0; // test
        }
        InvestGroupWritable outkey = new InvestGroupWritable(type, keys);
        InvestRecordWritable outvalue = new InvestRecordWritable(values);
        context.write(outkey, outvalue);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
