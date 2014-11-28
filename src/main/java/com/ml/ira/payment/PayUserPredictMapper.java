package com.ml.ira.payment;

import com.ml.ira.AppConfig;
import com.ml.ira.Constants;
import com.ml.ira.Counters;
import com.ml.ira.algos.LogisticModelParameters;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.classifier.sgd.CsvRecordFactory;
import org.apache.mahout.classifier.sgd.OnlineLogisticRegression;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.Vector;

import java.io.IOException;
import java.util.List;

/**
 * Created by yaming_deng on 14-4-17.
 */
public class PayUserPredictMapper extends Mapper<LongWritable, Text, VarIntWritable, PredictValueWritable> {

    private AppConfig appConfig = null;
    private PayMatrix payMatrix = null;

    private LogisticModelParameters lmp;
    private OnlineLogisticRegression lr;
    private CsvRecordFactory csv;
    private List<Integer> predictors = null;

    public PayUserPredictMapper() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        appConfig = new AppConfig(Constants.PAYMENT_CONF);
        payMatrix = new PayMatrix(appConfig);
        predictors = payMatrix.getPredictors();

        String str = appConfig.get("output");
        Path modelPath = new Path(str + "/model.bin");

        lmp = LogisticModelParameters.loadFrom(modelPath);
        csv = lmp.getCsvRecordFactory();
        lr = lmp.createRegression();

        csv.firstLine(lmp.getFieldNames());
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tmp = value.toString().split("\t");
        int iszat = Integer.parseInt(tmp[payMatrix.f_iszat]);
        if (iszat == 1){
            return;
        }

        //TODO:过滤预测过的人

        float[] user = payMatrix.mapUser(tmp, this.predictors);
        if (user == null){
            return;
        }
        String line = StringUtils.join(user, ",");
        Vector v = new SequentialAccessSparseVector(lmp.getNumFeatures());
        int target = csv.processLine(line, v);
        double score = lr.classifyScalar(v);
        double like = lr.logLikelihood(target, v);

        int userId = Integer.parseInt(tmp[0]);
        int cityId = Integer.parseInt(tmp[payMatrix.f_cityid]);
        int gender = Integer.parseInt(tmp[payMatrix.f_sex]);

        PredictValueWritable outValue = new PredictValueWritable(userId, cityId, iszat, score, target, gender, like);
        context.write(new VarIntWritable(target), outValue);

        if (gender == 1){
            context.getCounter(Counters.PREDICT_PAY).increment(1);
        }else{
            context.getCounter(Counters.PREDICT_NOPAY).increment(1);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
