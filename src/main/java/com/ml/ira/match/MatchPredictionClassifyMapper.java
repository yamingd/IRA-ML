package com.ml.ira.match;

import com.google.common.io.Closeables;
import com.ml.ira.AppConfig;
import com.ml.ira.Constants;
import com.ml.ira.Gender;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.mahout.classifier.df.DecisionForest;
import org.apache.mahout.classifier.df.data.Dataset;
import org.apache.mahout.classifier.df.data.Instance;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.common.iterator.FileLineIterable;
import org.apache.mahout.math.DenseVector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 分割用户
 * Created by yaming_deng on 14-4-28.
 */
public class MatchPredictionClassifyMapper extends Mapper<LongWritable, Text, UserMatchKeyWritable, UserMatchSocreWritable> {

    public static final String EXECUTOR_NUM = "ml.mapreduce.input.executor.num";
    public static int usersPerMatch = 1000;

    public enum Counters{
        Errors,
        MatchYes,
        MatchNo,
        IterateNo,
        Duration
    }

    private AppConfig appConfig;
    private DecisionForest forest;
    private final Random rng = RandomUtils.getRandom();
    private Dataset dataset;
    private MultipleOutputs mos;
    private ExecutorService executor;
    private List<int[]> candLists;
    // 线程池数量
    private static int executorSize = 20;

    public MatchPredictionClassifyMapper() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        appConfig = new AppConfig(Constants.MATCH_CONF);
        Configuration conf = context.getConfiguration();
        Path[] files = HadoopUtil.getCachedFiles(conf);
        if (files.length < 2) {
            throw new IOException("not enough paths in the DistributedCache");
        }
        dataset = Dataset.load(conf, files[0]);
        forest = DecisionForest.load(conf, files[1]);
        if (forest == null) {
            throw new InterruptedException("DecisionForest not found!");
        }
        loadCandsProfile(conf, files[2]);
        mos = new MultipleOutputs(context);

        executorSize = context.getConfiguration().getInt(EXECUTOR_NUM, 20);
        executor = Executors.newFixedThreadPool(executorSize);
    }

    private void loadCandsProfile(Configuration conf, Path path) throws IOException {
        int total = conf.getInt("total_cands", 0);
        candLists = new ArrayList<int[]>(total);
        FSDataInputStream in = null;
        try {
            FileSystem fs = FileSystem.get(path.toUri(), conf);
            in = fs.open(path);
            for (String line : new FileLineIterable(in)) {
                candLists.add(newUser(line.split("\t")));
            }
        } finally {
            Closeables.close(in, true);
        }
        System.out.println(candLists.size());
    }

    private int[] newUser(String[] vals) {
        int[] user = new int[vals.length - 1];
        for (int i = 0; i < user.length; i++) {
            user[i] = Integer.parseInt(vals[i + 1]);
        }
        return user;
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.getCounter(Counters.IterateNo).increment(1);
        int[] user = this.newUser(value.toString().split("\t"));
        int batchSize = candLists.size() / executorSize;
        int start = 0, end = 0;
        List<FindMatchTask> tasks = new ArrayList<FindMatchTask>();
        for (int i = 0; i < candLists.size(); i++) {
            start = i;
            end = start + batchSize;
            if (end > candLists.size()){
                end = candLists.size();
            }
            tasks.add(new FindMatchTask(context, user, candLists.subList(start, end)));
            i = end - 1;
        }
        this.executor.invokeAll(tasks);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (!executor.isShutdown()) {
            System.out.println("*****now shut down executor");
            executor.shutdown();
            while (!executor.isTerminated()) {
                System.out.println("wait for shut down executor");
                Thread.sleep(2000);
            }
            System.out.println("***end executor");
        }
        mos.close();
        super.cleanup(context);
    }

    public class FindMatchTask implements Callable<Boolean>{

        private int[] user;
        private List<int[]> cands;
        private Context context;

        public FindMatchTask(Context context, int[] user, List<int[]> cands) {
            this.context = context;
            this.user = user;
            this.cands = cands;
        }

        @Override
        public Boolean call() throws Exception {
            long ts1 = 0;
            for (int[] cand : cands) {
                ts1 = System.currentTimeMillis();
                this.doMatch(cand);
                ts1 = System.currentTimeMillis() - ts1;
                context.getCounter(Counters.Duration).increment(ts1/1000);
            }
            return true;
        }
        private int[] merges(int[] left, int[] right){
            int[] mattrs = new int[left.length-2 + right.length-2];
            int k = 0;
            for (int i = 2; i < left.length; i++) {
                mattrs[k] = left[i];
                k++;
            }
            for (int i = 2; i < right.length; i++) {
                mattrs[k] = right[i];
                k++;
            }
            return mattrs;
        }
        private void doMatch(int[] cand) throws IOException, InterruptedException {
            int gender = cand[1];
            int[] mattrs = null;
            int[] female = null, male = null;
            if (gender == Gender.Female){
                mattrs = merges(this.user, cand);
                female = cand;
                male = this.user;
            }else{
                mattrs = merges(cand, this.user);
                female = this.user;
                male = cand;
            }
            Instance instance = convert(mattrs);
            double prediction = forest.classify(dataset, rng, instance);
            if (prediction > 0.0) {
                context.getCounter(Counters.MatchYes).increment(1);
                UserMatchSocreWritable outval = new UserMatchSocreWritable(new double[]{prediction});

                UserMatchKeyWritable outkey = new UserMatchKeyWritable(new int[]{male[0], female[0]});
                mos.write("male", outkey, outval);
                outkey = new UserMatchKeyWritable(new int[]{female[0], male[0]});
                mos.write("female", outkey, outval);
            }else{
                context.getCounter(Counters.MatchNo).increment(1);
            }
        }

        private Instance convert(int[] mattrs) {
            int nbattrs = dataset.nbAttributes();
            DenseVector vector = new DenseVector(nbattrs);
            int aId = 0;
            for (int attr = 0; attr < mattrs.length; attr++) {
                if (dataset.isNumerical(aId)) {
                    vector.set(aId++, mattrs[attr]);
                } else { // CATEGORICAL
                    vector.set(aId, dataset.valueOf(aId, mattrs[attr] + ""));
                    aId++;
                }
            }
            return new Instance(vector);
        }
    }
}
