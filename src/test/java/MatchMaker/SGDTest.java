package MatchMaker;

import com.ml.ira.algos.TrainLogistic;
import junit.framework.TestCase;

/**
 * Created by yaming_deng on 14-4-18.
 */
public class SGDTest extends TestCase {

    public void testTrainArgs() throws Exception {
        String[] args = new String[]{
                "--input", "/a/b/c",
                "--output", "/a/b/c/d",
                "--target", "pay",
                "--categories", "2",
                "--predictors", "a b c d e",
                "--types", "w n n w n",
                "--features", "50",
                "--passes", "100",
                "--rate", "0.0001",
                "--fdnames", "a,b,c,d,e"
        };
        try {
            TrainLogistic.main(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
