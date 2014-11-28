package MatchMaker;

import com.ml.ira.AppConfig;
import com.ml.ira.Constants;
import com.ml.ira.match.DForestBuildJob;
import junit.framework.TestCase;
import org.apache.mahout.classifier.df.data.DescriptorException;

import java.io.IOException;
import java.util.List;

/**
 * Created by yaming_deng on 14-4-16.
 */
public class DFBuilderTest extends TestCase {

    AppConfig appConfig = null;
    String[] user = null;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        appConfig = new AppConfig(Constants.MATCH_CONF);
        String str = "10211769\t10115004\t-1\t1\t6\t3\t1\t1\t5\t3\t5\t2\t160\t4\t0\t20\t2\t37\t2\t5\t1\t3\t0\t6\t6\t1\t0";
        user = str.split("\t");
    }

    public void testBuilder() throws ClassNotFoundException, DescriptorException, InterruptedException, IOException {
        DForestBuildJob buildJob = new DForestBuildJob();
        buildJob.startBuilder();
    }

    private int[] fillVector(int[] original, List<Integer> fields){
        int[] ret = new int[fields.size()];
        for(int i=0; i<fields.size(); i++){
            ret[i] = original[fields.get(i)-1];
        }
        return ret;
    }
}
