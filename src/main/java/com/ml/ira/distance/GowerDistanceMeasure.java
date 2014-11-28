package com.ml.ira.distance;

import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.common.parameters.Parameter;
import org.apache.mahout.math.Vector;

import java.util.Collection;
import java.util.Collections;

/**
 * Created by yaming_deng on 14-5-13.
 */
public class GowerDistanceMeasure extends GowerSimilarity {

    /**
     * 0.5 <= d <=1
     *
     * @param v1
     * @param v2
     * @return
     */
    @Override
    public double distance(double[] v1, double[] v2){
        double sum = super.distance(v1, v2);
        if (sum == Integer.MAX_VALUE){
            return Integer.MAX_VALUE;
        }
        sum = 1 / (1 + sum);
        return sum;
    }

    /**
     * 0.5 <= d <=1
     *
     * @param v1
     * @param v2
     * @return
     */
    @Override
    public double distance(Vector v1, Vector v2) {
        double sum = super.distance(v1, v2);
        if (sum == Integer.MAX_VALUE){
            return Integer.MAX_VALUE;
        }
        sum = 1 / (1 + sum);
        return sum;
    }

    @Override
    public double distance(double centroidLengthSquare, Vector centroid, Vector v) {
        // 计算向量和中心的距离, 用于检验覆盖值
        return distance(centroid, v);
    }

    @Override
    public Collection<Parameter<?>> getParameters() {
        return Collections.EMPTY_LIST;
    }

    @Override
    public void createParameters(String prefix, Configuration jobConf) {

    }
}
