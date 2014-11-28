package com.ml.ira;

import java.util.Arrays;

/**
 * Created by yaming_deng on 14-4-17.
 */
public final class Values {

    public static final String NULL = "null";

    /**
     * 将类别值转为数值
     * @param cateValue
     * @param max
     * @param min
     * @return
     */
    public static int[] encoding(int cateValue, int max, int min){
        int[] vs = new int[max-min+1];
        for(int i=0; i<vs.length; i++){
            vs[i] = 0;
        }
        vs[cateValue-min] = 1;
        return vs;
    }

    public static float xmax(float x, float max){
        if (max == 0 || x== 0){
            return 0;
        }
        return x / max;
    }

    public static float xmean(float x, float mean){
        if (mean == 0 || x==0){
            return 0;
        }
        return x - mean;
    }

    public static double logx(float x){
        return Math.log(x);
    }

    public static int asInt(String val){
        if (val == null || NULL.equalsIgnoreCase(val)){
            return -1;
        }
        return Integer.parseInt(val);
    }

    public static float asFloat(String val){
        if (val == null || NULL.equalsIgnoreCase(val)){
            return -1;
        }
        return Float.parseFloat(val);
    }

    public static void main(String[] args){
        int[] vs = encoding(3, 5, 0);
        System.out.println(Arrays.toString(vs));
    }

}
