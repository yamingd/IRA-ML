package com.ml.ira.match;

import java.util.Arrays;
import java.util.List;

/**
 * Created by yaming_deng on 14-4-21.
 */
public class MatchVector {

    public static final String NULL = "null";

    public static int[] copy(int[] original, final List<Integer> fields){
        try {
            int[] ret = new int[fields.size()];
            for(int i=0; i<fields.size(); i++){
                ret[i] = original[fields.get(i)-1];
            }
            return ret;
        } catch (Exception e) {
            System.err.println("vectorFields: " + fields);
            System.err.println("user: " + Arrays.toString(original));
            e.printStackTrace();
            return null;
        }
    }

    public static int[] copy(String[] original, final List<Integer> fields){
        try {
            int[] ret = new int[fields.size()];
            for(int i=0; i<fields.size(); i++){
                int pos = fields.get(i);
                if (original[pos].equalsIgnoreCase(NULL)){
                    ret[i] = -1;
                }else{
                    ret[i] = Integer.parseInt(original[pos]);
                }
            }
            return ret;
        } catch (Exception e) {
            System.err.println("vectorFields: " + fields);
            System.err.println("user: " + Arrays.toString(original));
            e.printStackTrace();
            return null;
        }
    }
}
