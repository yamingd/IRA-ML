package com.ml.ira.standard;

import com.google.common.collect.Maps;
import com.ml.ira.AppConfig;
import com.ml.ira.Values;

import java.util.List;
import java.util.Map;

/**
 * Created by yaming_deng on 14-5-13.
 */
public class ValueRangeChecker {

    private AppConfig appConfig = null;
    private Map<Integer, Integer[]> ranges = Maps.newHashMap();

    public ValueRangeChecker(AppConfig appConfig) {
        this.appConfig = appConfig;
        Map<String, Integer> fields = appConfig.getFieldIndex();
        List rset = this.appConfig.get("ranges");
        if (rset != null){
            for (Object item : rset){
                String[] strs = (item+"").split(",");
                Integer fid = fields.get(strs[0]);
                Integer min = Integer.MIN_VALUE, max = Integer.MAX_VALUE;
                if (!"*".equals(strs[1].trim())){
                    min = Integer.parseInt(strs[1].trim());
                }
                if (!"*".equals(strs[2].trim())){
                    max = Integer.parseInt(strs[2].trim());
                }
                ranges.put(fid, new Integer[]{min, max});
            }
        }
    }

    public boolean valid(String[] vals){
        for(Integer index : ranges.keySet()){
            Integer[] mms = ranges.get(index);
            Integer iv = Values.asInt(vals[index]);
            if (iv<mms[0] || iv>mms[1]){
                return false;
            }
        }
        return true;
    }
}
