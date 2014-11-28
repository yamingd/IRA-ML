package com.ml.ira.cluster;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.ml.ira.propotion.AttrValueWritable;

import java.util.List;
import java.util.Map;

/**
 * http://en.wikipedia.org/wiki/Normalization_(statistics)
 * Created by yaming_deng on 14-5-13.
 */
public class VectorStdNorm {

    private Map<Integer, Map<Integer, Integer>> varMaps = null;
    private Map<Integer, Double> varMeans = null;
    private Map<Integer, Integer> varVCount = null;
    private Map<Integer, Double> varStds = null;
    private List<Integer> cateFields = null;
    private Map<Integer, List<AttrValueWritable>> propotion = null;

    public VectorStdNorm(List<Integer> cates, Map<Integer, List<AttrValueWritable>> propotion) {
        this.cateFields = cates;
        this.propotion = propotion;
        //属性分布
        varMaps = Maps.newHashMap();
        varVCount = Maps.newHashMap();
        for(Integer attrId : propotion.keySet()){
            List<? extends AttrValueWritable> list = propotion.get(attrId);
            Map<Integer, Integer> vals = Maps.newHashMap();
            int c = 0;
            for (AttrValueWritable value : list){
                vals.put(value.getValue(), value.getCount());
                c += value.getCount();
            }
            varMaps.put(attrId, vals);
            varVCount.put(attrId, c);
        }
        // 属性的平均值
        varMeans = Maps.newHashMap();
        for(Integer attrId : propotion.keySet()){
            if (cates.contains(attrId)){
                continue;
            }
            Double mean = 0.0;
            for (AttrValueWritable value : propotion.get(attrId)){
                if (value.getValue() <= 0){
                    continue;
                }
                mean += value.getCount() * Math.log10(value.getValue());
            }
            mean = mean / varVCount.get(attrId);
            varMeans.put(attrId, mean);
        }
        // 属性的标准方差
        varStds = Maps.newHashMap();
        for(Integer attrId : propotion.keySet()){
            if (cates.contains(attrId)){
                continue;
            }
            Double sum = 0.0;
            for (AttrValueWritable value : propotion.get(attrId)){
                if (value.getValue() <= 0){
                    continue;
                }
                sum += value.getCount() * Math.pow(Math.log10(value.getValue()) - varMeans.get(value.getAttr()), 2);
            }
            sum = sum / (varVCount.get(attrId) - 1);
            sum = Math.sqrt(sum);
            varStds.put(attrId, sum);
        }
    }

    public double inRatio(Integer attrId, Integer value){
        int count = varMaps.get(attrId).get(value);
        int total = varVCount.get(attrId);
        return 1.0 * count / total;
    }

    public double inStd(Integer attrId, Integer value){
        if (value<=0){
            return 0;
        }
        double mean = varMeans.get(attrId);
        double std = varStds.get(attrId);
        return (Math.log10(value) - mean) / std;
    }

    public List asBinary(Integer attrId, Integer value){
        List<AttrValueWritable> vals = this.propotion.get(attrId);
        if (vals == null){
            return null;
        }
        List<Integer> bs = Lists.newArrayList();
        for(AttrValueWritable attr : vals){
            if (attr.getValue() == value.intValue()){
                bs.add(1);
            }else{
                bs.add(0);
            }
        }
        return bs;
    }

    public void addBinary(List<Double> cols, Integer attrId, Integer value){
        List<AttrValueWritable> vals = this.propotion.get(attrId);
        if (vals == null){
            cols.add(value * 1.0);
            return;
        }
        for(AttrValueWritable attr : vals){
            if (attr.getValue() == value.intValue()){
                cols.add(1.0);
            }else{
                cols.add(0.0);
            }
        }
    }
}
