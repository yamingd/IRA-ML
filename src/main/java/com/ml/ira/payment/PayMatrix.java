package com.ml.ira.payment;

import com.ml.ira.AppConfig;
import org.apache.commons.lang3.time.DateUtils;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by yaming_deng on 14-4-18.
 */
public class PayMatrix {

    /*public static final int[] trainPositiveMods = new int[]{0, 1, 7, 6, 8, 3, 9};
    public static final int[] trainNegativeMods = new int[]{7};
    public static final int[] testNegativeMods = new int[]{5};*/

    public static final int[] trainPositiveMods = new int[]{0, 2, 4, 6, 8, 3, 9};
    public static final int[] trainNegativeMods = new int[]{7};
    public static final int[] testNegativeMods = new int[]{5};

    public static final String NULL = "null";
    private AppConfig appConfig = null;

    public int f_iszat = -1;
    public int f_birthday = -1;
    public int f_marriage = -1;
    public int f_cityid = 0;
    public int f_sex = 0;
    public int f_regtime = -1;

    public PayMatrix(AppConfig appConfig) {
        this.appConfig = appConfig;
        f_iszat = appConfig.getFieldIndex("iszat");
        f_birthday = appConfig.getFieldIndex("birthday");
        f_marriage = appConfig.getFieldIndex("marriage");
        f_cityid = appConfig.getFieldIndex("workcity");
        f_sex = appConfig.getFieldIndex("sex");
        f_regtime = appConfig.getFieldIndex("regtime");
    }

    public List<Integer> getPredictors(){
        List<Integer> predictors = new ArrayList<Integer>();
        Map<String, Integer> fidx = appConfig.getFieldIndex();
        List<Integer> ignores = appConfig.getIgnoreFields();
        String str = appConfig.get("f_predictors");
        String[] temp = str.split(",");
        for(String key : temp){
            key = key.trim().toLowerCase();
            Integer e = fidx.get(key);
            if (ignores!=null && ignores.contains(e)){
                continue;
            }
            predictors.add(e);
        }
        String target = appConfig.get("f_target");
        int pos = fidx.get(target);
        predictors.add(0, pos);
        return predictors;
    }

    public Integer getAges(String val){
        if (!val.contains("-")){
            return Integer.parseInt(val);
        }
        try {
            Date date = DateUtils.parseDate(val, "yyyy-MM-dd");
            int ages = new Date().getYear() - date.getYear();
            return ages;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public float[] mapUser(String[] val, List<Integer> fields){
        //System.out.println(fields);
        if (val.length <= f_birthday){
            System.err.println("Error Line: " + val);
            return null;
        }
        int m = Integer.parseInt(val[f_marriage]);
        if (m<0 || m>10){
            return null;
        }
        int ages = getAges(val[f_birthday]);
        if (ages <= 0 || ages >= 100){
            return null;
        }
        val[f_birthday] = ages+"";
        float[] user = new float[fields.size()]; //排除memberId
        //标准化数据
        int k = 0;
        for(int i=0; i<fields.size(); i++){
            //System.out.println(fields.get(i));
            int attrVal;
            if (val[fields.get(i)]==null || val[fields.get(i)].equalsIgnoreCase(NULL)){
                attrVal = 0;
            }else{
                attrVal = Integer.parseInt(val[fields.get(i)]);
            }
            user[k] = attrVal;
            k++;
        }
        return user;
    }

    /*public float[] mapUser(String[] val){
        //System.out.println(fields);
        if (val.length <= f_birthday){
            System.err.println("Error Line: " + val);
            return null;
        }
        int m = Integer.parseInt(val[f_marriage]);
        if (m<0 || m>10){
            return null;
        }
        int ages = getAges(val[f_birthday]);
        if (ages <= 0 || ages >= 100){
            return null;
        }
        float[] user = new float[17]; //排除memberId
        //标准化数据
        // 目标字段
        user[0] = asInt(val, "iszat");
        // 分类字段
        user[1] = asInt(val, "sex");
        user[2] = asInt(val, "salary");
        user[3] = asInt(val, "house");
        user[4] = asInt(val, "vehicle");
        user[5] = asInt(val, "marriage");
        // 数值字段
        user[6] = 1.0f * ages / 100; // ages
        // regtime, logincount,
        ages = this.getAges(val[f_regtime]);
        user[7] = ages > 0 ? 1.0f * asInt(val, "logincount") / ages : 0; // logins
        // telcount,telAvgTime,
        user[8] = ratio2(val, "telcount", "telAvgTime"); //tels
        // microPayCount,microPayAmount,
        user[9] = ratio2(val, "microPayAmount", "microPayCount"); //microPays
        // praiseCount, viewCount,
        user[10] = ratio(val, "praiseCount", "viewCount"); //pv0
        // praiseCount2, viewCount2,
        user[11] = ratio(val, "praiseCount2", "viewCount2"); //pv1
        // sendLeerCount,sendAskCount,sendGiftCount,followCount,hnServiceCount,
        user[12] = ratio(val, "sendLeerCount", "receiveLeerCount"); //leer
        // receiveLeerCount,receiveAskCount,receiveGiftCount,followCount2,hnServiceCount2
        user[13] = ratio(val, "sendAskCount", "receiveAskCount"); // ask
        user[14] = ratio(val, "sendGiftCount", "receiveGiftCount"); //gift
        user[15] = ratio(val, "hnServiceCount", "hnServiceCount2"); //hnService
        user[16] = ratio(val, "followCount", "followCount2"); //follow
        return user;
    }*/

    //public static final String matrixFields = "iszat,sex,salary,house,vehicel,marriage,age,login,tel,micropay,pv0,pv1,leer,ask,gift,hnservice,follow";
    //public static final int[] cateFields = new int[]{1,5}; //这个范围外全是数值

    private int asInt(String[] val, String name){
        int idx = appConfig.getFieldIndex(name);
        return Integer.parseInt(val[idx]);
    }

    private float ratio(String[] val, String name0, String name1){
        int num0 = asInt(val, name0);
        int num1 = asInt(val, name1);
        return num0 + num1 > 0 ? 1.0f * num0 / (num0 + num1) : 0;
    }

    private float ratio2(String[] val, String name0, String name1){
        int num0 = asInt(val, name0);
        int num1 = asInt(val, name1);
        return num1 > 0 ? 1.0f * num0 / num1 : 0;
    }
}
