package com.ml.ira;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.TypeFactory;
import org.codehaus.jackson.type.TypeReference;

import java.util.ArrayList;
import java.util.List;

public class JacksonUtils {

    /**
     * Object对象转换为JSON格式
     *
     * 例如List对象、JavaBean对象、JavaBean对象数组、Map对象、List Map对象
     *
     * @param object
     *            传入的Object对象
     * @return object的JSON格式字符串
     */
    public static String objectToJson(Object object) {
        ObjectMapper objectMapper = new ObjectMapper();
        String jsonString = "";
        try {

            jsonString = objectMapper.writeValueAsString(object);

        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
        return jsonString;

    }

    /**
     * JSON转换为Object对象
     *
     * @param jsonString
     *            JSON字符串
     * @param c
     *            要转换成的类的类型
     * @return Object对象
     */
    public static <T> T jsonToObject(String jsonString, Class<T> valueType) {
        if (jsonString == null || "".equals(jsonString)) {
            return null;
        } else {
            // Jackson方式将Json转换为对象
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                return objectMapper.readValue(jsonString, valueType);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }
    }

    /**
     * 数组格式JSON串转换为ObjectList对象
     *
     * @param <T>
     * @param jsonString
     *            JSON字符串
     * @param typeReference
     *            TypeReference,例如: new TypeReference< List<Integer>>(){}
     *            System.out.println(jsonToGenericObject("[1,3,4,58,2]",new
     *            TypeReference<ArrayList<Integer>>(){}));
     * @return ObjectList对象
     */
    public static <T> T jsonToGenericObject(String jsonString, TypeReference<T> typeReference) {

        if (jsonString == null || "".equals(jsonString)) {
            return null;
        } else {
            // Jackson方式将Json转换为对象
            ObjectMapper objectMapper = new ObjectMapper();
            try {

                return objectMapper.readValue(jsonString, typeReference);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }
    }

    public static <T> List<T> jsonToList(String jsonString, Class<T> valueType) {
        if (jsonString == null || "".equals(jsonString)) {
            return null;
        } else {
            // Jackson方式将Json转换为对象
            TypeFactory t = TypeFactory.defaultInstance();
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                List<T> list = objectMapper.readValue(jsonString,t.constructCollectionType(ArrayList.class, valueType));
                return list;
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }
    }

}
