package com.ml.ira;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URL;
import java.util.*;

/**
 * Created by yaming_deng on 14-4-14.
 */
public class AppConfig implements Serializable {

    private Map<String, Object> configMap = null;
    private Map<String, Integer> fieldIndex = null;
    private List<Integer> categoricalFields = null;
    private List<Integer> ignoreFields = null;

    public AppConfig(String cfgFile) throws IOException {
        this.load(cfgFile);
        this.resoveValues();
    }

    private void load(String cfgFile) throws IOException {
        InputStream is = null;
        if (cfgFile.startsWith("hdfs://")){
            Path path = new Path(cfgFile);
            FileSystem ofs = path.getFileSystem(new Configuration());
            is = ofs.open(path);
        }else{
            URL resource = Thread.currentThread().getContextClassLoader().getResource(cfgFile + ".yaml");
            is = resource.openStream();
        }
        Yaml yaml = new Yaml();
        Map<String, Object> data = (Map<String, Object>)yaml.load(is);
        this.configMap = data;
        if (is != null){
            is.close();
        }
    }

    private void resoveValues(){
        //1.
        fieldIndex = new HashMap<String, Integer>();
        String temp = this.get("fields");
        String[] fields = temp.split(",");
        for(int i=0;i<fields.length;i++){
            fieldIndex.put(fields[i].trim().toLowerCase(), i);
        }
        //2.
        String str = this.get("f_categories");
        if (str!=null && str.length() > 0){
            fields = str.split(",");
            this.categoricalFields = new ArrayList<Integer>();
            for(String field : fields){
                this.categoricalFields.add(fieldIndex.get(field.trim().toLowerCase()));
            }
        }
        str = this.get("f_ignore");
        if (str!=null && str.length() > 0){
            fields = str.split(",");
            this.ignoreFields = new ArrayList<Integer>();
            for(String field : fields){
                this.ignoreFields.add(fieldIndex.get(field.trim().toLowerCase()));
            }
        }

    }

    public <T> T get(String key){
        return (T) this.configMap.get(key);
    }

    public <T> T get(String key, T defaultValue){
        Object o = this.configMap.get(key);
        if (o == null){
            return defaultValue;
        }
        return (T) o;
    }

    public String getString(String key){
        String str = this.get(key);
        return str;
    }

    public List<String> getList(String key){
        String str = this.get(key);
        return str==null ? null : Arrays.asList(str.split(","));
    }

    public Integer getInt(String key){
        Integer str = this.get(key);
        return str;
    }

    public String getDataFile(String fileName){
        Map map = this.get("datafiles");
        return map.get(fileName)+"";
    }

    public Map<String, Integer> getFieldIndex() {
        return fieldIndex;
    }
    public Integer getFieldIndex(String name){
        return this.fieldIndex.get(name.toLowerCase().trim());
    }

    public List<Integer> getCategoricalFields() {
        return categoricalFields;
    }

    public boolean isCategoryField(String name){
        Integer index = this.getFieldIndex(name);
        return this.categoricalFields.contains(index);
    }

    public List<Integer> getIgnoreFields() {
        return ignoreFields;
    }

    public List<String> getPositiveValues(String action){
        Map map = this.get("positives");
        String[] val = (map.get(action)+"").split(",");
        return Arrays.asList(val);
    }
    public List<String> getNegetivesValues(String action){
        Map map = this.get("negetives");
        String[] val = (map.get(action)+"").split(",");
        return Arrays.asList(val);
    }
    public List<String> getPendingValues(String action){
        Map map = this.get("pending");
        String[] val = (map.get(action)+"").split(",");
        return Arrays.asList(val);
    }
    public List<Integer> getModelFields(String name){
        String[] str = (this.get(name)+"").split(",");
        List<Integer> fis = new ArrayList<Integer>();
        for(int i=0; i<str.length; i++){
            fis.add(this.fieldIndex.get(str[i].trim().toLowerCase()));
        }
        return fis;
    }
}
