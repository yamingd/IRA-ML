package com.ml.ira.match;

import com.ml.ira.AppConfig;
import com.ml.ira.Constants;
import com.ml.ira.JobCreator;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by yaming_deng on 14-4-24.
 */
public class DForestMatchStarterJob extends AbstractJob {

    private static final Logger log = LoggerFactory.getLogger(DForestMatchStarterJob.class);

    private AppConfig appConfig;

    public DForestMatchStarterJob() throws IOException {
        appConfig = new AppConfig(Constants.MATCH_CONF);
    }

    @Override
    public int run(String[] args) throws Exception {
        JobCreator.wrapConf(appConfig, getConf());

        List<Path> userPaths = new ArrayList<Path>();
        Path root = new Path(this.appConfig.getString("matchus"));
        FileSystem fs = root.getFileSystem(this.getConf());
        if (fs.isDirectory(root)){
            FileStatus[] temp = fs.listStatus(root);
            for (FileStatus fi : temp){
                if (fi.getPath().getName().startsWith("user_")) {
                    userPaths.add(fi.getPath());
                }
            }
        }else{
            userPaths.add(root);
        }
        for (Path path : userPaths){
            ToolRunner.run(new DForestMatchJob(), this.parseArgs(args, path));
        }

        return 0;
    }

    private String[] parseArgs(String[] args, Path path){
        String[] kargs = new String[args.length + 2];
        int i=0;
        for (i=0; i<args.length; i++){
            kargs[i] = args[i];
        }
        kargs[i] = "--path";
        kargs[i+1] = path.toString();
        return kargs;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new DForestMatchStarterJob(), args);
    }
}
