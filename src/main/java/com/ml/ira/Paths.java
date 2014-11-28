package com.ml.ira;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by yaming_deng on 14-4-21.
 */
public class Paths {

    public static void deleteIfExists(Path path, Configuration conf) throws IOException {
        FileSystem ofs = path.getFileSystem(conf);
        if (ofs.exists(path)) {
            ofs.delete(path, true);
        }
    }

    public static boolean ifExists(Path path, Configuration conf) throws IOException {
        FileSystem ofs = path.getFileSystem(conf);
        if (ofs.exists(path)) {
            return true;
        }
        return false;
    }
}
