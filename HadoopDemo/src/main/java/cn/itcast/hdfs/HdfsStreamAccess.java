package cn.itcast.hdfs;


import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.net.URI;

/**
 * 用流的方法来操作hdfs上的文件
 * 可以实现读取指定偏移量范围的数据
 * @author zbf
 *
 */

public class HdfsStreamAccess {
    FileSystem fs = null;
    Configuration conf=null;
    //InputFormat<K, V>;


    @Before
    public void init() throws Exception {
        conf = new Configuration();
        conf.set("dfs.replication", "2");
        // 拿到一个文件系统操作的客户端实例对象
        fs = FileSystem.get(conf);
        // 可以直接传入uri和用户身份
        fs = FileSystem.get(new URI("hdfs://master:9000"), conf, "root");
    }
    @Test
    public void testUpload() throws Exception {
        FSDataOutputStream outputStream = fs.create(new Path("/hahh.test"), true);
        FileInputStream inputStream = new FileInputStream("d:/haha.txt");
        IOUtils.copy(inputStream, outputStream);
    }
}
