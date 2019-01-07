package cn.itcast.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.Map;

public class HdfsClientDemo {
    FileSystem fs=null;
    Configuration conf=null;

    @Before
    public void init() throws IOException, URISyntaxException, InterruptedException {
        conf=new Configuration();
        conf.set("dfs.replication","2");
        //拿到一个文件系统操作的客户端实例对象
        fs=FileSystem.get(conf);
        //可以直接传入uri和用户身份
        fs=FileSystem.get(new URI("hdfs://master:9000"),conf,"root");
    }
    /**
     * 上传文件
     * @author zbf
     *
     */
    @Test
    public void testUpload() throws IOException {
        fs.copyFromLocalFile(new Path("F:/python.txt"),new Path("/"));
        fs.close();
    }
    /**
     * 下载文件
     * @author zbf
     *
     */
    @Test
    public void testDownload() throws IOException {
        fs.copyToLocalFile(new Path("/python.txt"),new Path("D:/"));
        fs.close();
    }
    /**
     * 打印配置参数
     * @author zbf
     */
    @Test
    public void printConf(){
        Iterator<Map.Entry<String, String>> it = conf.iterator();
        while(it.hasNext()){
            Map.Entry<String, String> next = it.next();
            System.out.println(next.getKey() + ":" + next.getValue());
        }
    }
    /**
     * 创建目录
     * @author zbf
     *
     */
    @Test
    public void testMkdir() throws IOException {
        boolean mkdirs = fs.mkdirs(new Path("/ceshi"));
        System.out.println("目录创建结果："+ mkdirs);
    }
    /**
     * 删除目录
     * @author
     */
    @Test
    public void testDelete() throws Exception {
        // 第二个参数是是否递归输删除目录
        boolean testDelete = fs.delete(new Path("/testmkdir/aaa"), true);
        System.out.println("删除结果：" + testDelete);
    }

    /**
     * 改方法会递归遍历每个目录中下的文件
     * @throws Exception
     */
    @Test
    public void testLs() throws Exception {
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println("blocksize:" + fileStatus.getBlockSize());
            System.out.println("Owner:" + fileStatus.getOwner());
            System.out.println("Replication:" + fileStatus.getReplication());
            System.out.println("Permission:" + fileStatus.getPermission());
            System.out.println("Name:" + fileStatus.getPath().getName());
            System.out.println("Path:" + fileStatus.getPath());
            System.out.println("-------------------");
        }
    }

    @Test
    public void testLs2() throws Exception{
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for(FileStatus file:listStatus){
            System.out.println("name:"+file.getPath().getName());
            System.out.println((file.isFile()?"file":"directory"));
        }
    }
}
