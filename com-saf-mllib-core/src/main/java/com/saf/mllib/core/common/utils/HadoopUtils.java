package com.saf.mllib.core.common.utils;

import com.alibaba.fastjson.JSON;
import com.saf.core.common.utils.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class HadoopUtils {
    private static final String HDFS_URL = "hdfs://112.125.16.240:9000";

    private static HadoopUtils instance;

    public FileSystem fs;

    private String currentHdfsPath;

    private Configuration conf;

    private HadoopUtils() {

    }

    public static void main(String[] args) throws Exception {
        HadoopUtils hadoopUtils = HadoopUtils.getInstance("hdfs://localhost:9000");
        String fileName = "/user/spark/als/logs/model_practice_output_file_path.log";
        hadoopUtils.writeContent("{\"123\":456}", fileName);
        hadoopUtils.read(fileName);
        //读取文件内容
        //readFile(args[0]);
        //创建文件目录
        /*String s= "hello";
        byte[] bytes = s.getBytes();
        createFile("/liu/h.txt",bytes);*/

        //删除文件
        /*rmdir("/liu2");*/

        //上传文件
        /*uploadFile("/home/liu/hello.text", "/liu/hello.text");*/

        //列出文件
//        HadoopUtils.getInstance().listFile("/liu");

        //文科重命名
        /*renameFile("/liu/hi.txt", "/liu/he1.text");*/

        //查询目录是否存在
        /*boolean existDir = existDir("/liu2", false);
        System.out.println(existDir);*/

        //写入文件末尾
//        appendFile("/home/liu/hello.text", "/liu1/hello.text");
//        FsStatus fsStatus = HadoopUtils.getInstance(HDFS_URL).fs.getStatus();
//        System.out.println(JSON.toJSON(fsStatus));
    }


    private void init(String path) throws Exception {
        if (fs == null || ObjectUtils.isNotEmpty(path)) {
            //读取配置文件
            conf = new Configuration();
            conf.setBoolean("fs.hdfs.impl.disable.cache", true);
            //获取文件系统
            fs = FileSystem.get(URI.create(path), conf);
            currentHdfsPath = path;
        }
    }

    public static synchronized HadoopUtils getInstance(String path) throws Exception {
        if (instance == null) {
            instance = new HadoopUtils();
        }
        instance.init(path);
        return instance;
    }

    public boolean mkdirs(String path) {
        try {
            Path srcPath = new Path(path);
            //调用mkdir（）创建目录，（可以一次性创建，以及不存在的父目录）
            return fs.mkdirs(srcPath);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            //关闭文件系统
            IOUtils.closeStream(fs);
        }
    }

    public boolean rmdir(String filePath) {
        try {
            Path path = new Path(filePath);
            //调用deleteOnExit(）
            return fs.deleteOnExit(path);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } finally {
            //关闭文件系统
            IOUtils.closeStream(fs);
        }
    }

    public boolean createFile(String dst, byte[] contents) {
        FSDataOutputStream outputStream = null;
        try {
            //目标路径
            Path dstPath = new Path(dst);
            //打开一个输出流
            outputStream = fs.create(dstPath);
            outputStream.write(contents);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } finally {
            //关闭文件系统
            IOUtils.closeStream(outputStream);
            IOUtils.closeStream(fs);
        }
    }

    public FileStatus[] listFile(String path) {
        //获取文件或目录状态
        try {
            return fs.listStatus(new Path(path));
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        } finally {
            //关闭文件系统
            IOUtils.closeStream(fs);
        }
    }

    public FileStatus[] uploadFile(String src, String dst) {
        Path srcPath = new Path(src); //原路径
        Path dstPath = new Path(dst); //目标路径
        List<String> result = new ArrayList<>();
        //调用文件系统的文件复制函数,前面参数是指是否删除原文件，true为删除，默认为false
        try {
            fs.copyFromLocalFile(false, srcPath, dstPath);
            return fs.listStatus(dstPath);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        } finally {
            //关闭文件系统
            IOUtils.closeStream(fs);
        }
    }

    public boolean renameFile(String oldName, String newName) {
        Path oldPath = new Path(oldName);
        Path newPath = new Path(newName);
        try {
            return fs.rename(oldPath, newPath);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } finally {
            IOUtils.closeStream(fs);
        }
    }


    public boolean readFile(String uri, File targetFile) {
        InputStream in = null;
        FileOutputStream fileOutputStream = null;
        try {
            fileOutputStream = new FileOutputStream(targetFile);
            in = fs.open(new Path(uri));
            //复制到标准输出流
            IOUtils.copyBytes(in, fileOutputStream, 4096, false);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            IOUtils.closeStream(fileOutputStream);
            IOUtils.closeStream(in);
        }
    }

    public boolean existDir(String filePath, boolean create) {
        //判断是否存在
        if (StringUtils.isEmpty(filePath)) {
            return false;
        }
        Path path = new Path(filePath);
        //读取配置文件
        try {
            //或者create为true
            if (create) {
                //如果文件不存在
                if (!fs.exists(path)) {
                    fs.mkdirs(path);
                }
            }
            //判断是否为目录
            if (fs.isDirectory(path)) {
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return false;
    }

    public boolean existFile(String filePath, boolean create) {
        //判断是否存在
        if (StringUtils.isEmpty(filePath)) {
            return false;
        }
        Path path = new Path(filePath);
        //读取配置文件
        try {
            //或者create为true
            if (create) {
                //如果文件不存在
                if (!fs.exists(path)) {
                    fs.mkdirs(path);
                    fs.create(path);
                }
            }
            //判断是否存在文件
            if (fs.isFile(path)) {
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return false;
    }

    /**
     * 添加到文件的末尾(src为本地地址，dst为hdfs文件地址)
     */
    public boolean appendFile(String src, String dst) {
        Path dstPath = new Path(dst);
        //创建需要写入的文件流
        try {
            FileInputStream fileInputStream = new FileInputStream(src);
            InputStream in = new BufferedInputStream(fileInputStream);
            //文件输出流写入
            FSDataOutputStream out = fs.append(dstPath);
            IOUtils.copyBytes(in, out, 4096, true);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            IOUtils.closeStream(fs);
        }
    }

    /**
     * 读取指定路径下的文件
     *
     * @param fileName
     */
    public void read(String fileName) {
        //read path
        Path readPath = new Path(fileName);
        FSDataInputStream inStream = null;

        try {
            inStream = fs.open(readPath);
            //read 输出到控制台System.out
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            IOUtils.copyBytes(inStream, baos, 4096, false);
            String result = baos.toString();
            System.out.println("------" + result);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(inStream);
        }
    }

    /**
     * 将本地文件 上传到HDFS
     *
     * @param content 文件内容
     * @param dPath   目标文件地址
     */
    public void writeContent(String content, String dPath) {
        //目标文件
        String putFileName = dPath;
        Path writePath = new Path(putFileName);
        FSDataOutputStream outStream = null;
        ByteArrayInputStream inStream = null;
        try {
            //输出流
            outStream = fs.create(writePath);
            //输入流
            inStream = new ByteArrayInputStream(content.getBytes("UTF-8"));
            //流操作
            IOUtils.copyBytes(inStream, outStream, 4096, false);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(inStream);
            IOUtils.closeStream(outStream);
        }
    }

    /**
     * 将本地文件 上传到HDFS
     *
     * @param sPath 本地文件地址
     * @param dPath 目标文件地址
     */
    public void write(String sPath, String dPath) {
        //目标文件
        String putFileName = dPath;
        Path writePath = new Path(putFileName);
        FSDataOutputStream outStream = null;
        FileInputStream inStream = null;
        try {
            //输出流
            outStream = fs.create(writePath);
            //输入流
            inStream = new FileInputStream(new File(sPath));
            //流操作
            IOUtils.copyBytes(inStream, outStream, 4096, false);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(inStream);
            IOUtils.closeStream(outStream);
        }
    }
}
