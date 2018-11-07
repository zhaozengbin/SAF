package com.saf.mllib.als.base;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;

public abstract class AbstarctMllib<T> {
    //数据文件路径
    private String dataFilePath;

    private JavaSparkContext javaSparkContex;

    public AbstarctMllib() {
    }

    public AbstarctMllib(String dataFilePath, JavaSparkContext javaSparkContex) {
        this.dataFilePath = dataFilePath;
        this.javaSparkContex = javaSparkContex;
    }

    public abstract JavaRDD createJavaRDD();

    /**
     * 方法：JavaRDDToDataSet
     * 描述：javaRDD 转换数据结果集
     * 作者：赵增斌 E-mail:zhaozengbin@gmail.com QQ:4415599 weibo:http://weibo.com/zhaozengbin
     * 时间: 2018年07月23日 上午10:18
     * 参数：[javaRDD 解析后的数据]
     * 返回: org.apache.spark.sql.Dataset
     */
    public abstract Dataset<T> javaRDDToDataSet(JavaRDD javaRDD);

    /**
     * 方法：dataFileToJavaRDD
     * 描述：数据文件转换 JAVARDD
     * 作者：赵增斌 E-mail:zhaozengbin@gmail.com QQ:4415599 weibo:http://weibo.com/zhaozengbin
     * 时间: 2018年07月20日 上午11:55
     * 参数：[javaSparkContext]
     * 返回: org.apache.spark.api.java.JavaRDD<java.lang.String>
     */
    protected JavaRDD<String> dataFileToJavaRDD() {
        // 加载评分数据
        JavaRDD<String> data = this.getJavaSparkContex().textFile(this.getDataFilePath());
        return data;
    }


    public String getDataFilePath() {
        return dataFilePath;
    }

    public void setDataFilePath(String dataFilePath) {
        this.dataFilePath = dataFilePath;
    }

    public JavaSparkContext getJavaSparkContex() {
        return javaSparkContex;
    }

    public void setJavaSparkContex(JavaSparkContext javaSparkContex) {
        this.javaSparkContex = javaSparkContex;
    }
}
