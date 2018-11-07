package com.saf.mllib.kmeans.app;

import com.saf.core.common.utils.ObjectUtils;
import com.saf.mllib.kmeans.app.entity.KMeansDataInfo;
import com.saf.mllib.kmeans.app.impl.KMeansImpl;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Serializable;

public class KMeans implements Serializable {
    private static final Logger LOGGER = Logger.getLogger(KMeans.class);

    private static KMeans instance;

    private String sparkAppName;

    private String sparkMaster;

    private String filePath;

    static {
        //屏蔽日志，由于结果是打印在控制台上的，为了方便查看结果，将spark日志输出关掉
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF);
    }

    public KMeans() {

    }

    /**
     * 方法：ALS
     * 描述：TODO
     * 作者：赵增斌 E-mail:zhaozengbin@gmail.com QQ:4415599 weibo:http://weibo.com/zhaozengbin
     * 时间: 2018年07月20日 上午10:45
     * 参数：[sparkAppName, sparkMaster]
     * 返回:
     */
    public KMeans(String sparkAppName, String sparkMaster) {
        this.sparkAppName = sparkAppName;
        this.sparkMaster = sparkMaster;
    }

    public KMeans(String sparkAppName, String sparkMaster, String filePath) {
        this.sparkAppName = sparkAppName;
        this.sparkMaster = sparkMaster;
        this.filePath = filePath;
    }

    /**
     * 方法：getInstance
     * 描述：TODO
     * 作者：赵增斌 E-mail:zhaozengbin@gmail.com QQ:4415599 weibo:http://weibo.com/zhaozengbin
     * 时间: 2018年07月20日 上午10:46
     * 参数：[sparkAppName, sparkMaster]
     * 返回: com.duia.spark.mllib.als.ALS
     */
    public static synchronized KMeans getInstance(String sparkAppName, String sparkMaster) {

        if (instance == null) {
            instance = new KMeans(sparkAppName, sparkMaster);
        }
        return instance;
    }

    public static synchronized KMeans getInstance(String sparkAppName, String sparkMaster, String flePath) {

        if (instance == null) {
            instance = new KMeans(sparkAppName, sparkMaster, flePath);
        }
        return instance;
    }

    public void execute(String hdfsPath, int k, int maxIterations, int run, String initializationMode, long seed) {
        // 创建入口对象
        if (filePath == null) {
            return;
        }

        SparkConf conf = new SparkConf().setAppName(this.getSparkAppName());
        if (this.getSparkMaster() != null && !this.getSparkMaster().startsWith("spark://")) {
            conf.setMaster(this.getSparkMaster());
            conf.set("spark.driver.allowMultipleContexts", "true");
        }
        JavaSparkContext sc = new JavaSparkContext(conf);

        KMeansDataInfo kMeansDataInfo = new KMeansDataInfo(filePath, sc);
        KMeansImpl.train(kMeansDataInfo.createJavaRDD(), k, maxIterations, run, initializationMode, seed);
        System.out.println("任务结束");
    }

    public static void main(String[] args) {
//        if (args.length == 9) {
//            String sparkAppName = args[0];
//            String sparkMaster = args[1];
//            String hdfsPath = args[2];
//            String filePath = args[3];
//            String k = args[4];
//            String maxIterator = args[5];
//            String run = args[6];
//            String initializationMode = args[7];
//            String seed = args[8];

            String sparkAppName = "k-means";
            String sparkMaster = "local[*]";
            String hdfsPath = "";
            String filePath = "/Users/zhaozengbin/data/spark/k-means/unzip/kmeans_game.csv";
            String k = "3";
            String maxIterator = "20";
            String run = "10";
            String initializationMode = "random";
            String seed = null;


            int kInt = 0;
            int maxIteratorInt = 0;
            int runInt = 0;
            long seedLong = 0;
            if (ObjectUtils.isNumber(k)) {
                kInt = Integer.parseInt(k);
            }
            if (ObjectUtils.isNumber(maxIterator)) {
                maxIteratorInt = Integer.parseInt(maxIterator);
            }
            if (ObjectUtils.isNotEmpty(hdfsPath)) {
                filePath = hdfsPath + filePath;
            }
            if (ObjectUtils.isNotEmpty(run)) {
                runInt = Integer.parseInt(run);
            }
            if (ObjectUtils.isNotEmpty(seed)) {
                seedLong = Long.parseLong(seed);
            }
            KMeans kMeans = KMeans.getInstance(sparkAppName, sparkMaster, filePath);
            kMeans.execute(hdfsPath, kInt, maxIteratorInt, runInt, initializationMode, seedLong);
//        }
    }

    public String getSparkAppName() {
        return sparkAppName;
    }


    public String getSparkMaster() {
        return sparkMaster;
    }
}