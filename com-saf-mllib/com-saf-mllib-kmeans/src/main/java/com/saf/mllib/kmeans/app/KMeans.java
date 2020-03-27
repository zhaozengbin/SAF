package com.saf.mllib.kmeans.app;

import com.alibaba.fastjson.JSONObject;
import com.saf.core.common.utils.ObjectUtils;
import com.saf.mllib.core.common.constant.ConstantSparkTask;
import com.saf.mllib.core.common.utils.RedisUtils;
import com.saf.mllib.core.entity.dto.WebSocketResponseMessageDto;
import com.saf.mllib.kmeans.app.entity.KMeansDataInfo;
import com.saf.mllib.kmeans.app.entity.KMeansDataResult;
import com.saf.mllib.kmeans.app.impl.KMeansImpl;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.util.Utils;
import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class KMeans implements Serializable {
    private static final Logger LOGGER = Logger.getLogger(KMeans.class);

    private static KMeans instance;

    private String sparkAppName;

    private String sparkMaster;

    private String filePath;

    static {
        //屏蔽日志，由于结果是打印在控制台上的，为了方便查看结果，将spark日志输出关掉
        Logger.getLogger("org.apache.spark").setLevel(Level.INFO);
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

    public void execute(String hdfsPath, List<Integer> ks, List<Integer> maxIterations, List<Integer> runs, String initializationMode, boolean seedFlag, String testFilePath, String stepFlag) {
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
        JavaPairRDD<String, Vector> javaPairRDD = kMeansDataInfo.createJavaPairRDD();
        Long seed = null;
        if (seedFlag) {
            seed = Utils.random().nextLong();
        }
        if (stepFlag.equalsIgnoreCase("training")) {
            KMeansDataResult kMeansDataResult = KMeansImpl.train(javaPairRDD, ks, maxIterations, runs, initializationMode, seed);

//            KMeansImpl.KMeansResult kMeansResult = KMeansImpl.train(javaPairRDD, kMeansDataResult.getBestK(),
//                    kMeansDataResult.getBestMaxIterator(), kMeansDataResult.getBestRun(), initializationMode, seed);
//
//            // 输入训练模型
//            for (Tuple2<Integer, Vector> tuple2 : kMeansResult.getPredictDetail().collect()) {
//                final WebSocketResponseMessageDto dto = new WebSocketResponseMessageDto(2, new KMeansDataResult.KMeansData(RedisUtils.getJedis().exists(ConstantSparkTask.KMEANS_CURRENT_SUBMISSIONID) ? RedisUtils.getJedis().get(ConstantSparkTask.KMEANS_CURRENT_SUBMISSIONID) : "",
//                        JSON.toJSON(tuple2._2()).toString(), tuple2._1()));
//
//                String msg = JSONObject.toJSONString(dto);
//                LOGGER.info("根据计算模型获得的推荐:" + msg);
//                RedisUtils.getJedis().publish("variance", msg);
//            }

        } else if (stepFlag.equalsIgnoreCase("recommend") && ks.size() == 1 && maxIterations.size() == 1 && runs.size() == 1) {
            // 训练数据模型
            KMeansImpl.KMeansResult kMeansResult = KMeansImpl.train(javaPairRDD, ks.get(0), maxIterations.get(0), runs.get(0), initializationMode, seed);

            List<Tuple2<Integer, Vector>> trainList = kMeansResult.getPredictDetail().collect();
            for (Tuple2<Integer, Vector> item : trainList) {
                final WebSocketResponseMessageDto dto = new WebSocketResponseMessageDto(2, new KMeansDataResult.KMeansData(RedisUtils.getJedis().exists(ConstantSparkTask.KMEANS_CURRENT_SUBMISSIONID) ? RedisUtils.getJedis().get(ConstantSparkTask.KMEANS_CURRENT_SUBMISSIONID) : "",
                        item._2().toString(), item._1()));
                String msg = JSONObject.toJSONString(dto);
                LOGGER.info("根据计算模型获得训练数据分组:" + msg);
                RedisUtils.getJedis().publish("variance", JSONObject.toJSONString(dto));
            }

            // 开始测试数据
            kMeansDataInfo = new KMeansDataInfo(testFilePath, sc);
            javaPairRDD = kMeansDataInfo.createJavaPairRDD();
            List<Integer> list = kMeansResult.getkMeansModel().predict(javaPairRDD.values()).collect();
            for (int i = 0; i < list.size(); i++) {
                LOGGER.info(String.format("kmeans info predict:%d ,train: %s", list.get(i), javaPairRDD.keys().collect().get(i)));
                WebSocketResponseMessageDto dto = new WebSocketResponseMessageDto(3, new KMeansDataResult.KMeansData(RedisUtils.getJedis().exists(ConstantSparkTask.KMEANS_CURRENT_SUBMISSIONID) ? RedisUtils.getJedis().get(ConstantSparkTask.KMEANS_CURRENT_SUBMISSIONID) : "",
                        javaPairRDD.keys().collect().get(i), list.get(i)));
                String msg = JSONObject.toJSONString(dto);
                LOGGER.info("根据计算模型测试数据分组:" + msg);
                RedisUtils.getJedis().publish("variance", msg);
            }
        }
        System.out.println("任务结束");
    }

    public static void main(String[] args) {
        if (args.length == 11) {
            String sparkAppName = args[0];
            String sparkMaster = args[1];
            String hdfsPath = args[2];
            String filePath = args[3];
            String k = args[4];
            String maxIterator = args[5];
            String run = args[6];
            String initializationMode = args[7];
            String seed = args[8];
            String testFilePath = args[9];
            String stepFlag = args[10];


//        String sparkAppName = "k-means";
//        String sparkMaster = "local[*]";
//        String hdfsPath = "";
//        String filePath = "/Users/zhaozengbin/data/spark/k-means/unzip/kmeans_game.csv";
//        String k = "3";
//        String maxIterator = "20";
//        String run = "1";
//        String initializationMode = "random";
//        String seed = null;
//        String testFilePath = "/Users/zhaozengbin/data/spark/k-means/unzip/kmeans_game_test.csv";
//        String stepFlag = "recommend";

            List<Integer> kInt = new ArrayList<>();
            List<Integer> maxIteratorInt = new ArrayList<>();
            List<Integer> runInt = new ArrayList<>();
            boolean seedBoolean = false;
            if (ObjectUtils.isNotEmpty(k)) {
                kInt = ObjectUtils.string2IntegerList(k, ",");
            }
            if (ObjectUtils.isNotEmpty(maxIterator)) {
                maxIteratorInt = ObjectUtils.string2IntegerList(maxIterator, ",");
            }
            if (ObjectUtils.isNotEmpty(hdfsPath)) {
                filePath = hdfsPath + filePath;
                testFilePath = hdfsPath + testFilePath;
            }
            if (ObjectUtils.isNotEmpty(run)) {
                runInt = ObjectUtils.string2IntegerList(run, ",");
            }
            if (ObjectUtils.isNotEmpty(seed)) {
                seedBoolean = Boolean.parseBoolean(seed);
            }
            KMeans kMeans = KMeans.getInstance(sparkAppName, sparkMaster, filePath);
            kMeans.execute(hdfsPath, kInt, maxIteratorInt, runInt, initializationMode, seedBoolean, testFilePath, stepFlag);
        }
    }

    public String getSparkAppName() {
        return sparkAppName;
    }


    public String getSparkMaster() {
        return sparkMaster;
    }
}