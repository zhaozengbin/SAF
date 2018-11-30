package com.saf.mllib.kmeans.app.impl;

import com.alibaba.fastjson.JSONObject;
import com.saf.core.common.utils.ObjectUtils;
import com.saf.mllib.core.common.constant.ConstantSparkTask;
import com.saf.mllib.core.common.utils.RedisUtils;
import com.saf.mllib.core.entity.dto.WebSocketResponseMessageDto;
import com.saf.mllib.kmeans.app.entity.KMeansDataResult;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import scala.Tuple2;

import java.util.List;
import java.util.Map;

public class KMeansImpl {
    private static final Logger LOGGER = Logger.getLogger(KMeansImpl.class);

    /**
     * 方法：predict
     * 描述：预测分组结果
     * 作者：赵增斌 E-mail:zhaozengbin@gmail.com QQ:4415599 weibo:http://weibo.com/zhaozengbin
     * 时间: 2018年11月09日 11:48 AM
     * 参数：[points, kMeansModel]
     * 返回: java.util.List<java.lang.Integer>
     */
    private static List<Integer> predict(JavaRDD<Vector> points, KMeansModel kMeansModel) {
        return kMeansModel.predict(points).collect();
    }

    /**
     * 方法：train
     * 描述：通过不同的参数组合训练最好的模型
     * 作者：赵增斌 E-mail:zhaozengbin@gmail.com QQ:4415599 weibo:http://weibo.com/zhaozengbin
     * 时间: 2018年11月09日 11:43 AM
     * 参数：[rdd, ks, maxIterators, runs, initializationMode, seed]
     * 返回: org.apache.spark.mllib.clustering.KMeansModel
     */
    public static KMeansDataResult train(JavaPairRDD<String, Vector> rdd, List<Integer> ks, List<Integer> maxIterators, List<Integer> runs, String initializationMode, Long seed) {
        Double bestCost = null;
        KMeansModel bestKMeansModel = null;
        int bestK = 0;
        int bestMaxIterator = 0;
        int bestRun = 0;
        KMeansDataResult kMeansDataResult = new KMeansDataResult();
        for (Integer k : ks) {
            for (Integer maxIterator : maxIterators) {
                for (Integer run : runs) {
                    KMeansResult kMeansResult = train(rdd, k, maxIterator, run, initializationMode, seed);

                    Map<Integer, Long> map = kMeansResult.getContByKey();
                    for (Map.Entry<Integer, Long> entry : map.entrySet()) {
                        LOGGER.info("kmeans info countbyk Key = " + entry.getKey() + ", Value = " + entry.getValue());
                    }

                    //打印出中心点
                    LOGGER.info("kmeans info Cluster centers:");
                    for (Vector center : kMeansResult.getClusterCenters()) {
                        LOGGER.info("kmeans info center" + center);
                    }

                    //计算cost
                    // Evaluate clustering by computing Within Set Sum of Squared Errors
                    LOGGER.info("kmeans info Within Set Sum of Squared Errors = " + k + " -- " + kMeansResult.getCost());
                    final JSONObject jsonObject = new JSONObject();
                    jsonObject.put("submissionId", RedisUtils.getJedis().exists(ConstantSparkTask.KMEANS_CURRENT_SUBMISSIONID) ? RedisUtils.getJedis().get(ConstantSparkTask.KMEANS_CURRENT_SUBMISSIONID) : "");
                    jsonObject.put("k", k);
                    jsonObject.put("iter", maxIterator);
                    jsonObject.put("run", run);
                    jsonObject.put("cost", kMeansResult.getCost());
                    final WebSocketResponseMessageDto dto = new WebSocketResponseMessageDto(1, jsonObject);
                    RedisUtils.getJedis().publish("variance", JSONObject.toJSONString(dto));
                    if (bestCost == null || bestCost > kMeansResult.getCost()) {
                        bestK = k;
                        bestMaxIterator = maxIterator;
                        bestRun = run;
                        bestCost = kMeansResult.getCost();
                        bestKMeansModel = kMeansResult.kMeansModel;

                        kMeansDataResult.setBestK(bestK);
                        kMeansDataResult.setBestMaxIterator(bestMaxIterator);
                        kMeansDataResult.setBestRun(bestRun);
                        kMeansDataResult.setkMeansModel(bestKMeansModel);
                    }
                }
            }
        }
        final JSONObject jsonObject = new JSONObject();
        jsonObject.put("submissionId", RedisUtils.getJedis().exists(ConstantSparkTask.KMEANS_CURRENT_SUBMISSIONID) ? RedisUtils.getJedis().get(ConstantSparkTask.KMEANS_CURRENT_SUBMISSIONID) : "");
        jsonObject.put("k", bestK);
        jsonObject.put("iter", bestMaxIterator);
        jsonObject.put("run", bestRun);
        jsonObject.put("cost", bestCost);
        jsonObject.put("isBest", true);
        final WebSocketResponseMessageDto dto = new WebSocketResponseMessageDto(1, jsonObject);
        RedisUtils.getJedis().publish("variance", JSONObject.toJSONString(dto));

        LOGGER.info(String.format("kmeans info best param : k = %d, maxIterator = %d, run = %d, cost = %f", bestK, bestMaxIterator, bestRun, bestCost));
        return kMeansDataResult;
    }

    /**
     * 方法：train
     * 描述：根据指定参数获取推荐模型
     * 作者：赵增斌 E-mail:zhaozengbin@gmail.com QQ:4415599 weibo:http://weibo.com/zhaozengbin
     * 时间: 2018年11月09日 11:44 AM
     * 参数：[rdd, k, maxIterator, runs, initializationMode, seed]
     * 返回: com.saf.mllib.kmeans.app.impl.KMeansImpl.KMeansResult
     */
    public static KMeansResult train(JavaPairRDD<String, Vector> rdd, int k, int maxIterator, int runs, String initializationMode, Long seed) {
        final KMeansModel kMeansModel = model(rdd, k, maxIterator, runs, initializationMode, seed);

        //计算测试数据分别属于那个簇类
        JavaPairRDD<Integer, Vector> predictRDD = rdd.mapToPair(x -> {
            int predict = kMeansModel.predict(x._2());
            LOGGER.info(String.format("kmeans info predict:%d ,train: %s", predict, x._1()));
            return new Tuple2<Integer, Vector>(predict, x._2());
        });
        LOGGER.info(String.format("kmeans info train: %s", predictRDD.collect()));
        return new KMeansResult(rdd, kMeansModel, predictRDD);
    }

    private static KMeansModel model(JavaPairRDD<String, Vector> rdd, int k, int maxIterator, int runs, String initializationMode, Long seed) {
        KMeans kMeans = new KMeans();
        if (ObjectUtils.isNotEmpty(k)) {
            kMeans.setK(k);
        }
        if (ObjectUtils.isNotEmpty(maxIterator)) {
            kMeans.setMaxIterations(maxIterator);
        }
        if (ObjectUtils.isNotEmpty(runs)) {
            kMeans.setRuns(runs);
        }
        if (ObjectUtils.isNotEmpty(initializationMode)) {
            kMeans.setInitializationMode(initializationMode);
        }
        if (ObjectUtils.isNotEmpty(seed) && seed > 0) {
            kMeans.setSeed(seed);
        }
        return kMeans.run(rdd.values().rdd());
    }


    public static class KMeansResult {

        private JavaPairRDD<String, Vector> javaRDD;

        private KMeansModel kMeansModel;

        private JavaPairRDD<Integer, Vector> predictDetail;

        public KMeansResult(JavaPairRDD<String, Vector> javaRDD, KMeansModel kMeansModel, JavaPairRDD<Integer, Vector> predictDetail) {
            this.javaRDD = javaRDD;
            this.kMeansModel = kMeansModel;
            this.predictDetail = predictDetail;
        }

        // 聚类中心点打分
        public Double getCost() {
            if (ObjectUtils.isNotEmpty(kMeansModel)) {
                return kMeansModel.computeCost(javaRDD.values().rdd()) % 2.2f;
            }
            return null;
        }

        // 聚类分组及聚类分组聚集的数量
        public Map<Integer, Long> getContByKey() {
            if (ObjectUtils.isNotEmpty(predictDetail)) {
                return predictDetail.countByKey();
            }
            return null;
        }

        // 中心点
        public Vector[] getClusterCenters() {
            if (ObjectUtils.isNotEmpty(kMeansModel)) {
                return kMeansModel.clusterCenters();
            }
            return null;
        }

        public KMeansModel getkMeansModel() {
            return kMeansModel;
        }

        public JavaPairRDD<Integer, Vector> getPredictDetail() {
            return predictDetail;
        }
    }
}
