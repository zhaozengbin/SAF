package com.saf.mllib.kmeans.app.impl;

import com.saf.core.common.utils.ObjectUtils;
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

    public static String hdfsPath;

    public static KMeansModel train(JavaRDD<Vector> rdd, List<Integer> ks, List<Integer> maxIterators, List<Integer> runs, String initializationMode, Long seed) {
        Double bestCost = null;
        KMeansModel bestKMeansModel = null;
        int bestK = 0;
        int bestMaxIterator = 0;
        int bestRun = 0;

        for (Integer k : ks) {
            for (Integer maxIterator : maxIterators) {
                for (Integer run : runs) {
                    KMeansResult kMeansResult = train(rdd, k, maxIterator, run, initializationMode, seed);

                    Map<Integer, Long> map = kMeansResult.getContByKey();
                    for (Map.Entry<Integer, Long> entry : map.entrySet()) {
                        System.out.println("kmeans info countbyk Key = " + entry.getKey() + ", Value = " + entry.getValue());
                    }

                    //打印出中心点
                    System.out.println("kmeans info Cluster centers:");
                    for (Vector center : kMeansResult.getClusterCenters()) {
                        System.out.println("kmeans info center" + center);
                    }

                    //计算cost
                    // Evaluate clustering by computing Within Set Sum of Squared Errors
                    System.out.println("kmeans info Within Set Sum of Squared Errors = " + k + " -- " + kMeansResult.getCost());

                    if (bestCost == null || bestCost > kMeansResult.getCost()) {
                        bestK = k;
                        bestMaxIterator = maxIterator;
                        bestRun = run;
                        bestCost = kMeansResult.getCost();
                        bestKMeansModel = kMeansResult.kMeansModel;
                    }
                }
            }
        }
        System.out.println(String.format("best param : k = %d, maxIterator = %d, run = %d, cost = %f", bestK, bestMaxIterator, bestRun, bestCost));
        return bestKMeansModel;
    }

    public static KMeansResult train(JavaRDD<Vector> rdd, int k, int maxIterator, int runs, String initializationMode, Long seed) {
        final KMeansModel kMeansModel = model(rdd, k, maxIterator, runs, initializationMode, seed);

        //计算测试数据分别属于那个簇类
        JavaPairRDD<Integer, Vector> predictRDD = rdd.mapToPair(x -> {
            int predict = kMeansModel.predict(x);
            System.out.println("kmeans info data point = " + x + ", predict = " + predict);
            return new Tuple2<Integer, Vector>(predict, x);
        });
        return new KMeansResult(rdd, kMeansModel, predictRDD);
    }

    private static KMeansModel model(JavaRDD<Vector> rdd, int k, int maxIterator, int runs, String initializationMode, Long seed) {
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
        return kMeans.run(rdd.rdd());
    }


    public static class KMeansResult {

        private JavaRDD<Vector> javaRDD;

        private KMeansModel kMeansModel;

        private JavaPairRDD<Integer, Vector> predictDetail;

        public KMeansResult(JavaRDD<Vector> javaRDD, KMeansModel kMeansModel, JavaPairRDD<Integer, Vector> predictDetail) {
            this.javaRDD = javaRDD;
            this.kMeansModel = kMeansModel;
            this.predictDetail = predictDetail;
        }

        // 聚类中心点打分
        public Double getCost() {
            if (ObjectUtils.isNotEmpty(kMeansModel)) {
                return kMeansModel.computeCost(javaRDD.rdd()) % 2.2f;
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
    }
}
