package com.saf.mllib.kmeans.app.impl;

import com.saf.core.common.utils.ObjectUtils;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import scala.Tuple2;

import java.util.Map;

public class KMeansImpl {
    private static final Logger LOGGER = Logger.getLogger(KMeansImpl.class);

    public static String hdfsPath;

    public static void train(JavaRDD<Vector> rdd, int k, int maxIterator, int runs, String initializationMode, Long seed) {
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
        final KMeansModel kMeansModel = kMeans.run(rdd.rdd());

        //计算测试数据分别属于那个簇类
        JavaPairRDD<Integer, Vector> countbyk = rdd.mapToPair(x -> {
            int pd = kMeansModel.predict(x);
            System.out.println("kmeans info data point = " + x + ", predict = " + pd);
            return new Tuple2<Integer, Vector>(pd, x);
        });
        Map<Integer, Long> map = countbyk.countByKey();
        for (Map.Entry<Integer, Long> entry : map.entrySet()) {
            System.out.println("kmeans info countbyk Key = " + entry.getKey() + ", Value = " + entry.getValue());
        }

        //打印出中心点
        System.out.println("kmeans info Cluster centers:");
        for (Vector center : kMeansModel.clusterCenters()) {
            System.out.println("kmeans info center" + center);
        }

        //计算cost
        // Evaluate clustering by computing Within Set Sum of Squared Errors
        double WSSSE = kMeansModel.computeCost(rdd.rdd());
        System.out.println("kmeans info Within Set Sum of Squared Errors = " + WSSSE);
    }
}
