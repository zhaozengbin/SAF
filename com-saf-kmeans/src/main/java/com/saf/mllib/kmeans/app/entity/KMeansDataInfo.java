package com.saf.mllib.kmeans.app.entity;

import com.saf.mllib.kmeans.app.AbstractKMeans;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.io.Serializable;

public class KMeansDataInfo extends AbstractKMeans<Vector> implements Serializable {
    public KMeansDataInfo() {
    }

    public KMeansDataInfo(String dataFilePath, JavaSparkContext javaSparkContex) {
        super(dataFilePath, javaSparkContex);
    }

    @Override
    public JavaRDD createJavaRDD() {
        JavaRDD<String> data = super.dataFileToJavaRDD();
        JavaRDD<Vector> parsedData = data.map(s -> {
            String[] sarray = s.split(",");
            double[] values = new double[sarray.length - 1];
            for (int i = 0; i < sarray.length; i++) {
                if (i == 0) {
                    continue;
                }
                values[i - 1] = Double.parseDouble(sarray[i]);
            }
            return Vectors.dense(values);
        });
        return parsedData;
    }

    @Override
    public JavaPairRDD<String, Vector> createJavaPairRDD() {
        JavaRDD<String> data = super.dataFileToJavaRDD();
        JavaPairRDD<String, Vector> parsedData = data.mapToPair(s -> {
            String[] sarray = s.split(",");
            double[] values = new double[sarray.length - 1];
            for (int i = 0; i < sarray.length; i++) {
                if (i == 0) {
                    continue;
                }
                values[i - 1] = Double.parseDouble(sarray[i]);
            }
            return new Tuple2<>(s, Vectors.dense(values));
        });
        return parsedData;
    }

    @Override
    public Dataset<Vector> javaRDDToDataSet(JavaRDD javaRDD) {
        SQLContext sqlContext = new SQLContext(super.getJavaSparkContex());
        Dataset result = sqlContext.createDataFrame(javaRDD, Vector.class);
        return result;
    }
}
