package com.saf.mllib.kmeans.app;

import com.saf.mllib.core.app.base.AbstarctMllib;
import org.apache.spark.api.java.JavaSparkContext;

public abstract class AbstractKMeans<T> extends AbstarctMllib<T> {


    public AbstractKMeans() {
    }

    public AbstractKMeans(String dataFilePath, JavaSparkContext javaSparkContex) {
        super(dataFilePath, javaSparkContex);
    }

}
