package com.saf.mllib.als.app;

import com.saf.mllib.als.base.AbstarctMllib;
import org.apache.spark.api.java.JavaSparkContext;

public abstract class AbstractALS<T> extends AbstarctMllib<T> {


    public AbstractALS() {
    }

    public AbstractALS(String dataFilePath, JavaSparkContext javaSparkContex) {
        super(dataFilePath, javaSparkContex);
    }

}
