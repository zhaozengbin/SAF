package com.saf.mllib.als.app.entity;

import com.saf.mllib.als.app.AbstractALS;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import scala.Tuple3;

import java.io.Serializable;

public class ALSProducts extends AbstractALS<ALSProducts.Products> implements Serializable {
    private static final Logger LOGGER = Logger.getLogger(ALSProducts.class);

    public ALSProducts() {
    }

    public ALSProducts(String dataFilePath, JavaSparkContext javaSparkContex) {
        super(dataFilePath, javaSparkContex);
    }

    @Override
    public JavaRDD createJavaRDD() {
        JavaRDD<String> data = super.dataFileToJavaRDD();
        // 将电影的id，标题，类型以三元组的形式保存
        JavaRDD<Tuple3<Integer, String, String>> javaRDD = data.map(new Function<String, Tuple3<Integer, String, String>>() {
            @Override
            public Tuple3<Integer, String, String> call(String line) {
                String[] fields = line.split("::");
                if (fields.length != 3) {
                    throw new IllegalArgumentException("Each line must contain 3 fields");
                }
                int id = Integer.parseInt(fields[0]);
                String title = fields[1];
                String type = fields[2];
                LOGGER.debug(String.format("ALSProducts JavaRDD : userId ==> %d , title ==> %s , type ==> %s", id, title, type));
                Tuple3<Integer, String, String> tuple3 = new Tuple3<>(id, title, type);
                return tuple3;
            }
        });
        return javaRDD;
    }

    @Override
    public Dataset<Products> javaRDDToDataSet(JavaRDD javaRDD) {
        // 将电影的id，标题，类型以三元组的形式保存
        JavaRDD<Products> productJavaRDD = javaRDD.map(new Function<Tuple3<Integer, String, String>, Products>() {

            @Override
            public Products call(Tuple3<Integer, String, String> tuple3) {
                if (tuple3 == null) {
                    throw new IllegalArgumentException("tuple is null");
                }
                int id = tuple3._1();
                String title = tuple3._2();
                String type = tuple3._3();
                LOGGER.debug(String.format("ALSProducts JavaRDDToDataSet : userId ==> %d , title ==> %s , type ==> %s", id, title, type));
                Products products = new Products(id, title, type);
                return products;
            }
        });
        SQLContext sqlContext = new SQLContext(super.getJavaSparkContex());
        Dataset result = sqlContext.createDataFrame(productJavaRDD, Products.class);
        return result;
    }


    public static class Products implements Serializable {
        private int id;

        private String title;

        private String type;

        private Double rating;

        public Products(int id, String title, String type) {
            this.id = id;
            this.title = title;
            this.type = type;
        }

        public Products(int id, String title, String type, Double rating) {
            this.id = id;
            this.title = title;
            this.type = type;
            this.rating = rating;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public Double getRating() {
            return rating;
        }

        public void setRating(Double rating) {
            this.rating = rating;
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this.rating != null) {
                if (obj instanceof Products) {
                    Products products = (Products) obj;
                    return this.getId() == products.getId();
                }
                return false;
            }
            return super.equals(obj);
        }
    }
}
