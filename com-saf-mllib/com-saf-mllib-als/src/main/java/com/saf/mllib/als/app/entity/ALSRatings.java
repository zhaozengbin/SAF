package com.saf.mllib.als.app.entity;

import com.saf.mllib.als.app.AbstractALS;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import scala.Serializable;
import scala.Tuple2;

public class ALSRatings extends AbstractALS<ALSRatings.Ratings> implements Serializable {
    private static final Logger LOGGER = Logger.getLogger(ALSRatings.class);

    public ALSRatings() {
        super();
    }

    public ALSRatings(String dataFilePath, JavaSparkContext javaSparkContex) {
        super(dataFilePath, javaSparkContex);
    }

    @Override
    public JavaRDD createJavaRDD() {
        JavaRDD<String> data = super.dataFileToJavaRDD();
        // 所有评分数据，由于此数据要分三部分使用，60%用于训练，20%用于验证，最后20%用于测试。将时间戳%10可以得到近似的10等分，用于三部分数据切分
        JavaRDD<Tuple2<Integer, Rating>> javaRDD = data.map(new Function<String, Tuple2<Integer, Rating>>() {
            @Override
            public Tuple2<Integer, Rating> call(String line) {
                String[] fields = line.split("::");
                if (fields.length != 4) {
                    throw new IllegalArgumentException("每一行必须有且只有4个元素");
                }
                int userId = Integer.parseInt(fields[0]);
                int productId = Integer.parseInt(fields[1]);
                double rating = Float.parseFloat(fields[2]);
                int timestamp = (int) (Long.parseLong(fields[3]) % 10);
                LOGGER.debug(String.format("ALSRatings JavaRDD : userId ==> %d , productId ==> %d , rating ==> %f", userId, productId, rating));
                Tuple2<Integer, Rating> tuple2 = new Tuple2<>(timestamp, new Rating(userId, productId, rating));
                return tuple2;
            }
        });

        long ratings = javaRDD.count();
        long users = javaRDD.map(new Function<Tuple2<Integer, Rating>, Integer>() {
            @Override
            public Integer call(Tuple2<Integer, Rating> integerRatingTuple2) {
                return integerRatingTuple2._2.user();
            }
        }).distinct().count();
        long products = javaRDD.map(new Function<Tuple2<Integer, Rating>, Integer>() {
            @Override
            public Integer call(Tuple2<Integer, Rating> integerRatingTuple2) {
                return integerRatingTuple2._2.product();
            }
        }).distinct().count();

        LOGGER.info(String.format("get %d ratings from %d users on %d products", ratings, users, products));

        return javaRDD;
    }


    @Override
    public Dataset<Ratings> javaRDDToDataSet(JavaRDD javaRDD) {
        JavaRDD<Ratings> ratingsJavaRDD = javaRDD.map(new Function<Tuple2<Integer, Rating>, Ratings>() {

            @Override
            public Ratings call(Tuple2<Integer, Rating> tuple2) {
                if (tuple2 == null) {
                    throw new IllegalArgumentException("tuple is null");
                }
                int userId = tuple2._2.user();
                int productId = tuple2._2.product();
                double rating = tuple2._2.rating();
                int timestamp = tuple2._1;
                LOGGER.debug(String.format("ALSRatings JavaRDDToDataSet : userId ==> %d , productId ==> %d , rating ==> %f", userId, productId, rating));
                Ratings ratings = new Ratings(userId, productId, rating);
                return ratings;
            }
        });
        SQLContext sqlContext = new SQLContext(super.getJavaSparkContex());
        Dataset result = sqlContext.createDataFrame(ratingsJavaRDD, Ratings.class);
        return result;
    }


    public static class Ratings implements Serializable {

        private int userId;

        private int productId;

        private double rating;

        public Ratings(int userId, int productId, double rating) {
            this.userId = userId;
            this.productId = productId;
            this.rating = rating;
        }

        public int getUserId() {
            return userId;
        }

        public void setUserId(int userId) {
            this.userId = userId;
        }

        public int getProductId() {
            return productId;
        }

        public void setProductId(int productId) {
            this.productId = productId;
        }

        public double getRating() {
            return rating;
        }

        public void setRating(double rating) {
            this.rating = rating;
        }
    }
}
