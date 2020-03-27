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

public class ALSUserRatings extends AbstractALS<ALSUserRatings.UserProductRatings> implements Serializable {
    private static final Logger LOGGER = Logger.getLogger(ALSUserRatings.class);

    public ALSUserRatings() {
    }

    public ALSUserRatings(String dataFilePath, JavaSparkContext javaSparkContex) {
        super(dataFilePath, javaSparkContex);
    }

    @Override
    public JavaRDD createJavaRDD() {
        JavaRDD<String> data = super.dataFileToJavaRDD();
        // 加载我的评分数据
        JavaRDD<Rating> javaRDD = data.map(new Function<String, Rating>() {
            @Override
            public Rating call(String line) {
                String[] fields = line.split("::");
                if (fields.length != 4) {
                    throw new IllegalArgumentException("每一行必须有且只有4个元素");
                }
                int userId = Integer.parseInt(fields[0]);
                int productId = Integer.parseInt(fields[1]);
                double ratingScore = Float.parseFloat(fields[2]);
                LOGGER.debug(String.format("ALSUserRatings JavaRDD : userId ==> %d , productId ==> %d , rating ==> %f", userId, productId, ratingScore));
                Rating rating = new Rating(userId, productId, ratingScore);
                return rating;
            }
        });
        return javaRDD;
    }

    @Override
    public Dataset<UserProductRatings> javaRDDToDataSet(JavaRDD javaRDD) {
        // 将电影的id，标题，类型以三元组的形式保存
        JavaRDD<UserProductRatings> userProductJavaRDD = javaRDD.map(new Function<Rating, UserProductRatings>() {
            @Override
            public UserProductRatings call(Rating rating) {
                if (rating == null) {
                    throw new IllegalArgumentException("rating is null");
                }
                int userId = rating.user();
                int productId = rating.product();
                double ratingScore = rating.rating();
                LOGGER.debug(String.format("ALSUserRatings JavaRDDToDataSet : userId ==> %d , productId ==> %d , rating ==> %f", userId, productId, ratingScore));
                UserProductRatings userProductRatings = new UserProductRatings(userId, productId, ratingScore);
                return userProductRatings;
            }
        });
        SQLContext sqlContext = new SQLContext(super.getJavaSparkContex());
        Dataset result = sqlContext.createDataFrame(userProductJavaRDD, UserProductRatings.class);
        return result;
    }

    public static class UserProductRatings implements Serializable {

        private int userId;

        private int productId;

        private double ratingScore;

        public UserProductRatings(int userId, int productId, double ratingScore) {
            this.userId = userId;
            this.productId = productId;
            this.ratingScore = ratingScore;
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

        public double getRatingScore() {
            return ratingScore;
        }

        public void setRatingScore(double ratingScore) {
            this.ratingScore = ratingScore;
        }
    }
}