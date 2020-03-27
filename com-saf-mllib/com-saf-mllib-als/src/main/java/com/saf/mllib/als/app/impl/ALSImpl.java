package com.saf.mllib.als.app.impl;

import com.alibaba.fastjson.JSONObject;
import com.saf.mllib.als.app.entity.ALSProducts;
import com.saf.mllib.als.app.entity.ALSRatings;
import com.saf.mllib.als.app.entity.ALSUsers;
import com.saf.mllib.core.common.constant.ConstantSparkTask;
import com.saf.mllib.core.common.utils.RedisUtils;
import com.saf.mllib.core.entity.dto.WebSocketResponseMessageDto;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ALSImpl {

    private static final Logger LOGGER = Logger.getLogger(ALSImpl.class);

    public static String hdfsPath;

    /**
     * 方法：execute
     * 描述：TODO
     * 作者：赵增斌 E-mail:zhaozengbin@gmail.com QQ:4415599 weibo:http://weibo.com/zhaozengbin
     * 时间: 2018年07月20日 上午11:47
     * 参数：[numPartitions 分区数, ratingsJavaRDD 产品总体打数据, userRatedJavaRDD 用户打分数据,productJavaRDD 产品信息数据]
     * 返回: void
     */
    public static ExecuteResult recommondProductPre(final int numPartitions, final int userId,
                                                    final JavaRDD<Tuple2<Integer, Rating>> ratingsJavaRDD, final JavaRDD<Rating> userRatedJavaRDD, final JavaRDD<Tuple3<Integer, String, String>> productJavaRDD,
                                                    final List<Integer> ranks, final List<Double> lambdas, final List<Double> alphas, final List<Integer> iters, final List<Integer> proportion, final boolean isRecommend) {
        //将键值小于6（60%）的数据用于训练
        final JavaRDD<Rating> traningData_Rating = JavaPairRDD.fromJavaRDD(ratingsJavaRDD.filter(new Function<Tuple2<Integer, Rating>, Boolean>() {
            private static final long serialVersionUID = 4005833442740113567L;

            @Override
            public Boolean call(final Tuple2<Integer, Rating> v1) {
                final int end = proportion.get(0) / 10;
                return v1._1 < end;
            }
        })).values().union(userRatedJavaRDD).repartition(numPartitions).cache();

        //将键值大于6小于8（20%）的数据用于验证
        final JavaRDD<Rating> validateData_Rating = JavaPairRDD.fromJavaRDD(ratingsJavaRDD.filter(new Function<Tuple2<Integer, Rating>, Boolean>() {
            private static final long serialVersionUID = 7119412348542034306L;

            @Override
            public Boolean call(final Tuple2<Integer, Rating> v1) {
                final int start = proportion.get(0) / 10;
                final int end = (proportion.get(0) / 10) + (proportion.get(1) / 10);
                return v1._1 >= start && v1._1 < end;
            }
        })).values().repartition(numPartitions).cache();

        //将键值大于8（20%）的数据用于测试
        final JavaRDD<Rating> testData_Rating = JavaPairRDD.fromJavaRDD(ratingsJavaRDD.filter(new Function<Tuple2<Integer, Rating>, Boolean>() {
            private static final long serialVersionUID = 6083590621861360724L;

            @Override
            public Boolean call(final Tuple2<Integer, Rating> v1) {
                final int start = (proportion.get(0) / 10) + (proportion.get(1) / 10);
                return v1._1 >= start;
            }
        })).values().cache();
        LOGGER.info(String.format("training data's num : %d validate data's num : %d test data's num : %s", traningData_Rating.count(), validateData_Rating.count(), testData_Rating.count()));


        // 初始化最好的模型参数
        final ALSImpl.Variance variance = createMatrixFactorizationModel(traningData_Rating, validateData_Rating, testData_Rating, ranks, lambdas, alphas, iters, isRecommend);

        if (!isRecommend) {
            train(variance, traningData_Rating, validateData_Rating, testData_Rating);
            return new ExecuteResult(variance, null);
        } else {
            //训练数据
            //计算需要推荐的数据
            final JavaPairRDD<Integer, Integer> needRecommondList = getNeedRecommondProduct(userId, userRatedJavaRDD, productJavaRDD);
            return new ExecuteResult(variance, needRecommondList);
        }
    }

    private static ALSImpl.Variance createMatrixFactorizationModel(final JavaRDD<Rating> traningData_Rating, final JavaRDD<Rating> validateData_Rating, final JavaRDD<Rating> testData_Rating,
                                                                   final List<Integer> ranks, final List<Double> lambdas, List<Double> alphas, final List<Integer> iters, final boolean isRecommend) {
        MatrixFactorizationModel bestModel = null;
        // 为训练设置参数 每种参数设置2个值，三层for循环，一共进行8次训练
//        List<Integer> ranks = new ArrayList<>();
//        ranks.add(8);
//        ranks.add(22);
//
//        List<Double> lambdas = new ArrayList<>();
//        lambdas.add(0.1);
//        lambdas.add(10.0);
//
//        List<Integer> iters = new ArrayList<>();
//        iters.add(5);
//        iters.add(7);

        double bestValidateRnse = Double.MAX_VALUE;
        int bestRank = 0;
        double bestLambda = -1.0;
        int bestIter = -1;
        double bestAlphas = -1.0;

        if (ranks.size() == 1 && lambdas.size() == 1 && iters.size() == 1) {
            bestRank = ranks.get(0);
            bestLambda = lambdas.get(0);
            bestIter = iters.get(0);
            if (alphas.size() == 1 && alphas.get(0) > 0) {
                bestAlphas = alphas.get(0);
                bestModel = ALS.trainImplicit(JavaRDD.toRDD(traningData_Rating), bestRank, bestIter, bestLambda, bestAlphas);
            } else {
                bestModel = ALS.train(JavaRDD.toRDD(traningData_Rating), ranks.get(0), iters.get(0), lambdas.get(0));
            }
        }
        if (!isRecommend || bestModel == null) {
            if (alphas.get(0) <= 0) {
                alphas = new ArrayList<>();
                alphas.add(0.0);
            }
            for (int i = 0; i < ranks.size(); i++) {
                for (int j = 0; j < iters.size(); j++) {
                    for (int k = 0; k < lambdas.size(); k++) {
                        for (int l = 0; l < alphas.size(); l++) {
                            //训练获得模型
                            MatrixFactorizationModel model = null;
                            if (alphas.get(l) > 0) {
                                model = ALS.trainImplicit(JavaRDD.toRDD(traningData_Rating), ranks.get(i), iters.get(j), lambdas.get(k), alphas.get(l));
                            } else {
                                model = ALS.train(JavaRDD.toRDD(traningData_Rating), ranks.get(i), iters.get(j), lambdas.get(k));
                            }
                            //通过校验集validateData_Rating获取方差，以便查看此模型的好坏，方差方法定义在最下面
                            final double validateRnse = variance(model, validateData_Rating, validateData_Rating.count());
                            LOGGER.info(String.format("validation = %f for the model trained with rank = %d lambda = %f and numIter %d and alphas %f", validateRnse, ranks.get(i), lambdas.get(j), iters.get(k), alphas.get(l)));
                            final JSONObject jsonObject = new JSONObject();
                            jsonObject.put("submissionId", RedisUtils.getJedis().exists(ConstantSparkTask.ALS_CURRENT_SUBMISSIONID) ? RedisUtils.getJedis().get(ConstantSparkTask.ALS_CURRENT_SUBMISSIONID) : "");
                            jsonObject.put("rank", ranks.get(i));
                            jsonObject.put("iter", iters.get(j));
                            jsonObject.put("lambda", lambdas.get(k));
                            jsonObject.put("alpha", alphas.get(l));
                            jsonObject.put("dataRnse", validateRnse);
                            final WebSocketResponseMessageDto dto = new WebSocketResponseMessageDto(1, jsonObject);
                            RedisUtils.getJedis().publish("variance", JSONObject.toJSONString(dto));
                            //将最好的模型训练结果所设置的参数进行保存
                            if (validateRnse < bestValidateRnse) {
                                bestModel = model;
                                bestValidateRnse = validateRnse;
                                bestRank = ranks.get(i);
                                bestIter = iters.get(j);
                                bestLambda = lambdas.get(k);
                                bestAlphas = alphas.get(l);
                            }
                        }
                    }
                }
            }
        }
        //8次训练后获取最好的模型，根据最好的模型及训练集testData_Rating来获取此方差
        final double testDataRnse = variance(bestModel, testData_Rating, testData_Rating.count());
        LOGGER.info(String.format("the best model was trained with rank = %d and lambda = %f and numIter = %d and alpha = %f and Rnse on the test data is %f", bestRank, bestLambda, bestIter, bestAlphas, testDataRnse));

        if (!isRecommend) {
            final JSONObject jsonObject = new JSONObject();
            jsonObject.put("submissionId", RedisUtils.getJedis().exists(ConstantSparkTask.ALS_CURRENT_SUBMISSIONID) ? RedisUtils.getJedis().get(ConstantSparkTask.ALS_CURRENT_SUBMISSIONID) : "");
            jsonObject.put("rank", bestRank);
            jsonObject.put("lambda", bestLambda);
            jsonObject.put("iter", bestIter);
            jsonObject.put("alpha", bestAlphas);
            jsonObject.put("dataRnse", testDataRnse);
            jsonObject.put("isBest", true);
            final WebSocketResponseMessageDto dto = new WebSocketResponseMessageDto(1, jsonObject);
            RedisUtils.getJedis().publish("variance", JSONObject.toJSONString(dto));
        }
        final ALSImpl.Variance variance = new ALSImpl.Variance(bestModel, testDataRnse);
        return variance;
    }

    /**
     * 方法：train
     * 描述：数据训练 测试集的评分和实际评分之间的均方根误差
     * 作者：赵增斌 E-mail:zhaozengbin@gmail.com QQ:4415599 weibo:http://weibo.com/zhaozengbin
     * 时间: 2018年07月20日 下午3:23
     * 参数：[variance,traningData_Rating, validateData_Rating, testData_Rating]
     * 返回: void
     */

    private static void train(final ALSImpl.Variance
                                      variance, final JavaRDD<Rating> traningData_Rating, final JavaRDD<Rating> validateData_Rating, final JavaRDD<Rating> testData_Rating) {

        // 获取测试数据中，分数的平均值
        final double meanRating = traningData_Rating.union(validateData_Rating).mapToDouble(new DoubleFunction<Rating>() {
            private static final long serialVersionUID = -8123749556243711232L;

            @Override
            public double call(final Rating t) {
                return t.rating();
            }
        }).mean();

        // 根据平均值来计算旧的方差值
        final double baseLineRnse = Math.sqrt(testData_Rating.mapToDouble(new DoubleFunction<Rating>() {
            private static final long serialVersionUID = -4695196330753444514L;

            @Override
            public double call(final Rating t) {
                return (meanRating - t.rating()) * (meanRating - t.rating());
            }
        }).mean());

        // 通过模型，数据的拟合度提升了多少
        final double improvent = (baseLineRnse - variance.getVariance()) / baseLineRnse * 100;
        LOGGER.info(String.format("the best model improves the baseline by %f ", improvent) + "%");
    }


    // 1、获取我看过的产品 2、获取所有产品列表 3、组装为user product格式的二元祖
    private static JavaPairRDD<Integer, Integer> getNeedRecommondProduct(final int userId, final JavaRDD<
            Rating> userRatedJavaRDD, final JavaRDD<Tuple3<Integer, String, String>> productJavaRDD) {
        // 获取我所看过的产品ids
        final List<Integer> productIds = userRatedJavaRDD.map(new Function<Rating, Integer>() {
            private static final long serialVersionUID = 3675446530607274653L;

            @Override
            public Integer call(final Rating v1) {
                return v1.product();
            }
        }).collect();
        LOGGER.info("根据用户用户编号:" + userId + ",找到[" + productIds.size() + "]个关联的用户编号");

        // 从电影数据中去除我看过的电影数据
        final JavaRDD<Tuple3<Integer, String, String>> productIdList = productJavaRDD.filter(new Function<Tuple3<Integer, String, String>, Boolean>() {

            private static final long serialVersionUID = 271257018845869584L;

            @Override
            public Boolean call(final Tuple3<Integer, String, String> v1) {
                return !productIds.contains(v1._1());
            }
        });

        // 封装rating的参数形式，user为0，product为电影id进行封装
        final JavaPairRDD<Integer, Integer> recommondList = JavaPairRDD.fromJavaRDD(productIdList.map(new Function<Tuple3<Integer, String, String>, Tuple2<Integer, Integer>>() {

            private static final long serialVersionUID = 6332130295249306126L;

            @Override
            public Tuple2<Integer, Integer> call(final Tuple3<Integer, String, String> v1) {
                return new Tuple2<>(userId, v1._1());
            }
        }));
        return recommondList;
    }

    // 返回推荐数据
    public static List<ALSProducts.Products> recommondProduct(final ALSImpl.Variance
                                                                      variance, final JavaPairRDD<Integer, Integer> recommondList, final JavaRDD<Tuple3<Integer, String, String>> productJavaRDD, final int recommendNum) {
        //通过模型预测出user为0的各product(电影id)的评分，并按照评分进行排序，获取前10个电影id
        final List<Rating> tuple2List = variance.getMatrixFactorizationModel().predict(recommondList).sortBy(new Function<Rating, Double>() {
            private static final long serialVersionUID = -3869487271950732366L;

            @Override
            public Double call(final Rating v1) {
                return v1.rating();
            }
        }, false, 1).map(new Function<Rating, Rating>() {

            private static final long serialVersionUID = -7010953216040418045L;

            @Override
            public Rating call(final Rating v1) {
                return v1;
            }
        }).take(recommendNum);

        final List<Integer> list = new ArrayList<>();
        for (final Rating tuple2 : tuple2List) {
            list.add(tuple2.product());
        }

        if (list != null && !list.isEmpty()) {
            //从电影数据中过滤出这10部电影，遍历打印
            final List<Tuple3<Integer, String, String>> result = productJavaRDD.filter(new Function<Tuple3<Integer, String, String>, Boolean>() {
                private static final long serialVersionUID = -8776019628860810981L;

                @Override
                public Boolean call(final Tuple3<Integer, String, String> v1) {
                    return list.contains(v1._1());
                }
            }).collect();//.foreach(new VoidFunction<Tuple3<Integer, String, String>>() {
//                @Override
//                public void call(Tuple3<Integer, String, String> t) {
//                    LOGGER.info(String.format("product name --> %s ,product type --> %s", t._2(), t._3()));
//                    productList.add(new ALSProduct.Product(t._1(), t._2(), t._3()));
//                }
//            });
            final List<ALSProducts.Products> productList = new ArrayList<>();
            for (final Tuple3<Integer, String, String> t : result) {
                for (final Rating rating : tuple2List) {
                    if (t._1().intValue() == rating.product()) {
                        productList.add(new ALSProducts.Products(t._1(), t._2(), t._3(), rating.rating()));
                        break;
                    }
                }
            }
            return productList;
        }
        return null;
    }


    // 返回推荐数据
    public static Set<Integer> recommondUser
    (final Dataset<ALSRatings.Ratings> ratingsDataset, final Dataset<ALSProducts.Products> productsDataset, final Dataset<ALSUsers.Users> usersDataset) {
        //计算可能感兴趣的人
        final List<Row> results = ratingsDataset.filter(ratingsDataset.col("rating").geq(5))//过滤出评分列表中评分为5的记录
                .join(productsDataset, ratingsDataset.col("productId").equalTo(productsDataset.col("id")))//和电影DataFrame进行join操作
                .filter(productsDataset.col("type").equalTo("Drama"))//筛选出评分为5，且电影类型为Drama的记录（本来应该根据我的评分数据中电影的类型来进行筛选操作，由于数据格式的限制，这里草草的以一个Drama作为代表）
                .join(usersDataset, ratingsDataset.col("userId").equalTo(usersDataset.col("id")))//对用户DataFrame进行join
                .filter(usersDataset.col("age").equalTo(18))//筛选出年龄=18（和我的信息一致）的记录
                .filter(usersDataset.col("occupation").equalTo(15)) //筛选出工作类型=18（和我的信息一致）的记录
                .select(usersDataset.col("id"), usersDataset.col("age"))//只保存用户id，得到的结果为和我的个人信息差不多的，而且喜欢看的电影类型也和我差不多 的用户集合
                .takeAsList(10);
        final Set<Integer> intResult = new HashSet<>();
        for (final Row row : results) {
            intResult.add(row.getInt(0));
        }
        return intResult;
    }


    public static double getCollaborateSource(final JavaRDD<Tuple2<Integer, Rating>> userRatings,
                                              final ALSUsers.Users user1, final ALSUsers.Users user2) {
        // 过滤用户产品评分  符合当前用户的记录
        final JavaPairRDD<Integer, Double> user1JavaRDD = userRatings.filter(new Function<Tuple2<Integer, Rating>, Boolean>() {
            private static final long serialVersionUID = -8236557621453567393L;

            @Override
            public Boolean call(final Tuple2<Integer, Rating> tuple2) throws Exception {
                final boolean result = (tuple2._2.user() == user1.getId());
                return result;
            }
        }).mapToPair(new PairFunction<Tuple2<Integer, Rating>, Integer, Double>() {
            private static final long serialVersionUID = -6281951256533227538L;

            @Override
            public Tuple2<Integer, Double> call(final Tuple2<Integer, Rating> tuple2) throws Exception {
                return new Tuple2<>(tuple2._2().product(), tuple2._2().rating());
            }
        });

        // 过滤用户产品评分  符合对比用户的记录
        final JavaPairRDD<Integer, Double> user2JavaRDD = userRatings.filter(new Function<Tuple2<Integer, Rating>, Boolean>() {
            private static final long serialVersionUID = 5699740664048988759L;

            @Override
            public Boolean call(final Tuple2<Integer, Rating> tuple2) throws Exception {
                final boolean result = (tuple2._2.user() == user2.getId());
                return result;
            }
        }).mapToPair(new PairFunction<Tuple2<Integer, Rating>, Integer, Double>() {
            private static final long serialVersionUID = 3168224967671990394L;

            @Override
            public Tuple2<Integer, Double> call(final Tuple2<Integer, Rating> tuple2) throws Exception {
                return new Tuple2<>(tuple2._2().product(), tuple2._2().rating());
            }
        });

        // 获取当前用户产品评分记录与对比用户评分记录交集
        final JavaRDD<Integer> userKeysJavaRDD = user1JavaRDD.keys().intersection(user2JavaRDD.keys());
        final List<Integer> userKeys = userKeysJavaRDD.collect();

        // 只保留当前用户产品评分与对比用户产品评分有交集的 当前用户记录
        final JavaPairRDD<Integer, Double> user1JavaRDDNew = user1JavaRDD.filter(new Function<Tuple2<Integer, Double>, Boolean>() {
            private static final long serialVersionUID = -9075381436585054047L;

            @Override
            public Boolean call(final Tuple2<Integer, Double> integerDoubleTuple2) throws Exception {
                return userKeys.contains(integerDoubleTuple2._1());
            }
        }).sortByKey();

        // 只保留当前用户产品评分与对比用户产品评分有交集的 对比用户记录
        final JavaPairRDD<Integer, Double> user2JavaRDDNew = user2JavaRDD.filter(new Function<Tuple2<Integer, Double>, Boolean>() {
            private static final long serialVersionUID = -6234940740107925238L;

            @Override
            public Boolean call(final Tuple2<Integer, Double> integerDoubleTuple2) throws Exception {
                return userKeys.contains(integerDoubleTuple2._1());
            }
        }).sortByKey();
        final long user1JavaRDDAllCount = user1JavaRDD.count();
        final long user1JavaRDDNewCount = user1JavaRDDNew.count();
        final long user2JavaRDDNewCount = user2JavaRDDNew.count();

        // 判断如果当前用户记录与对比用户记录都不为0 且 数量一致则进行计算
        if (user1JavaRDDNewCount > 0 && user2JavaRDDNewCount > 0 && user1JavaRDDNewCount == user2JavaRDDNewCount) {
            try {
                //对公式部分分子进行计算
                final double member = user1JavaRDDNew.zip(user2JavaRDDNew).map(new Function<Tuple2<Tuple2<Integer, Double>, Tuple2<Integer, Double>>, Double>() {
                    private static final long serialVersionUID = 6501546832308004817L;

                    @Override
                    public Double call(final Tuple2<Tuple2<Integer, Double>, Tuple2<Integer, Double>> tuple2Tuple2Tuple2) throws Exception {
                        final double oneRating = tuple2Tuple2Tuple2._1()._2();
                        final double twoRating = tuple2Tuple2Tuple2._2()._2();
                        final double result = oneRating * twoRating;
                        ALSImpl.LOGGER.debug("对公式部分分子进行计算 map ==> " + oneRating + "*" + twoRating + "=" + result);
                        return result;
                    }
                }).reduce(new Function2<Double, Double, Double>() {
                    private static final long serialVersionUID = 7932371196400741930L;

                    @Override
                    public Double call(final Double aDouble, final Double aDouble2) throws Exception {
                        final double result = aDouble + aDouble2;
                        ALSImpl.LOGGER.debug("对公式部分分子进行计算 reduce ==> " + aDouble + "+" + aDouble2 + "=" + result);
                        return result;
                    }
                });
                //求出分母第一个变量值
                final double temp1Item = user1JavaRDDNew.map(new Function<Tuple2<Integer, Double>, Double>() {
                    private static final long serialVersionUID = 810701629754227066L;

                    @Override
                    public Double call(final Tuple2<Integer, Double> tuple2) throws Exception {
                        final double result = Math.pow(tuple2._2(), 2);
                        ALSImpl.LOGGER.debug("求出分母第一个变量值 map ==> Math.pow(" + tuple2._2() + ", 2) = " + result);
                        return result;
                    }
                }).reduce(new Function2<Double, Double, Double>() {
                    private static final long serialVersionUID = -135233303636540681L;

                    @Override
                    public Double call(final Double aDouble, final Double aDouble2) throws Exception {
                        final double result = aDouble + aDouble2;
                        ALSImpl.LOGGER.debug("求出分母第一个变量值 reduce ==> " + aDouble + "+" + aDouble2 + "=" + result);
                        return result;
                    }
                });
                final double temp1 = Math.sqrt(temp1Item);

                //求出分母第二个变量值
                final double temp2Item = user2JavaRDDNew.map(new Function<Tuple2<Integer, Double>, Double>() {
                    private static final long serialVersionUID = 7997748557687006573L;

                    @Override
                    public Double call(final Tuple2<Integer, Double> tuple2) throws Exception {
                        final double result = Math.pow(tuple2._2(), 2);
                        ALSImpl.LOGGER.debug("求出分母第二个变量值 map ==> Math.pow(" + tuple2._2() + ", 2) = " + result);
                        return result;
                    }
                }).reduce(new Function2<Double, Double, Double>() {
                    private static final long serialVersionUID = -5038287306569265955L;

                    @Override
                    public Double call(final Double aDouble, final Double aDouble2) throws Exception {
                        final double result = aDouble + aDouble2;
                        ALSImpl.LOGGER.debug("求出分母第二个变量值 reduce ==> " + aDouble + "+" + aDouble2 + "=" + result);
                        return result;
                    }
                });
                final double temp2 = Math.sqrt(temp2Item);

                final double denominator = temp1 * temp2;

                final double userDiffRate = 1 - (((double) user1JavaRDDNewCount / (double) user1JavaRDDAllCount) * 0.3d); // 差异率(交集数据/总记录数据)占比 * 30%（差异占比)
                LOGGER.info("userDiffRate : " + userDiffRate);
                return (member / denominator);//* userDiffRate;
            } catch (final Exception e) {
                e.getStackTrace();
            }
        }
        return 0.0d;
    }

    /**
     * 方法：variance
     * 描述：方差的计算方法<br>
     * 当数据分布比较分散（即数据在平均数附近波动较大）时，各个数据与平均数的差的平方和较大，方差就较大；当数据分布比较集中时，各个数据与平均数的差的平方和较小。因此方差越大，数据的波动越大；方差越小，数据的波动就越小。 [6]
     * 样本中各数据与样本平均数的差的平方和的平均数叫做样本方差；样本方差的算术平方根叫做样本标准差。样本方差和样本标准差都是衡量一个样本波动大小的量，样本方差或样本标准差越大，样本数据的波动就越大。
     * 方差和标准差是测算离散趋势最重要、最常用的指标。方差是各变量值与其均值离差平方的平均数，它是测算数值型数据离散程度的最重要的方法。标准差为方差的算术平方根，用S表示。
     * 作者：赵增斌 E-mail:zhaozengbin@gmail.com QQ:4415599 weibo:http://weibo.com/zhaozengbin
     * 时间: 2018年07月20日 下午1:34
     * 参数：[model, predictionData, n]
     * 返回: double
     */
    private static double variance(final MatrixFactorizationModel model, final JavaRDD<Rating> predictionData, final long n) {
        //将predictionData转化成二元组型式，以便训练使用
        final JavaRDD<Tuple2<Object, Object>> userProducts = predictionData.map(new Function<Rating, Tuple2<Object, Object>>() {
            private static final long serialVersionUID = -6483562250864068492L;

            @Override
            public Tuple2<Object, Object> call(final Rating r) {
                final Tuple2<Object, Object> tuple2 = new Tuple2<>(r.user(), r.product());
                return tuple2;
            }
        });

        //通过模型对数据进行预测
        final JavaPairRDD<Tuple2<Integer, Integer>, Double> prediction = JavaPairRDD.fromJavaRDD(model.predict(JavaRDD.toRDD(userProducts)).toJavaRDD().map(new Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>() {
            private static final long serialVersionUID = 5907258254849783378L;

            @Override
            public Tuple2<Tuple2<Integer, Integer>, Double> call(final Rating r) {
                ALSImpl.LOGGER.debug(String.format("通过模型对数据进行预测 user:%d,product:%d,rating:%f", r.user(), r.product(), r.rating()));
                final Tuple2<Tuple2<Integer, Integer>, Double> tuple2 = new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating());
                return tuple2;
            }
        }));

        //预测值和原值内连接
        final JavaRDD<Tuple2<Double, Double>> ratesAndPreds = JavaPairRDD.fromJavaRDD(predictionData.map(new Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>() {
            private static final long serialVersionUID = 803336669205970787L;

            @Override
            public Tuple2<Tuple2<Integer, Integer>, Double> call(final Rating r) {
                ALSImpl.LOGGER.debug(String.format("预测值和原值内连接 user:%d,product:%d,rating:%f", r.user(), r.product(), r.rating()));
                return new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating());
            }
        })).join(prediction).values();

        //计算方差并返回结果
        final Double dVar = ratesAndPreds.map(new Function<Tuple2<Double, Double>, Double>() {
            private static final long serialVersionUID = 5344280828069814339L;

            @Override
            public Double call(final Tuple2<Double, Double> v1) {
                final double result = (v1._1 - v1._2) * (v1._1 - v1._2);
                ALSImpl.LOGGER.debug(String.format("计算方差map值:%f", result));
                return result;
            }
        }).reduce(new Function2<Double, Double, Double>() {
            private static final long serialVersionUID = -6232600235691254064L;

            @Override
            public Double call(final Double v1, final Double v2) {
                final double result = v1 + v2;
                ALSImpl.LOGGER.debug(String.format("计算方差reduce值:%f", result));
                return result;
            }
        });
        return Math.sqrt(dVar / n);
    }


    public static class Variance {
        private MatrixFactorizationModel matrixFactorizationModel;

        private double variance;

        Variance(final MatrixFactorizationModel matrixFactorizationModel, final double variance) {
            this.matrixFactorizationModel = matrixFactorizationModel;
            this.variance = variance;
        }

        MatrixFactorizationModel getMatrixFactorizationModel() {
            return this.matrixFactorizationModel;
        }

        public void setMatrixFactorizationModel(final MatrixFactorizationModel matrixFactorizationModel) {
            this.matrixFactorizationModel = matrixFactorizationModel;
        }

        double getVariance() {
            return this.variance;
        }

        public void setVariance(final double variance) {
            this.variance = variance;
        }
    }

    public static class ExecuteResult {
        //推荐模型
        private ALSImpl.Variance variance;

        // 待推荐的电影列表
        private JavaPairRDD<Integer, Integer> recommondList;

        private JavaRDD<Tuple3<Integer, String, String>> productJavaRDD;

        ExecuteResult(final Variance variance, final JavaPairRDD<Integer, Integer> recommondList) {
            this.variance = variance;
            this.recommondList = recommondList;
        }

        public Variance getVariance() {
            return this.variance;
        }

        public void setVariance(final Variance variance) {
            this.variance = variance;
        }

        public JavaPairRDD<Integer, Integer> getRecommondList() {
            return this.recommondList;
        }

        public void setRecommondList(final JavaPairRDD<Integer, Integer> recommondList) {
            this.recommondList = recommondList;
        }

    }
}
