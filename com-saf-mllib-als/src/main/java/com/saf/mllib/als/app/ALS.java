package com.saf.mllib.als.app;

import com.alibaba.fastjson.JSONObject;
import com.saf.core.common.utils.ObjectUtils;
import com.saf.mllib.als.app.entity.ALSProducts;
import com.saf.mllib.als.app.entity.ALSRatings;
import com.saf.mllib.als.app.entity.ALSUserRatings;
import com.saf.mllib.als.app.entity.ALSUsers;
import com.saf.mllib.als.app.impl.ALSImpl;
import com.saf.mllib.core.common.constant.ConstantSparkTask;
import com.saf.mllib.core.common.utils.RedisUtils;
import com.saf.mllib.core.entity.dto.WebSocketResponseMessageDto;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.mortbay.util.ajax.JSON;
import scala.Serializable;
import scala.Tuple4;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ALS implements Serializable {
    private static final Logger LOGGER = Logger.getLogger(ALS.class);

    private static ALS instance;

    private String sparkAppName;

    private String sparkMaster;

    private String ratingFilePath;

    private String productFilePath;

    private String userProductFilePath;

    private String userFilePath;

    static {
        //屏蔽日志，由于结果是打印在控制台上的，为了方便查看结果，将spark日志输出关掉
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF);
    }

    public ALS() {
    }

    /**
     * 方法：ALS
     * 描述：TODO
     * 作者：赵增斌 E-mail:zhaozengbin@gmail.com QQ:4415599 weibo:http://weibo.com/zhaozengbin
     * 时间: 2018年07月20日 上午10:45
     * 参数：[sparkAppName, sparkMaster]
     * 返回:
     */
    public ALS(String sparkAppName, String sparkMaster) {

        this.sparkAppName = sparkAppName;
        this.sparkMaster = sparkMaster;
    }

    public ALS(String sparkAppName, String sparkMaster, String userFilePath, String productFilePath, String ratingFilePath, String userProductFilePath) {
        this.sparkAppName = sparkAppName;
        this.sparkMaster = sparkMaster;
        this.userFilePath = userFilePath;
        this.productFilePath = productFilePath;
        this.ratingFilePath = ratingFilePath;
        this.userProductFilePath = userProductFilePath;
    }

    /**
     * 方法：getInstance
     * 描述：TODO
     * 作者：赵增斌 E-mail:zhaozengbin@gmail.com QQ:4415599 weibo:http://weibo.com/zhaozengbin
     * 时间: 2018年07月20日 上午10:46
     * 参数：[sparkAppName, sparkMaster]
     * 返回: com.duia.spark.mllib.als.ALS
     */
    public static synchronized ALS getInstance(String sparkAppName, String sparkMaster) {

        if (instance == null) {
            instance = new ALS(sparkAppName, sparkMaster);
        }
        return instance;
    }

    public static synchronized ALS getInstance(String sparkAppName, String sparkMaster, String userFilePath, String productFilePath, String ratingFilePath, String userProductFilePath) {

        if (instance == null) {
            instance = new ALS(sparkAppName, sparkMaster, userFilePath, productFilePath, ratingFilePath, userProductFilePath);
        }
        return instance;
    }

    public void execute(String hdfsPath, int numPartitions, int userId, List<Integer> ranks, List<Double> lambdas, List<Double> alphas, List<Integer> iters,
                        List<Integer> proportion, String stepFlag, boolean isRecommerUsers, int recommendNum) {
        // 创建入口对象
        if (ratingFilePath == null || productFilePath == null || userFilePath == null || userProductFilePath == null) {
            return;
        }

        SparkConf conf = new SparkConf().setAppName(this.getSparkAppName());
        if (this.getSparkMaster() != null && !this.getSparkMaster().startsWith("spark://")) {
            conf.setMaster(this.getSparkMaster());
            conf.set("spark.driver.allowMultipleContexts", "true");
        }
        JavaSparkContext sc = new JavaSparkContext(conf);

        ALSRatings alsRatings = new ALSRatings(ratingFilePath, sc);
        JavaRDD ratingsJavaRDD = alsRatings.createJavaRDD();

        ALSProducts alsProductRatings = new ALSProducts(productFilePath, sc);
        JavaRDD productJavaRDD = alsProductRatings.createJavaRDD();

        ALSUserRatings alsUserRatings = new ALSUserRatings(userProductFilePath, sc);
        JavaRDD userRatingsJavaRDD = alsUserRatings.createJavaRDD();
//        Dataset<ALSUserRatings.UserProductRatings> userProductRatingsDataset = alsUserRatings.javaRDDToDataSet(userRatingsJavaRDD);

        ALSUsers alsUsers = new ALSUsers(userFilePath, sc);
        JavaRDD userJavaRDD = alsUsers.createJavaRDD();
        ALSImpl.hdfsPath = hdfsPath;
        boolean isRecommend = stepFlag.equalsIgnoreCase("recommend");
        ALSImpl.ExecuteResult executeResult = ALSImpl.recommondProductPre(numPartitions, userId, ratingsJavaRDD, userRatingsJavaRDD, productJavaRDD, ranks, lambdas, alphas, iters, proportion, isRecommend);
        if (isRecommend) {
            List<ALSProducts.Products> productsList = ALSImpl.recommondProduct(executeResult.getVariance(), executeResult.getRecommondList(), productJavaRDD, recommendNum);
            Set<ALSProducts.Products> productsSet = new HashSet<>(productsList);
            for (ALSProducts.Products products : productsSet) {
                WebSocketResponseMessageDto dto = new WebSocketResponseMessageDto(2, products);
                String msg = JSONObject.toJSONString(dto);
                LOGGER.info("根据计算模型获得的推荐:" + msg);
                RedisUtils.getJedis().publish("variance", msg);
            }
        }
        if (isRecommerUsers) {
            Dataset<ALSRatings.Ratings> ratingsDataset = alsRatings.javaRDDToDataSet(ratingsJavaRDD);
            Dataset<ALSProducts.Products> productsDataset = alsProductRatings.javaRDDToDataSet(productJavaRDD);
            Dataset<ALSUsers.Users> usersDataset = alsUsers.javaRDDToDataSet(userJavaRDD);

            Set<Integer> set = ALSImpl.recommondUser(ratingsDataset, productsDataset, usersDataset);
            System.out.println(JSON.toString(set.toArray()));

            ALSUsers.Users users = new ALSUsers.Users(6, 50, 9);
            Double result = ALSImpl.getCollaborateSource(ratingsJavaRDD,
                    new ALSUsers.Users(6, 50, 9), users);

            System.out.println(6 + "==与==" + users.getId() + "相似度为： ===>" + result);

            List<Tuple4<Integer, String, Integer, Integer>> userList = userJavaRDD.collect();

            for (Tuple4<Integer, String, Integer, Integer> item : userList) {
                ALSUsers.Users user = new ALSUsers.Users(item._1(), item._3(), item._4());
                Double rating = ALSImpl.getCollaborateSource(ratingsJavaRDD,
                        new ALSUsers.Users(6, 50, 9), user);
                System.out.println(item._1() + "==与==" + users.getId() + "相似度为： ===>" + rating);
            }
        }
        sc.stop();
        sc.close();
        if (RedisUtils.getJedis().exists(ConstantSparkTask.ALS_CURRENT_SUBMISSIONID)) {
            String jobStatus = RedisUtils.getJedis().get(ConstantSparkTask.ALS_CURRENT_SUBMISSIONID);
            if (jobStatus.startsWith("\"") && jobStatus.endsWith("\"")) {
                jobStatus = jobStatus.substring(jobStatus.indexOf("\"") + 1, jobStatus.lastIndexOf("\""));
            }
            RedisUtils.getJedis().set(jobStatus + "_status", "finished");
        }
        System.out.println("任务结束");
    }

    public static void main(String[] args) {
        if (args.length == 17) {
            String sparkAppName = args[0];
            String sparkMaster = args[1];
            String hdfsPath = args[2];
            String userFilePath = args[3];
            String productFilePath = args[4];
            String ratingFilePath = args[5];
            String userProductFilePath = args[6];
            String numPartitions = args[7];
            String userId = args[8];
            String ranks = args[9];
            String lambdas = args[10];
            String alphas = args[11];
            String iters = args[12];
            String proportion = args[13];
            String stepFlag = args[14];
            String isRecommerUsers = args[15];
            String recommendNum = args[16];

            int numPartitionsInt = 0;
            int userIdInt = 0;
            int recommendNumInt = 10;
            boolean isRecommerUsersBoolean = false;
            if (ObjectUtils.isNumber(numPartitions)) {
                numPartitionsInt = Integer.parseInt(numPartitions);
            }
            if (ObjectUtils.isNumber(userId)) {
                userIdInt = Integer.parseInt(userId);
            }
            if (ObjectUtils.isNotEmpty(isRecommerUsers)) {
                isRecommerUsersBoolean = Boolean.parseBoolean(isRecommerUsers);
            }

            if (ObjectUtils.isNotEmpty(recommendNum)) {
                recommendNumInt = Integer.parseInt(recommendNum);
            }
            if (numPartitionsInt > 0) {
                List<Integer> ranksList = ObjectUtils.string2IntegerList(ranks, ",");
                List<Double> lambdasList = ObjectUtils.string2DoubleList(lambdas, ",");
                List<Double> alphasList = ObjectUtils.string2DoubleList(alphas, ",");
                List<Integer> itersList = ObjectUtils.string2IntegerList(iters, ",");
                List<Integer> proportionList = ObjectUtils.string2IntegerList(proportion, ",");


                if (ObjectUtils.isNotEmpty(hdfsPath)) {
                    userFilePath = hdfsPath + userFilePath;
                    productFilePath = hdfsPath + productFilePath;
                    ratingFilePath = hdfsPath + ratingFilePath;
                    userProductFilePath = hdfsPath + userProductFilePath;
                }
                ALS als = ALS.getInstance(sparkAppName, sparkMaster, userFilePath, productFilePath, ratingFilePath, userProductFilePath);
                als.execute(hdfsPath, numPartitionsInt, userIdInt, ranksList, lambdasList, alphasList, itersList, proportionList, stepFlag, isRecommerUsersBoolean, recommendNumInt);
            }
        }
    }

    public String getSparkAppName() {
        return sparkAppName;
    }


    public String getSparkMaster() {
        return sparkMaster;
    }
}