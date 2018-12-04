package com.saf.monitor.controller;

import com.alibaba.fastjson.JSONObject;
import com.saf.core.base.BaseResponseVo;
import com.saf.core.common.utils.ObjectUtils;
import com.saf.core.controller.AbstractExecuteController;
import com.saf.mllib.als.app.ALS;
import com.saf.mllib.core.common.constant.ConstantSparkTask;
import com.saf.mllib.core.common.utils.SparkUtils;
import com.saf.monitor.socket.service.WebSocketService;
import com.saf.monitor.thread.MonitorRunable;
import org.apache.spark.launcher.SparkLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.servlet.http.HttpSession;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Controller
@RequestMapping("/mllib/als")
public class ExecuteController extends AbstractExecuteController {

    @Autowired
    private WebSocketService webSocketService;

    @Override
    protected BaseResponseVo submit(HttpSession session, JSONObject jsonObject) {
        String mainClass = "com.saf.mllib." + mllibName() + ".app." + mllibName().toUpperCase();
        boolean localModel = false;
        int userId = 0;
        int recommendNum = 10;
        if (jsonObject.containsKey("user_id")) {
            userId = jsonObject.getIntValue("user_id");
        }
        if (jsonObject.containsKey("recommend_num")) {
            recommendNum = jsonObject.getIntValue("recommend_num");
        }
        if (jsonObject.containsKey("als_step_flag")) {
            String rank = null;
            String lambda = null;
            String alpha = null;
            String iter = null;
            String proportion = null;
            if (jsonObject.getString("als_step_flag").equalsIgnoreCase("training")) {
                if (jsonObject.containsKey("local_model") && jsonObject.getBoolean("local_model")) {
                    localModel = true;
                }
                if (jsonObject.containsKey("als_rank_min") && jsonObject.containsKey("als_rank_max")) {
                    if (jsonObject.getString("als_rank_min").equalsIgnoreCase(jsonObject.getString("als_rank_max"))) {
                        rank = jsonObject.getString("als_rank_min");
                    } else {
                        rank = String.join(",", jsonObject.getString("als_rank_min"), jsonObject.getString("als_rank_max"));
                    }
                }
                if (jsonObject.containsKey("als_lambda_min") && jsonObject.containsKey("als_lambda_max")) {
                    if (jsonObject.getString("als_lambda_min").equalsIgnoreCase(jsonObject.getString("als_lambda_max"))) {
                        lambda = jsonObject.getString("als_lambda_min");
                    } else {
                        lambda = String.join(",", jsonObject.getString("als_lambda_min"), jsonObject.getString("als_lambda_max"));
                    }
                }
                if (jsonObject.containsKey("als_alpha_min") && jsonObject.containsKey("als_alpha_max")) {
                    if ((jsonObject.getString("als_alpha_min").equalsIgnoreCase(jsonObject.getString("als_alpha_max")))
                            || jsonObject.getString("als_alpha_min").equalsIgnoreCase("0")) {
                        alpha = jsonObject.getString("als_alpha_min");
                    } else {
                        alpha = String.join(",", jsonObject.getString("als_alpha_min"), jsonObject.getString("als_alpha_max"));
                    }
                }
                if (jsonObject.containsKey("als_iter_min") && jsonObject.containsKey("als_iter_max")) {
                    if (jsonObject.getString("als_iter_min").equalsIgnoreCase(jsonObject.getString("als_iter_max"))) {
                        iter = jsonObject.getString("als_iter_min");
                    } else {
                        iter = String.join(",", jsonObject.getString("als_iter_min"), jsonObject.getString("als_iter_max"));
                    }
                }
            } else if (jsonObject.getString("als_step_flag").equalsIgnoreCase("recommend")) {
                if (jsonObject.containsKey("recommend_local_model") && jsonObject.getBoolean("recommend_local_model")) {
                    localModel = true;
                }
                if (jsonObject.containsKey("als_best_rank")) {
                    rank = jsonObject.getString("als_best_rank");
                }
                if (jsonObject.containsKey("als_best_lambda")) {
                    lambda = jsonObject.getString("als_best_lambda");
                }
                if (jsonObject.containsKey("als_best_alpha")) {
                    alpha = jsonObject.getString("als_best_alpha");
                }
                if (jsonObject.containsKey("als_best_iter")) {
                    iter = jsonObject.getString("als_best_iter");
                }
            }
            if (jsonObject.containsKey("als_training_proportion") && jsonObject.containsKey("als_validata_proportion") && jsonObject.containsKey("als_test_proportion")) {
                proportion = String.join(",", jsonObject.getString("als_training_proportion"),
                        jsonObject.getString("als_validata_proportion"),
                        jsonObject.getString("als_test_proportion"));
            }
            if (ObjectUtils.isEmpty(rank) || ObjectUtils.isEmpty(lambda) || ObjectUtils.isEmpty(alpha) || ObjectUtils.isEmpty(iter) || ObjectUtils.isEmpty(proportion)) {
                return fail("必传参数为空");
            }

            return submit(localModel, appName(session),
                    master(session),
                    sparkHome(session),
                    mainClass,
                    appResource(session),
                    hadoopConfPath(session),
                    javaHome(session),
                    hdfsPath(session),
                    sessionStringValue(session, "als_user_data"),
                    sessionStringValue(session, "als_product_data"),
                    sessionStringValue(session, "als_ratings_data"),
                    sessionStringValue(session, "als_personalRatings_data"),
                    3,
                    userId,
                    rank,
                    lambda,
                    alpha,
                    iter,
                    proportion,
                    jsonObject.getString("als_step_flag"),
                    false,
                    recommendNum
            );
        }
        return fail("提交失败");
    }

//    public static void main(String[] args) {
//        submit(true, "ALS", "spark://127.0.0.1:6066", "/usr/local/Cellar/apache-spark/2.3.1/libexec", "com.saf.mllib.als.app.ALS", "/Users/zhaozengbin/git/com-saf-parent/com-saf-mllib-als/target/com-saf-mllib-als-1.0-SNAPSHOT.jar",
//                "/usr/local/Cellar/hadoop/3.1.0/libexec/etc/hadoop", "/Library/Java/JavaVirtualMachines/jdk1.8.0_151.jdk/Contents/Home", "hdfs://localhost:9000",usersPath, productPath, ratingsPath, usersProductRatingsPath);
//    }

    private BaseResponseVo submit(boolean localMode, String appName, String master, String sparkHome,
                                  String mainClass, String appResource, String hadoopConfDir,
                                  String javaHome, String hdfsPath, String userFilePath, String productFilePath,
                                  String ratingFilePath, String userProductFilePath, int numPartitions, int userId,
                                  String ranks, String lambdas, String alphas, String iters, String proportion, String stepFlag, boolean isRecommerUsers, int recommendNum) {
        JSONObject jsonObject = new JSONObject();
        if (super.existSparkTask(ConstantSparkTask.ALS_CURRENT_SUBMISSIONID)) {
            String submissionId = redisTemplate.opsForValue().get(ConstantSparkTask.ALS_CURRENT_SUBMISSIONID).toString();
            jsonObject.put(ConstantSparkTask.ALS_CURRENT_SUBMISSIONID, submissionId);
            jsonObject.put("exception", submissionId + " - 任务正在执行");
            return fail(1000, jsonObject.toJSONString());
        }
        try {
            List<String> args = new ArrayList<>();
            args.add(appName);
            args.add(master);
            args.add(hdfsPath);
            args.add(userFilePath);
            args.add(productFilePath);
            args.add(ratingFilePath);
            args.add(userProductFilePath);
            args.add(String.valueOf(numPartitions));
            args.add(String.valueOf(userId));
            args.add(ranks);
            args.add(lambdas);
            args.add(alphas);
            args.add(iters);
            args.add(proportion);
            args.add(stepFlag);
            args.add(String.valueOf(isRecommerUsers));
            args.add(String.valueOf(recommendNum));
            String[] mainArgs = args.toArray(new String[]{});
            if (localMode) {
                mainArgs[1] = "local[1]";
                ALS.main(mainArgs);
                return success("提交成功");
            } else {
                Map<String, String> env = new HashMap<>();
                //这两个属性必须设置
                // /usr/local/Cellar/hadoop/3.1.0/libexec/etc/hadoop
                env.put("HADOOP_CONF_DIR", hadoopConfDir);
                // /Library/Java/JavaVirtualMachines/jdk1.8.0_151.jdk/Contents/Home
                env.put("JAVA_HOME", javaHome);
                Process process = new SparkLauncher(env)
                        .setAppName(appName)
                        .setSparkHome(sparkHome)
                        .setMaster(master)
                        .setConf("spark.driver.memory", "2g")
                        .setConf("spark.executor.memory", "1g")
                        .setConf("spark.executor.cores", "3")
                        .setAppResource(appResource)
                        .setMainClass(mainClass)
                        .setJavaHome(javaHome)
                        .addAppArgs(mainArgs)
                        .setDeployMode("cluster")//cluster
                        .launch();
//                InputStreamReaderRunnable inputStreamReaderRunnable = new InputStreamReaderRunnable(process.getInputStream(), "input");
//                Thread inputThread = new Thread(inputStreamReaderRunnable, "LogStreamReader input");
//                inputThread.start();
//                InputStreamReaderRunnable errorStreamReaderRunnable = new InputStreamReaderRunnable(process.getErrorStream(), "error");
//                Thread errorThread = new Thread(errorStreamReaderRunnable, "LogStreamReader error");
//                errorThread.start();
                SparkUtils.SparkResult sparkResult = SparkUtils.inputStreamReade(process);
                super.addSparkTask(ConstantSparkTask.ALS_CURRENT_SUBMISSIONID, sparkResult.getSubmissionId());
                System.out.println("Waiting for finish...");
                int exitCode = process.waitFor();
                System.out.println("Finished! Exit code:" + exitCode);
                new Thread(new MonitorRunable(ConstantSparkTask.ALS_CURRENT_SUBMISSIONID, sparkResult.getSubmissionId(), master, webSocketService)).start();
                if (sparkResult.isSuccess()) {
                    String submissionId = redisTemplate.opsForValue().get(ConstantSparkTask.ALS_CURRENT_SUBMISSIONID).toString();
                    jsonObject.put(ConstantSparkTask.ALS_CURRENT_SUBMISSIONID, submissionId);
                    jsonObject.put("msg", "提交成功");
                    return success(jsonObject.toJSONString());
                } else {
                    return fail("提交失败");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return fail("提交失败");
    }
}
