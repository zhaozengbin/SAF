package com.saf.monitor.controller;

import com.alibaba.fastjson.JSONObject;
import com.saf.core.base.BaseResponseVo;
import com.saf.core.common.utils.ObjectUtils;
import com.saf.mllib.als.app.ALS;
import com.saf.mllib.core.common.constant.ConstantSparkTask;
import com.saf.monitor.socket.entity.WebSocketResponseMessage;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.servlet.http.HttpSession;
import java.util.ArrayList;
import java.util.List;

@Controller
@RequestMapping("/mllib/als")
public class ExecuteALSController extends AbstractParentExecuteController {

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
        if (jsonObject.containsKey("step_flag")) {
            String rank = null;
            String lambda = null;
            String alpha = null;
            String iter = null;
            String proportion = null;
            if (jsonObject.getString("step_flag").equalsIgnoreCase("training")) {
                if (jsonObject.containsKey("local_model") && jsonObject.getBooleanValue("local_model")) {
                    localModel = true;
                }
                if (jsonObject.containsKey("rank_min") && jsonObject.containsKey("rank_max")) {
                    if (jsonObject.getString("rank_min").equalsIgnoreCase(jsonObject.getString("rank_max"))) {
                        rank = jsonObject.getString("rank_min");
                    } else {
                        rank = String.join(",", jsonObject.getString("rank_min"), jsonObject.getString("rank_max"));
                    }
                }
                if (jsonObject.containsKey("lambda_min") && jsonObject.containsKey("lambda_max")) {
                    if (jsonObject.getString("lambda_min").equalsIgnoreCase(jsonObject.getString("lambda_max"))) {
                        lambda = jsonObject.getString("lambda_min");
                    } else {
                        lambda = String.join(",", jsonObject.getString("lambda_min"), jsonObject.getString("lambda_max"));
                    }
                }
                if (jsonObject.containsKey("alpha_min") && jsonObject.containsKey("alpha_max")) {
                    if ((jsonObject.getString("alpha_min").equalsIgnoreCase(jsonObject.getString("alpha_max")))
                            || jsonObject.getString("alpha_min").equalsIgnoreCase("0")) {
                        alpha = jsonObject.getString("alpha_min");
                    } else {
                        alpha = String.join(",", jsonObject.getString("alpha_min"), jsonObject.getString("alpha_max"));
                    }
                }
                if (jsonObject.containsKey("iter_min") && jsonObject.containsKey("iter_max")) {
                    if (jsonObject.getString("iter_min").equalsIgnoreCase(jsonObject.getString("iter_max"))) {
                        iter = jsonObject.getString("iter_min");
                    } else {
                        iter = String.join(",", jsonObject.getString("iter_min"), jsonObject.getString("iter_max"));
                    }
                }
            } else if (jsonObject.getString("step_flag").equalsIgnoreCase("recommend")) {
                if (jsonObject.containsKey("recommend_local_model") && jsonObject.getBooleanValue("recommend_local_model")) {
                    localModel = true;
                }
                if (jsonObject.containsKey("best_rank")) {
                    rank = jsonObject.getString("best_rank");
                }
                if (jsonObject.containsKey("best_lambda")) {
                    lambda = jsonObject.getString("best_lambda");
                }
                if (jsonObject.containsKey("best_alpha")) {
                    alpha = jsonObject.getString("best_alpha");
                }
                if (jsonObject.containsKey("best_iter")) {
                    iter = jsonObject.getString("best_iter");
                }
            }
            if (jsonObject.containsKey("training_proportion") && jsonObject.containsKey("validata_proportion") && jsonObject.containsKey("test_proportion")) {
                proportion = String.join(",", jsonObject.getString("training_proportion"),
                        jsonObject.getString("validata_proportion"),
                        jsonObject.getString("test_proportion"));
            }
            if (ObjectUtils.isEmpty(rank) || ObjectUtils.isEmpty(lambda) || ObjectUtils.isEmpty(alpha) || ObjectUtils.isEmpty(iter) || ObjectUtils.isEmpty(proportion)) {
                return fail("必传参数为空");
            }

            return submit(localModel,
                    appName(session),
                    master(session),
                    sparkHome(session),
                    mainClass,
                    appResource(session),
                    hadoopConfPath(session),
                    javaHome(session),
                    hdfsPath(session),
                    sessionStringValue(session, "user_data"),
                    sessionStringValue(session, "product_data"),
                    sessionStringValue(session, "ratings_data"),
                    sessionStringValue(session, "personalRatings_data"),
                    3,
                    userId,
                    rank,
                    lambda,
                    alpha,
                    iter,
                    proportion,
                    jsonObject.getString("step_flag"),
                    false,
                    recommendNum
            );
        }
        return fail("提交失败");
    }

//    public static void main(String[] args) {
//        submit(true, "ALS", "spark://127.0.0.1:6066", "/usr/local/Cellar/apache-spark/2.3.1/libexec", "com.saf.mllib.als.app.ALS", "/Users/zhaozengbin/git/com-saf-parent/com-saf-mllib/com-saf-mllib-als/target/com-saf-mllib-als-1.0-SNAPSHOT.jar",
//                "/usr/local/Cellar/hadoop/x.x.x/libexec/etc/hadoop", "/Library/Java/JavaVirtualMachines/jdk1.8.0_151.jdk/Contents/Home", "hdfs://localhost:9000",usersPath, productPath, ratingsPath, usersProductRatingsPath);
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
                webSocketService.sendMsg(new WebSocketResponseMessage(WebSocketResponseMessage.EWebSocketResponseMessageType.PROGRESS, WebSocketResponseMessage.EWebSocketResponseMessageFormat.NUMBER, "", 100l));
                webSocketService.sendMsg(new WebSocketResponseMessage(WebSocketResponseMessage.EWebSocketResponseMessageType.CONSOLE, WebSocketResponseMessage.EWebSocketResponseMessageFormat.STRING, "" + "_variance", "任务状态:FINISHED"));
                return success("提交成功");
            } else {
                return super.submit(mainArgs, hadoopConfDir, javaHome, appName, sparkHome, master, appResource, mainClass,
                        ConstantSparkTask.ALS_CURRENT_SUBMISSIONID);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return fail("提交失败");
    }

    @Override
    protected String mllibName() {
        return "als";
    }

}
