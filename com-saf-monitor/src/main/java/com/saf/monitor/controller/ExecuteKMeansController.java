package com.saf.monitor.controller;

import com.alibaba.fastjson.JSONObject;
import com.saf.core.base.BaseResponseVo;
import com.saf.core.common.utils.ObjectUtils;
import com.saf.mllib.core.common.constant.ConstantSparkTask;
import com.saf.mllib.kmeans.app.KMeans;
import com.saf.monitor.socket.entity.WebSocketResponseMessage;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.servlet.http.HttpSession;
import java.util.ArrayList;
import java.util.List;

@Controller
@RequestMapping("/mllib/kmeans")
public class ExecuteKMeansController extends AbstractParentExecuteController {

    @Override
    protected BaseResponseVo submit(HttpSession session, JSONObject jsonObject) {
        String mainClass = "com.saf.mllib." + mllibName() + ".app." + mllibName().toUpperCase();
        boolean localModel = false;
        if (jsonObject.containsKey("step_flag")) {
            String k = null;
            String maxIterations = null;
            String runs = null;
            if (jsonObject.getString("step_flag").equalsIgnoreCase("training")) {
                if (jsonObject.containsKey("local_model") && jsonObject.getBooleanValue("local_model")) {
                    localModel = true;
                }
                if (jsonObject.containsKey("kmeans_k_min") && jsonObject.containsKey("kmeans_k_max")) {
                    if (jsonObject.getString("kmeans_k_min").equalsIgnoreCase(jsonObject.getString("kmeans_k_max"))) {
                        k = jsonObject.getString("kmeans_k_min");
                    } else {
                        k = String.join(",", jsonObject.getString("kmeans_k_min"), jsonObject.getString("kmeans_k_max"));
                    }
                }
                if (jsonObject.containsKey("kmeans_maxIterations_min") && jsonObject.containsKey("kmeans_maxIterations_max")) {
                    if (jsonObject.getString("kmeans_maxIterations_min").equalsIgnoreCase(jsonObject.getString("kmeans_maxIterations_max"))) {
                        maxIterations = jsonObject.getString("kmeans_maxIterations_min");
                    } else {
                        maxIterations = String.join(",", jsonObject.getString("kmeans_maxIterations_min"), jsonObject.getString("kmeans_maxIterations_max"));
                    }
                }
                if (jsonObject.containsKey("kmeans_runs_min") && jsonObject.containsKey("kmeans_runs_max")) {
                    if ((jsonObject.getString("kmeans_runs_min").equalsIgnoreCase(jsonObject.getString("kmeans_runs_max")))
                            || jsonObject.getString("kmeans_runs_min").equalsIgnoreCase("0")) {
                        runs = jsonObject.getString("kmeans_runs_min");
                    } else {
                        runs = String.join(",", jsonObject.getString("kmeans_runs_min"), jsonObject.getString("kmeans_runs_max"));
                    }
                }
            } else if (jsonObject.getString("step_flag").equalsIgnoreCase("recommend")) {
                if (jsonObject.containsKey("recommend_local_model") && jsonObject.getBooleanValue("recommend_local_model")) {
                    localModel = true;
                }
                if (jsonObject.containsKey("kmeans_best_k")) {
                    k = jsonObject.getString("kmeans_best_k");
                }
                if (jsonObject.containsKey("kmeans_best_maxIterations")) {
                    maxIterations = jsonObject.getString("kmeans_best_maxIterations");
                }
                if (jsonObject.containsKey("kmeans_best_runs")) {
                    runs = jsonObject.getString("kmeans_best_runs");
                }
            }
            if (ObjectUtils.isEmpty(k) || ObjectUtils.isEmpty(maxIterations) || ObjectUtils.isEmpty(runs)) {
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
                    sessionStringValue(session, "kmeans_train_data"),
                    "random",
                    k,
                    maxIterations,
                    runs,
                    sessionStringValue(session, "kmeans_train_test_data"),
                    jsonObject.getString("step_flag")
            );
        }
        return fail("提交失败");
    }

    private BaseResponseVo submit(boolean localMode, String appName, String master, String sparkHome,
                                  String mainClass, String appResource, String hadoopConfDir,
                                  String javaHome, String hdfsPath, String trainFilePath, String initializationMode,
                                  String k, String maxIterations, String runs, String trainTestFilePath, String stepFlag) {
        JSONObject jsonObject = new JSONObject();
        if (super.existSparkTask(ConstantSparkTask.KMEANS_CURRENT_SUBMISSIONID)) {
            String submissionId = redisTemplate.opsForValue().get(ConstantSparkTask.KMEANS_CURRENT_SUBMISSIONID).toString();
            jsonObject.put(ConstantSparkTask.KMEANS_CURRENT_SUBMISSIONID, submissionId);
            jsonObject.put("exception", submissionId + " - 任务正在执行");
            return fail(1000, jsonObject.toJSONString());
        }

        try {
            List<String> args = new ArrayList<>();
            args.add(appName);
            args.add(master);
            args.add(hdfsPath);
            args.add(trainFilePath);
            args.add(k);
            args.add(maxIterations);
            args.add(runs);
            args.add(initializationMode);
            args.add("false");
            args.add(trainTestFilePath);
            args.add(stepFlag);
            String[] mainArgs = args.toArray(new String[]{});
            if (localMode) {
                mainArgs[1] = "local[1]";
                KMeans.main(mainArgs);
                webSocketService.sendMsg(new WebSocketResponseMessage(WebSocketResponseMessage.EWebSocketResponseMessageType.PROGRESS, WebSocketResponseMessage.EWebSocketResponseMessageFormat.NUMBER, "", 100l));
                webSocketService.sendMsg(new WebSocketResponseMessage(WebSocketResponseMessage.EWebSocketResponseMessageType.CONSOLE, WebSocketResponseMessage.EWebSocketResponseMessageFormat.STRING, "" + "_variance", "任务状态:FINISHED"));
                return success("提交成功");
            } else {
                return super.submit(mainArgs, hadoopConfDir, javaHome, appName, sparkHome, master, appResource, mainClass, ConstantSparkTask.KMEANS_CURRENT_SUBMISSIONID);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return fail("提交失败");
    }

    @Override
    protected String mllibName() {
        return "kmeans";
    }
}
