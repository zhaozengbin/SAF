package com.saf.monitor.thread;

import com.alibaba.fastjson.JSONObject;
import com.saf.core.common.utils.ObjectUtils;
import com.saf.mllib.core.common.constant.ConstantSparkTask;
import com.saf.mllib.core.common.utils.HttpUtils;
import com.saf.mllib.core.common.utils.RedisUtils;
import com.saf.mllib.core.common.utils.SparkUtils;
import com.saf.monitor.socket.entity.WebSocketResponseMessage;
import com.saf.monitor.socket.service.WebSocketService;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.log4j.Logger;
import org.springframework.data.redis.core.RedisTemplate;
import redis.clients.jedis.Jedis;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class MonitorRunable implements Runnable {
    private Logger log = Logger.getLogger(getClass());
    private static final String REPORT_URL = "/v1/submissions/status/%s";
    private static final String KILL_URL = "/v1/submissions/kill/%s";
    private final int runProgress = 99;
    private final int acceptedProgress = 20;

    // 允许多用户提交spark任务 TODO 还应解决模型输出目录问题
    private static ConcurrentHashMap<String, String> allAppStatus = new ConcurrentHashMap<>();

    private WebSocketService webSocketService;
    private String submissionId;
    private String submissionKey;
    private boolean isMonitor = false;
    private String reportUrl;
    private String killUrl;

    public MonitorRunable() {
    }

    //http://localhost:6066/v1/submissions/status/driver-20180907161919-0039
    public MonitorRunable(String submissionKey, String submissionId, String sparkPath, WebSocketService webSocketService) {
        if (sparkPath.contains("spark://")) {
            this.submissionKey = submissionKey;
            this.submissionId = submissionId;
            this.webSocketService = webSocketService;
            sparkPath = sparkPath.replace("spark://", "http://");
            this.reportUrl = sparkPath + String.format(REPORT_URL, submissionId);
            this.killUrl = sparkPath + String.format(KILL_URL, submissionId);
            this.isMonitor = true;
        }

    }

    @Override
    public void run() {
        if (isMonitor) {
            long interval = 1000;// 更新Application 状态间隔
            int count = 3; // 时间
            while (true) {
                try {
                    Jedis jedis = RedisUtils.getJedis();
                    Thread.sleep(interval);

                    // 检测是否有结束标识 如果有结束标识则杀掉运行进程
                    String jobStatus = submissionId + "_status";
                    if (jedis.exists(jobStatus) && jedis.get(jobStatus).contains("finished")) {
                        String response = HttpUtils.httpPost(this.killUrl, null);
                        System.out.println(response);
                    }

                    SparkUtils.SparkReport sparkReport = response();
                    if (ObjectUtils.isEmpty(sparkReport)) {
                        return;
                    }
                    String state = sparkReport.getDriverState();
                    log.info("Thread:" + Thread.currentThread().getName() + " - " + submissionId + "，任务状态是：" + state);
                    // 完成/ 失败/杀死
                    if (state.equalsIgnoreCase(YarnApplicationState.FINISHED.name()) || state.equalsIgnoreCase(YarnApplicationState.FAILED.name()) || state.equalsIgnoreCase("ERROR")
                            || state.equalsIgnoreCase(YarnApplicationState.KILLED.name())) {
                        //  更新 app状态
                        log.info("Thread:" + Thread.currentThread().getName() + " - " + submissionId + "完成，任务状态是：" + state);
                        allAppStatus.put(submissionId, state);
                        jedis.del(submissionKey);
                        if (jedis.exists(jobStatus)) {
                            jedis.del(jobStatus);
                        }
                        webSocketService.sendMsg(new WebSocketResponseMessage(WebSocketResponseMessage.EWebSocketResponseMessageType.PROGRESS, WebSocketResponseMessage.EWebSocketResponseMessageFormat.NUMBER, submissionId, 100));
                        webSocketService.sendMsg(new WebSocketResponseMessage(WebSocketResponseMessage.EWebSocketResponseMessageType.CONSOLE, WebSocketResponseMessage.EWebSocketResponseMessageFormat.STRING, submissionId + "_variance", "任务状态:" + state));
                        return;
                    }
                    // 获得ApplicationID后就说明已经是SUBMITTED状态了
                    if (state.equalsIgnoreCase(YarnApplicationState.ACCEPTED.name())) {
                        //  更新app状态
                        if (count < acceptedProgress) {
                            count++;
                            webSocketService.sendMsg(new WebSocketResponseMessage(WebSocketResponseMessage.EWebSocketResponseMessageType.PROGRESS, WebSocketResponseMessage.EWebSocketResponseMessageFormat.NUMBER, submissionId, count));
                            allAppStatus.put(submissionId, count + "%");
                        }
                    }
                    if (state.equalsIgnoreCase(YarnApplicationState.RUNNING.name())) {
                        //  更新app状态
                        if (count < runProgress) {
                            count++;
                            webSocketService.sendMsg(new WebSocketResponseMessage(WebSocketResponseMessage.EWebSocketResponseMessageType.PROGRESS, WebSocketResponseMessage.EWebSocketResponseMessageFormat.NUMBER, submissionId, count));
                            allAppStatus.put(submissionId, count + "%");
                        } else {
                            webSocketService.sendMsg(new WebSocketResponseMessage(WebSocketResponseMessage.EWebSocketResponseMessageType.PROGRESS, WebSocketResponseMessage.EWebSocketResponseMessageFormat.NUMBER, submissionId, runProgress));
                            allAppStatus.put(submissionId, runProgress + "%");
                        }
                    }
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }


    }

    private SparkUtils.SparkReport response() {
        try {
            String response = HttpUtils.httpGet(this.reportUrl);
            if (response.startsWith("{") && response.endsWith("}")) {
                SparkUtils.SparkReport sparkReport = JSONObject.toJavaObject(JSONObject.parseObject(response), SparkUtils.SparkReport.class);
                return sparkReport;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private void showInfo() {
        Set<String> keys = RedisUtils.getJedis().keys("*_" + submissionId);
        for (String key : keys) {
            if (key.startsWith("variance_")) {
                Map<String, String> varianceMap = RedisUtils.getJedis().hgetAll(key);
                for (String field : varianceMap.keySet()) {
                    log.info("variance:" + field + ",msg:" + varianceMap.get(field));
                }
            }
        }
    }
}
