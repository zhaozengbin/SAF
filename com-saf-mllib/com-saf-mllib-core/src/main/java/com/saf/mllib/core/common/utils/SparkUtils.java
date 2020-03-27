package com.saf.mllib.core.common.utils;

import com.alibaba.fastjson.JSONObject;
import com.saf.core.common.utils.ObjectUtils;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

public class SparkUtils {
    private static final Logger LOGGER = Logger.getLogger(SparkUtils.class);

    public static void inpputProcessMsg(String key, String value) {
        if (RedisUtils.getJedis().exists("currentSubmissionId")) {
            key = key + "_" + RedisUtils.getJedis().get("currentSubmissionId");
            RedisUtils.getJedis().rpush(key, value);
        } else {
            inpputProcessMsg(key, value);
        }
    }

    public static List<String> outputProcessMsg(String key) {
        return RedisUtils.getJedis().lrange(key + "_" + RedisUtils.getJedis().get("currentSubmissionId"), 0, -1);
    }

    // 判断是否提交成功
    public static SparkResult inputStreamReade(Process process) {
        SparkResult sparkResult = inputStreamReade(process.getErrorStream());
        if (ObjectUtils.isEmpty(sparkResult)) {
            sparkResult = inputStreamReade(process.getInputStream());
        } else {
            sparkResult = new SparkResult(false);
        }
        return sparkResult;
    }

    public static SparkResult inputStreamReade(InputStream is) {
        SparkResult sparkResult = null;
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        try {
            String line = reader.readLine();
            boolean jsonContent = false;
            StringBuffer stringBuffer = new StringBuffer();
            StringBuffer msg = new StringBuffer();
            while (line != null) {
                msg.append(line);
                msg.append("\r\n");
                if (line.equals("{")) {
                    jsonContent = true;
                } else if (line.equals("}")) {
                    jsonContent = false;
                    stringBuffer.append(line);
                    JSONObject json = JSONObject.parseObject(stringBuffer.toString());
                    sparkResult = JSONObject.toJavaObject(json, SparkResult.class);
                }
                if (jsonContent) {
                    stringBuffer.append(line);
                }
                line = reader.readLine();
            }
            LOGGER.info(msg.toString());
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sparkResult;
    }

    public static class SparkResult {

        private String action;
        private String message;
        private String serverSparkVersion;
        private String submissionId;
        private boolean success;

        public SparkResult() {
        }

        public SparkResult(String action, String message, String serverSparkVersion, String submissionId, boolean success) {
            this.action = action;
            this.message = message;
            this.serverSparkVersion = serverSparkVersion;
            this.submissionId = submissionId;
            this.success = success;
        }

        public SparkResult(boolean success) {
            this.success = success;
        }

        public String getAction() {
            return action;
        }

        public void setAction(String action) {
            this.action = action;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        public String getServerSparkVersion() {
            return serverSparkVersion;
        }

        public void setServerSparkVersion(String serverSparkVersion) {
            this.serverSparkVersion = serverSparkVersion;
        }

        public String getSubmissionId() {
            return submissionId;
        }

        public void setSubmissionId(String submissionId) {
            this.submissionId = submissionId;
        }

        public boolean isSuccess() {
            return success;
        }

        public void setSuccess(boolean success) {
            this.success = success;
        }
    }

    public static class SparkReport {
        private String action;
        private String driverState;
        private String serverSparkVersion;
        private String submissionId;
        private boolean success;
        private String workerHostPort;
        private String workerId;

        public SparkReport() {
        }

        public SparkReport(String action, String driverState, String serverSparkVersion, String submissionId, boolean success, String workerHostPort, String workerId) {
            this.action = action;
            this.driverState = driverState;
            this.serverSparkVersion = serverSparkVersion;
            this.submissionId = submissionId;
            this.success = success;
            this.workerHostPort = workerHostPort;
            this.workerId = workerId;
        }

        public String getAction() {
            return action;
        }

        public void setAction(String action) {
            this.action = action;
        }

        public String getDriverState() {
            return driverState;
        }

        public void setDriverState(String driverState) {
            this.driverState = driverState;
        }

        public String getServerSparkVersion() {
            return serverSparkVersion;
        }

        public void setServerSparkVersion(String serverSparkVersion) {
            this.serverSparkVersion = serverSparkVersion;
        }

        public String getSubmissionId() {
            return submissionId;
        }

        public void setSubmissionId(String submissionId) {
            this.submissionId = submissionId;
        }

        public boolean isSuccess() {
            return success;
        }

        public void setSuccess(boolean success) {
            this.success = success;
        }

        public String getWorkerHostPort() {
            return workerHostPort;
        }

        public void setWorkerHostPort(String workerHostPort) {
            this.workerHostPort = workerHostPort;
        }

        public String getWorkerId() {
            return workerId;
        }

        public void setWorkerId(String workerId) {
            this.workerId = workerId;
        }
    }
}
