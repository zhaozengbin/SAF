package com.saf.monitor.controller;

import com.alibaba.fastjson.JSONObject;
import com.saf.core.base.BaseResponseVo;
import com.saf.core.controller.AbstractExecuteController;
import com.saf.mllib.core.common.utils.SparkUtils;
import com.saf.monitor.socket.service.WebSocketService;
import com.saf.monitor.thread.MonitorRunable;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

@Controller
public abstract class AbstractParentExecuteController extends AbstractExecuteController {

    @Autowired
    protected WebSocketService webSocketService;

    protected BaseResponseVo submit(String[] mainArgs, String hadoopConfDir, String javaHome, String appName, String sparkHome, String master, String appResource, String mainClass, String submissionIdKey) throws InterruptedException, IOException {
        Map<String, String> env = new HashMap<>();
        //这两个属性必须设置
        // /usr/local/Cellar/hadoop/x.x.x/libexec/etc/hadoop
        env.put("HADOOP_CONF_DIR", hadoopConfDir);
        // /Library/Java/JavaVirtualMachines/jdk1.8.0_151.jdk/Contents/Home
        env.put("JAVA_HOME", javaHome);


        CountDownLatch countDownLatch = new CountDownLatch(1);
        //这里调用setJavaHome()方法后，JAVA_HOME is not set 错误依然存在
        SparkAppHandle handle = new SparkLauncher(env)
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
                .setVerbose(true).startApplication(new SparkAppHandle.Listener() {
                    //这里监听任务状态，当任务结束时（不管是什么原因结束）,isFinal（）方法会返回true,否则返回false
                    @Override
                    public void stateChanged(SparkAppHandle sparkAppHandle) {
                        if (sparkAppHandle.getState().isFinal()) {
                            countDownLatch.countDown();
                        }
                        System.out.println("state:" + sparkAppHandle.getState().toString());
                    }


                    @Override
                    public void infoChanged(SparkAppHandle sparkAppHandle) {
                        System.out.println("Info:" + sparkAppHandle.getState().toString());
                    }
                });
        System.out.println("The task is executing, please wait ....");
        //线程等待任务结束
        countDownLatch.await();
        System.out.println("The task is finished!");

//        Process process = new SparkLauncher(env)
//                .setAppName(appName)
//                .setSparkHome(sparkHome)
//                .setMaster(master)
//                .setConf("spark.driver.memory", "2g")
//                .setConf("spark.executor.memory", "1g")
//                .setConf("spark.executor.cores", "3")
//                .setAppResource(appResource)
//                .setMainClass(mainClass)
//                .setJavaHome(javaHome)
//                .addAppArgs(mainArgs)
//                .setDeployMode("cluster")//cluster
//                .launch();
////                InputStreamReaderRunnable inputStreamReaderRunnable = new InputStreamReaderRunnable(process.getInputStream(), "input");
////                Thread inputThread = new Thread(inputStreamReaderRunnable, "LogStreamReader input");
////                inputThread.start();
////                InputStreamReaderRunnable errorStreamReaderRunnable = new InputStreamReaderRunnable(process.getErrorStream(), "error");
////                Thread errorThread = new Thread(errorStreamReaderRunnable, "LogStreamReader error");
////                errorThread.start();
//        SparkUtils.SparkResult sparkResult = SparkUtils.inputStreamReade(process);
//        super.addSparkTask(submissionIdKey, sparkResult.getSubmissionId());
//        System.out.println("Waiting for finish...");
//        int exitCode = process.waitFor();
//        System.out.println("Finished! Exit code:" + exitCode);
//        new Thread(new MonitorRunable(submissionIdKey, sparkResult.getSubmissionId(), master, webSocketService)).start();
//        if (sparkResult.isSuccess()) {
//            String submissionId = redisTemplate.opsForValue().get(submissionIdKey).toString();
//            JSONObject jsonObject = new JSONObject();
//            jsonObject.put(submissionIdKey, submissionId);
//            jsonObject.put("msg", "提交成功");
//            return success(jsonObject.toJSONString());
//        } else {
//            return fail("提交失败");
//        }
        return success(null);
    }
}
