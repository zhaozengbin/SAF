package com.saf.core.controller;

import com.alibaba.fastjson.JSONObject;
import com.saf.core.base.AbstractBaseController;
import com.saf.core.base.BaseResponseVo;
import com.saf.core.common.utils.ObjectUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpSession;

@Controller
@RequestMapping(value = "/mllib")
public abstract class AbstractExecuteController extends AbstractBaseController {
    @Autowired
    protected RedisTemplate redisTemplate;


    @RequestMapping(value = "/execute", method = RequestMethod.POST)
    @ResponseBody
    public BaseResponseVo execute(HttpSession session, @RequestBody JSONObject jsonObject) {
        if (!validata(session, jsonObject)) {
            return fail("验证失败");
        }
        return submit(session, jsonObject);
    }

    /**
     * 方法：validata
     * 描述：验证
     * 作者：赵增斌 E-mail:zhaozengbin@gmail.com QQ:4415599 weibo:http://weibo.com/zhaozengbin
     * 时间: 2018年09月03日 下午8:06
     * 参数：[jsonObject]
     * 返回: boolean
     */
    protected boolean validata(HttpSession session, JSONObject jsonObject) {
        if (ObjectUtils.isEmpty(jsonObject)) {
            return false;
        }
        if (ObjectUtils.isEmpty(appName(session))) {
            return false;
        }
        if (ObjectUtils.isEmpty(master(session))) {
            return false;
        }

        if (ObjectUtils.isEmpty(sparkHome(session))) {
            return false;
        }

        for (String key : jsonObject.keySet()) {
            if (ObjectUtils.isEmpty(jsonObject.getString(key))) {
                return false;
            }
        }
        return true;
    }

    protected boolean existSparkTask(String submissionIdKey) {
        return redisTemplate.hasKey(submissionIdKey);
    }

    protected void addSparkTask(String submissionIdKey, String submissionIdValue) {
        redisTemplate.opsForValue().set(submissionIdKey, submissionIdValue);
    }

    protected abstract BaseResponseVo submit(HttpSession session, JSONObject jsonObject);

    protected String mllibName() {
        return "";
    }

    protected String appName(HttpSession session) {
        Object appName = session.getAttribute(mllibName() + "_" + "app_name");
        return ObjectUtils.isNotEmpty(appName) ? appName.toString() : null;
    }

    protected String master(HttpSession session) {
        Object master = session.getAttribute(mllibName() + "_" + "master");
        return ObjectUtils.isNotEmpty(master) ? master.toString() : null;
    }

    protected String yarnPath(HttpSession session) {
        Object yarnPath = session.getAttribute(mllibName() + "_" + "yarn_path");
        return ObjectUtils.isNotEmpty(yarnPath) ? yarnPath.toString() : null;
    }

    protected String sparkHome(HttpSession session) {
        Object sparkName = session.getAttribute(mllibName() + "_" + "spark_home_path");
        return ObjectUtils.isNotEmpty(sparkName) ? sparkName.toString() : null;
    }

    protected String appResource(HttpSession session) {
        Object sparkName = session.getAttribute(mllibName() + "_" + "app_resource");
        return ObjectUtils.isNotEmpty(sparkName) ? sparkName.toString() : null;
    }

    protected String hadoopConfPath(HttpSession session) {
        Object sparkName = session.getAttribute(mllibName() + "_" + "hadoop_conf_path");
        return ObjectUtils.isNotEmpty(sparkName) ? sparkName.toString() : null;
    }

    protected String javaHome(HttpSession session) {
        Object sparkName = session.getAttribute(mllibName() + "_" + "java_home_path");
        return ObjectUtils.isNotEmpty(sparkName) ? sparkName.toString() : null;
    }

    protected String hdfsPath(HttpSession session) {
        Object sparkName = session.getAttribute(mllibName() + "_" + "hdfs_path");
        return ObjectUtils.isNotEmpty(sparkName) ? sparkName.toString() : null;
    }

    protected String sessionStringValue(HttpSession session, String name) {
        Object sparkName = session.getAttribute((name.startsWith(mllibName())) ? name : (mllibName() + "_" + name));
        return ObjectUtils.isNotEmpty(sparkName) ? sparkName.toString() : null;
    }
}

