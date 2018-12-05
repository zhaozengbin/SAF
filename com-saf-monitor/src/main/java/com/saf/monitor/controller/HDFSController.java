package com.saf.monitor.controller;

import com.alibaba.fastjson.JSONObject;
import com.saf.core.base.AbstractBaseController;
import com.saf.core.base.BaseResponseVo;
import com.saf.core.common.utils.ObjectUtils;
import com.saf.mllib.core.common.utils.HadoopUtils;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;

@Controller
@RequestMapping(value = "/hdfs")
public class HDFSController extends AbstractBaseController {
    /**
     * 类名称：HDFSController
     * 类描述：检测HDFS连接是否正确
     * 创建人：赵增斌
     * 修改人：赵增斌
     * 修改时间：2018/8/29 下午5:17
     * 修改备注：TODO
     */
    @RequestMapping(value = "/checkHDFSStatus", method = RequestMethod.POST)
    @ResponseBody
    public BaseResponseVo checkHDFSStatus(HttpServletRequest request, @RequestBody JSONObject jsonObject) {
        if (ObjectUtils.isNotEmpty(jsonObject)) {
            for (String key : jsonObject.keySet()) {
                if (key.contains("hdfs_path")) {
                    String hdfsPath = jsonObject.getString(key);
                    if (ObjectUtils.isEmpty(hdfsPath)) {
                        return fail("HDFS 地址未填写");
                    }
                    try {
                        HadoopUtils monitorUtils = HadoopUtils.getInstance(hdfsPath);
                        long used = monitorUtils.fs.getStatus().getUsed();
                        if (used > 0) {
                            request.getSession().setAttribute(key, hdfsPath);
                            return success("HDFS 检测成功");
                        }
                    } catch (Exception e) {
                        return fail("HDFS 地址连接异常");
                    }
                }
            }
        }
        return fail("HDFS 配置填写有问题");
    }

    /**
     * 类名称：checkHDFSTmpStatus
     * 类描述：检测HDFS tmp目录是否正确
     * 创建人：赵增斌
     * 修改人：赵增斌
     * 修改时间：2018/8/29 下午5:17
     * 修改备注：TODO
     */
    @RequestMapping(value = "/checkHDFSTmpStatus", method = RequestMethod.POST)
    @ResponseBody
    public BaseResponseVo checkHDFSTmpStatus(HttpServletRequest request, @RequestBody JSONObject jsonObject) {
        if (ObjectUtils.isNotEmpty(jsonObject)) {
            for (String key : jsonObject.keySet()) {
                if (key.contains("check_point_dir")) {
                    String hdfsTmpPath = jsonObject.getString(key);
                    if (ObjectUtils.isEmpty(hdfsTmpPath)) {
                        return fail("HDFS Check Point 目录未填写");
                    }
                    boolean exists = false;
                    try {
                        HadoopUtils monitorUtils = HadoopUtils.getInstance(null);
                        exists = monitorUtils.existDir(hdfsTmpPath, false);
                    } catch (Exception e) {
                        return fail("HDFS 地址连接异常");
                    }
                    if (exists) {
                        request.getSession().setAttribute(key, hdfsTmpPath);
                        return success("HDFS Check Point 目录检测成功");
                    } else {
                        return fail("HDFS Check Point 目录不存在");
                    }
                }
            }
        }
        return fail("HDFS Check Point 目录配置填写有问题");
    }

    /**
     * 类名称：checkHDFSFileStatus
     * 类描述：检测HDFS 文件是否正确
     * 创建人：赵增斌
     * 修改人：赵增斌
     * 修改时间：2018/8/29 下午5:17
     * 修改备注：TODO
     */
    @RequestMapping(value = "/checkHDFSFileStatus", method = RequestMethod.POST)
    @ResponseBody
    public BaseResponseVo checkHDFSFileStatus(HttpServletRequest request, @RequestBody JSONObject jsonObject) {
        if (ObjectUtils.isNotEmpty(jsonObject)) {
            for (String key : jsonObject.keySet()) {
                String hdfsFilePath = jsonObject.getString(key);
                if (ObjectUtils.isEmpty(hdfsFilePath)) {
                    return fail("HDFS 文件路径未填写");
                }
                boolean exists = false;
                try {
                    HadoopUtils monitorUtils = HadoopUtils.getInstance(null);
                    exists = monitorUtils.existFile(hdfsFilePath, false);
                } catch (Exception e) {
                    return fail("HDFS 文件路径检测异常");
                }
                if (exists) {
                    request.getSession().setAttribute(key, hdfsFilePath);
                    return success("HDFS 文件路径检测成功");
                } else {
                    return fail("HDFS 文件路径不存在");
                }
            }
        }
        return fail("HDFS 文件路径配置填写有问题");
    }
}