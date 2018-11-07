package com.saf.core.controller;

import com.alibaba.fastjson.JSONObject;
import com.saf.core.base.AbstractBaseController;
import com.saf.core.base.BaseResponseVo;
import com.saf.core.common.utils.ObjectUtils;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;

@Controller
@RequestMapping(value = "/sessionCache")
public class SessionController extends AbstractBaseController {

    @RequestMapping(value = "/add", method = RequestMethod.POST)
    @ResponseBody
    public BaseResponseVo add(HttpServletRequest request, @RequestBody JSONObject jsonObject) {
        if (ObjectUtils.isNotEmpty(jsonObject)) {
            for (String key : jsonObject.keySet()) {
                if (ObjectUtils.isNotEmpty(jsonObject.get(key))) {
                    request.getSession().setAttribute(key, jsonObject.get(key));
                }
            }
        }
        return success("设置成功");
    }
}
