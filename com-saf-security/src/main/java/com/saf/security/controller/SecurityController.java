package com.saf.security.controller;

import com.alibaba.fastjson.JSONObject;
import com.saf.core.base.AbstractBaseController;
import com.saf.core.base.BaseResponseVo;
import com.saf.core.common.utils.ObjectUtils;
import com.saf.security.service.IUserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;


@Controller
@RequestMapping("/security")
public class SecurityController extends AbstractBaseController {

    @Autowired
    private IUserService iUserService;

    @RequestMapping(value = "/login", method = RequestMethod.POST)
    @ResponseBody
    public BaseResponseVo login(HttpServletRequest request, @RequestBody JSONObject jsonObject) {
        if (ObjectUtils.isNotEmpty(jsonObject) && jsonObject.containsKey("username") && jsonObject.containsKey("password")) {
            String username = jsonObject.getString("username");
            String password = jsonObject.getString("password");
            if (ObjectUtils.isNotEmpty(username) && ObjectUtils.isNotEmpty(password)) {
                boolean isLongin = iUserService.login(username, password);
                if (isLongin) {
                    super.setUserInfo(request, username);
                    return super.success(username);
                }
            }
        }
        return super.fail(null);
    }

    @RequestMapping(value = "/logout", method = RequestMethod.POST)
    @ResponseBody
    public BaseResponseVo logout(HttpServletRequest request) {
        super.removeUserInfo(request);
        return super.success(null);
    }

    @RequestMapping(value = "/checkUser", method = RequestMethod.POST)
    @ResponseBody
    public BaseResponseVo checkUser(HttpServletRequest request) {
        String username = super.getUserInfo(request);
        if (ObjectUtils.isNotEmpty(username)) {
            return super.success(username);
        }
        return super.fail(null);
    }
}
