package com.saf.core.base;

import com.alibaba.fastjson.JSONObject;
import com.saf.core.common.utils.CookieUtils;
import com.saf.core.common.utils.ObjectUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Controller;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@Controller
public abstract class AbstractBaseController {

    private static final String SESSION_USER_INFO = "session_user_info";

    private static final String COOKIE_USER_INFO = "x-auth-token";

    @Autowired
    protected RedisTemplate<String, String> redisTemplate;

    /**
     * 方法：setUserInfo
     * 描述：设置用户信息
     * 作者：赵增斌 E-mail:zhaozengbin@gmail.com QQ:4415599 weibo:http://weibo.com/zhaozengbin
     * 时间: 2018年08月27日 下午1:37
     * 参数：[request, response, username]
     * 返回: void
     */
    protected void setUserInfo(HttpServletRequest request, String username) {
        request.getSession().setAttribute(SESSION_USER_INFO, username);
    }

    /**
     * 方法：removeUserInfo
     * 描述：删除用户信息
     * 作者：赵增斌 E-mail:zhaozengbin@gmail.com QQ:4415599 weibo:http://weibo.com/zhaozengbin
     * 时间: 2018年08月28日 下午2:13
     * 参数：[request]
     * 返回: void
     */
    protected void removeUserInfo(HttpServletRequest request) {
        /**
         *  方法：removeUserInfo
         *  描述：TODO
         *  作者：赵增斌 E-mail:zhaozengbin@gmail.com QQ:4415599 weibo:http://weibo.com/zhaozengbin
         *  时间: 2018年08月28日 下午2:13
         *  参数：[request]
         *  返回: void
         */
        request.getSession().removeAttribute(SESSION_USER_INFO);
    }

    protected String getUserInfo(HttpServletRequest request) {
        String username = (String) request.getSession().getAttribute(SESSION_USER_INFO);
        if (ObjectUtils.isNotEmpty(username)) {
            return username;
        }
        return username;
    }

    protected BaseResponseVo success(Object data) {
        return new BaseResponseVo(200, data);
    }


    protected BaseResponseVo fail(Object exception) {
        return new BaseResponseVo(500, exception);
    }

    protected BaseResponseVo fail(int status, Object exception) {
        return new BaseResponseVo(status, exception);
    }
}
