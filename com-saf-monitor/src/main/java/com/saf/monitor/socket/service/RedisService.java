package com.saf.monitor.socket.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.saf.mllib.core.common.constant.ConstantSparkTask;
import com.saf.monitor.socket.entity.WebSocketMessage;
import com.saf.monitor.socket.entity.WebSocketResponseMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

@Service
public class RedisService {
    @Autowired
    private RedisTemplate redisTemplate;

    @Autowired
    private WebSocketService webSocketService;

    public void receiveMessage(String message) {
        //这里是收到通道的消息之后执行的方法
        webSocketService.sendMsg(new WebSocketResponseMessage(WebSocketResponseMessage.EWebSocketResponseMessageType.CONSOLE, WebSocketResponseMessage.EWebSocketResponseMessageFormat.JSON,
                redisTemplate.opsForValue().get(ConstantSparkTask.ALS_CURRENT_SUBMISSIONID) + "_" + "variance", message));
    }
}
