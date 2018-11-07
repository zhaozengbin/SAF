package com.saf.monitor.socket.controller;

import com.saf.monitor.socket.constant.WebSocketConstant;
import com.saf.monitor.socket.entity.WebSocketMessage;
import com.saf.monitor.socket.entity.WebSocketResponseMessage;
import com.saf.monitor.socket.service.WebSocketService;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Controller;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

@Controller
public class WebSocketController {
    @Resource
    private WebSocketService webSocketService;

    @MessageMapping(WebSocketConstant.FORETOSERVERPATH)
//@MessageMapping和@RequestMapping功能类似，用于设置URL映射地址，浏览器向服务器发起请求，需要通过该地址。
    @SendTo(WebSocketConstant.PRODUCERPATH)//如果服务器接受到了消息，就会对订阅了@SendTo括号中的地址传送消息。
    public WebSocketResponseMessage say(WebSocketMessage message) throws Exception {
        List<String> users = new ArrayList<>();
        users.add("d892bf12bf7d11e793b69c5c8e6f60fb");//此处写死只是为了方便测试,此值需要对应页面中订阅个人消息的userId
        webSocketService.send2Users(users, new WebSocketResponseMessage(WebSocketResponseMessage.EWebSocketResponseMessageType.CUSTOM, WebSocketResponseMessage.EWebSocketResponseMessageFormat.STRING, "admin hello"));

        return new WebSocketResponseMessage(WebSocketResponseMessage.EWebSocketResponseMessageType.CUSTOM, WebSocketResponseMessage.EWebSocketResponseMessageFormat.STRING, "Welcome, " + message.getName() + "!");
    }
}
