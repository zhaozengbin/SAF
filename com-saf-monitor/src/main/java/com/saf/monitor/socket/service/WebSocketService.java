package com.saf.monitor.socket.service;

import com.saf.monitor.socket.constant.WebSocketConstant;
import com.saf.monitor.socket.entity.WebSocketMessage;
import com.saf.monitor.socket.entity.WebSocketResponseMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class WebSocketService {

    @Autowired
    private SimpMessagingTemplate template;

    /**
     * 广播
     * 发给所有在线用户
     *
     * @param msg
     */
    public void sendMsg(WebSocketResponseMessage msg) {
        template.convertAndSend(WebSocketConstant.PRODUCERPATH, msg);
    }

    /**
     * 发送给指定用户
     *
     * @param users
     * @param msg
     */
    public void send2Users(List<String> users, WebSocketResponseMessage msg) {
        users.forEach(userName -> {
            template.convertAndSendToUser(userName, WebSocketConstant.P2PPUSHPATH, msg);
        });
    }
}
