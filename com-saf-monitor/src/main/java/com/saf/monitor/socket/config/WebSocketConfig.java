package com.saf.monitor.socket.config;

import com.saf.monitor.socket.constant.WebSocketConstant;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.AbstractWebSocketMessageBrokerConfigurer;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;

@Configuration
// @EnableWebSocketMessageBroker注解用于开启使用STOMP协议来传输基于代理（MessageBroker）的消息，这时候控制器（controller）
// 开始支持@MessageMapping,就像是使用@requestMapping一样。
@EnableWebSocketMessageBroker
public class WebSocketConfig extends AbstractWebSocketMessageBrokerConfigurer {

    @Override
    public void registerStompEndpoints(StompEndpointRegistry stompEndpointRegistry) {
        //注册一个Stomp的节点（endpoint）,并指定使用SockJS协议。
        stompEndpointRegistry.addEndpoint(WebSocketConstant.WEBSOCKETPATH).setAllowedOrigins("*").withSockJS();
    }

    @Override
    public void configureMessageBroker(MessageBrokerRegistry registry) {
        //服务端发送消息给客户端的域,多个用逗号隔开
        registry.enableSimpleBroker(WebSocketConstant.WEBSOCKETBROADCASTPATH, WebSocketConstant.P2PPUSHBASEPATH);
        //定义一对一推送的时候前缀
        registry.setUserDestinationPrefix(WebSocketConstant.P2PPUSHBASEPATH);
        //定义websoket前缀
        registry.setApplicationDestinationPrefixes(WebSocketConstant.WEBSOCKETPATHPERFIX);
    }
}
