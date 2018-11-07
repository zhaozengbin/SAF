package com.saf.core.config;

import com.saf.core.common.utils.ObjectUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.session.data.redis.config.ConfigureRedisAction;
import org.springframework.session.data.redis.config.annotation.web.http.EnableRedisHttpSession;
import org.springframework.session.web.http.CookieHttpSessionStrategy;
import org.springframework.session.web.http.CookieSerializer;
import org.springframework.session.web.http.DefaultCookieSerializer;
import org.springframework.session.web.http.HttpSessionStrategy;

@Configuration
@EnableRedisHttpSession
public class HttpSessionConfig {

    @Value("${spring.redis.host}")
    private String host;
    @Value("${spring.redis.port}")
    private int port;
    @Value("${spring.redis.password}")
    private String password;
    @Value("${spring.redis.database}")
    private int database;

    @Bean
    public static ConfigureRedisAction configureRedisAction() {
        return ConfigureRedisAction.NO_OP;
    }

    /**
     * RedisHttpSession 创建 连接工厂
     *
     * @return LettuceConnectionFactory
     */
    @Bean
    public LettuceConnectionFactory connectionFactory() {
        RedisStandaloneConfiguration config = new RedisStandaloneConfiguration();
        config.setHostName(host);
        config.setPort(port);
        if (ObjectUtils.isNotEmpty(password)) {
            config.setPassword(RedisPassword.of(password));
        }
        config.setDatabase(database);
        return new LettuceConnectionFactory(config);
    }


    /**
     * 自定义 Token
     *
     * @return
     */
    @Bean
    public HttpSessionStrategy httpSessionStrategy(CookieSerializer cookieSerializer) {
        CookieHttpSessionStrategy cookieHttpSessionStrategy = new CookieHttpSessionStrategy();
        cookieHttpSessionStrategy.setCookieSerializer(cookieSerializer);
        return cookieHttpSessionStrategy;
    }

    @Bean
    public CookieSerializer cookieSerializer() {
        String cookieName = "SAF-TOKEN";
        DefaultCookieSerializer defaultCookieSerializer = new DefaultCookieSerializer();
        //cookie名字
        defaultCookieSerializer.setCookieName(cookieName);
        //域
        defaultCookieSerializer.setDomainName("duia.com");
        //存储路径
        defaultCookieSerializer.setCookiePath("/");
        return defaultCookieSerializer;
    }

}

