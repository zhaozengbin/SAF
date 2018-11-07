package com.saf.mllib.core.common.utils;

import com.saf.core.common.utils.ObjectUtils;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HttpUtils {
    //生成httpClient
    private static final CloseableHttpClient httpClient;
    public static final String CHAESET = "utf-8";

    static {
        RequestConfig config = RequestConfig.custom()
                .setConnectTimeout(6000) //请求超时时间
                .setSocketTimeout(6000) //响应超时时间
                .build();

        httpClient = HttpClientBuilder.create()
                .setDefaultRequestConfig(config)
                .build();
    }


    public static String httpGet(String url) throws Exception {
        return httpGet(url, null);
    }


    public static String httpGet(String url, Map<String, String> params) throws Exception {
        return httpGet(url, params, CHAESET);
    }


    /**
     * @param url
     * @param params
     * @param charset
     * @return
     * @throws Exception
     */

    public static String httpGet(String url, Map<String, String> params, String charset) throws Exception {
        //构造url
        if (params != null && !params.isEmpty()) {
            List<NameValuePair> pairs = new ArrayList<NameValuePair>();
            for (Map.Entry<String, String> entry : params.entrySet()) {
                if (entry.getValue() != null) {
                    pairs.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
                }
            }

            String queryString = EntityUtils.toString(new UrlEncodedFormEntity(pairs), charset);
            if (url.indexOf("?") != -1) {
                url += "&" + queryString;
            } else {
                url += "?" + queryString;
            }
        }


//httpGet获取请求，返回数据
        HttpGet httpGet = new HttpGet(url);
        CloseableHttpResponse response = httpClient.execute(httpGet);
        try {
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != 200) {
                httpGet.abort();
                throw new RuntimeException("httpClient: error status code:" + statusCode);

            }
            HttpEntity entity = response.getEntity();
            String result = null;
            if (entity != null) {
                result = EntityUtils.toString(entity, charset);
            }
            EntityUtils.consume(entity);
            return result;
        } finally {
            response.close();
        }
    }


    public static String httpPost(String url, HttpEntity requestEntity) throws Exception {
        return httpPost(url, null, requestEntity);
    }

    public static String httpPost(String url, Map<String, String> params, HttpEntity requsetEntity) throws Exception {
        //构建url
        if (params != null && !params.isEmpty()) {
            List<NameValuePair> pairs = new ArrayList<NameValuePair>();
            for (Map.Entry<String, String> entry : params.entrySet()) {
                if (entry.getValue() != null) {
                    pairs.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
                }
            }
            String queryString = EntityUtils.toString(new UrlEncodedFormEntity(pairs), CHAESET);
            if (url.indexOf("?") > 0) {
                url += "&" + queryString;
            } else {
                url += "?" + queryString;
            }
        }
        //httpPost请求获取数据
        HttpPost httpPost = new HttpPost(url);
        if (ObjectUtils.isNotEmpty(requsetEntity)) {
            httpPost.setEntity(requsetEntity);
        }
        CloseableHttpResponse response = httpClient.execute(httpPost);
        try {
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != 200) {
                httpPost.abort();
                throw new RuntimeException("httpClient error status code:" + statusCode);
            }
            HttpEntity entity = response.getEntity();
            String result = null;
            if (entity != null) {
                result = EntityUtils.toString(entity, CHAESET);
            }
            EntityUtils.consume(entity);
            return result;
        } finally {
            response.close();
        }
    }
}
