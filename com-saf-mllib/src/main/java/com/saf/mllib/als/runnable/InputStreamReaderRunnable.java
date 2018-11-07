package com.saf.mllib.als.runnable;

import com.alibaba.fastjson.JSONObject;
import com.saf.mllib.core.common.utils.SparkUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class InputStreamReaderRunnable implements Runnable {

    private InputStream is;

    private String name;

    public InputStreamReaderRunnable(InputStream is, String name) {
        this.is = is;
        this.name = name;
    }

    public void run() {
        System.out.println("InputStream " + name + ":");
        SparkUtils.inputStreamReade(is);
    }
}

