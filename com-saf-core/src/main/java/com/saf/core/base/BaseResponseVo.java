package com.saf.core.base;

public class BaseResponseVo {

    private int status;

    private Object data;

    public BaseResponseVo() {
    }

    public BaseResponseVo(int status, Object data) {
        this.status = status;
        this.data = data;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }
}
