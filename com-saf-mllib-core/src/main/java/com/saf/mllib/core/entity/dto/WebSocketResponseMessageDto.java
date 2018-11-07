package com.saf.mllib.core.entity.dto;

public class WebSocketResponseMessageDto {
    private int type;

    private Object data;

    public WebSocketResponseMessageDto(int type, Object data) {
        this.type = type;
        this.data = data;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }
}
