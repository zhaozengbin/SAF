package com.saf.monitor.socket.entity;

public class WebSocketResponseMessage {
    private EWebSocketResponseMessageType eResponseMessageType;

    private EWebSocketResponseMessageFormat eWebSocketResponseMessageFormat;

    private String responseMessageType;

    private String responseMessageId;

    private String responseMessageFormat;

    private Object responseMessage;

    public WebSocketResponseMessage(EWebSocketResponseMessageType eResponseMessageType, EWebSocketResponseMessageFormat eWebSocketResponseMessageFormat, String responseMessage) {
        this.eResponseMessageType = eResponseMessageType;
        this.responseMessageType = eResponseMessageType.name().toLowerCase();
        this.responseMessageFormat = eWebSocketResponseMessageFormat.name().toLowerCase();
        this.responseMessage = responseMessage;
    }

    public WebSocketResponseMessage(EWebSocketResponseMessageType eResponseMessageType, EWebSocketResponseMessageFormat eWebSocketResponseMessageFormat, String responseMessageId, Object responseMessage) {
        this.eResponseMessageType = eResponseMessageType;
        this.responseMessageType = eResponseMessageType.name().toLowerCase();
        this.responseMessageFormat = eWebSocketResponseMessageFormat.name().toLowerCase();
        this.responseMessageId = responseMessageId;
        this.responseMessage = responseMessage;
    }

    public EWebSocketResponseMessageType geteResponseMessageType() {
        return eResponseMessageType;
    }

    public String getResponseMessageType() {
        return responseMessageType;
    }

    public String getResponseMessageId() {
        return responseMessageId;
    }

    public Object getResponseMessage() {
        return responseMessage;
    }

    public EWebSocketResponseMessageFormat geteWebSocketResponseMessageFormat() {
        return eWebSocketResponseMessageFormat;
    }

    public String getResponseMessageFormat() {
        return responseMessageFormat;
    }

    public enum EWebSocketResponseMessageType {
        PROGRESS, CONSOLE, CUSTOM;
    }

    public enum EWebSocketResponseMessageFormat {
        JSON, STRING, NUMBER;
    }
}
