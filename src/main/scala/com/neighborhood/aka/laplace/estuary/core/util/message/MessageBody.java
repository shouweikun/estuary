package com.neighborhood.aka.laplace.estuary.core.util.message;

import java.util.List;

public class MessageBody {
    private String batchNo;//不用填
    private String bsCode ="SYSTEM";//默认的SYSTEM
    private List<String> messageContents;//内容 必填
    private List<String> mobiles;//必填
    private int senderId =1;
    private String senderName;//必填

    public static MessageBody buildMessage(List<String> messageContents,List<String> mobiles,String senderName) {
        MessageBody msb = new MessageBody();
        msb.setMessageContents(messageContents);
        msb.setMobiles(messageContents);
        msb.setSenderName(senderName);
        return msb;
    }
    public String getBatchNo() {
        return batchNo;
    }

    public void setBatchNo(String batchNo) {
        this.batchNo = batchNo;
    }

    public String getBsCode() {
        return bsCode;
    }

    public void setBsCode(String bsCode) {
        this.bsCode = bsCode;
    }

    public List<String> getMessageContents() {
        return messageContents;
    }

    public void setMessageContents(List<String> messageContents) {
        this.messageContents = messageContents;
    }

    public List<String> getMobiles() {
        return mobiles;
    }

    public void setMobiles(List<String> mobiles) {
        this.mobiles = mobiles;
    }

    public int getSenderId() {
        return senderId;
    }

    public void setSenderId(int senderId) {
        this.senderId = senderId;
    }

    public String getSenderName() {
        return senderName;
    }

    public void setSenderName(String senderName) {
        this.senderName = senderName;
    }
}
