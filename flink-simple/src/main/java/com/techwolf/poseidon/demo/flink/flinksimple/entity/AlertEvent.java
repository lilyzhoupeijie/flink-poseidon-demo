package com.techwolf.poseidon.demo.flink.flinksimple.entity;

public class AlertEvent {
    private String tag;
    private String content;

    @Override
    public String toString() {
        return "AlertEvent{" +
                "tag='" + tag + '\'' +
                ", content='" + content + '\'' +
                '}';
    }

    public AlertEvent(String tag, String content){
        this.tag=tag;
        this.content=content;
    }
    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}
