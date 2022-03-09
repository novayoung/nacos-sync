package com.alibaba.nacossync.constant;


public enum FrameworkEnum {

    ALL("ALL", "所有"),

    DUBBO("DUBBO", "Dubbo"),

    SPRING_CLOUD("SPRING_CLOUD", "Spring-Cloud");

    private String code;

    private String desc;

    FrameworkEnum(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public String getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }
}
