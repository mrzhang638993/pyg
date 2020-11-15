package com.itheima.report.bean;

import java.util.Objects;

public class AdClickLog {

    // 广告Id数据
    private  Long t_id;

    // 广告主ID
    private  Long corpurin;

    public Long getT_id() {
        return t_id;
    }

    public void setT_id(Long t_id) {
        this.t_id = t_id;
    }

    public Long getCorpurin() {
        return corpurin;
    }

    public void setCorpurin(Long corpurin) {
        this.corpurin = corpurin;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getDevice_type() {
        return device_type;
    }

    public void setDevice_type(String device_type) {
        this.device_type = device_type;
    }

    public String getAd_sources() {
        return ad_sources;
    }

    public void setAd_sources(String ad_sources) {
        this.ad_sources = ad_sources;
    }

    public String getAd_media() {
        return ad_media;
    }

    public void setAd_media(String ad_media) {
        this.ad_media = ad_media;
    }

    public String getAd_campaign() {
        return ad_campaign;
    }

    public void setAd_campaign(String ad_campaign) {
        this.ad_campaign = ad_campaign;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getClick_user_id() {
        return click_user_id;
    }

    public void setClick_user_id(String click_user_id) {
        this.click_user_id = click_user_id;
    }

    // 域名
    private String host;

    // 设备类型
    private  String device_type;

    // 广告来源
    private  String  ad_sources;

    // 广告媒介
    private  String ad_media;

    // 广告系列
    private  String ad_campaign;

    // 城市
    private  String city;

    // 时间
    private  String timeStamp;

    //用户ID
    private  String userId;

    //  点击的用户ID
    private  String click_user_id;


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AdClickLog that = (AdClickLog) o;
        return t_id.equals(that.t_id) &&
                corpurin.equals(that.corpurin) &&
                host.equals(that.host) &&
                device_type.equals(that.device_type) &&
                ad_sources.equals(that.ad_sources) &&
                ad_media.equals(that.ad_media) &&
                ad_campaign.equals(that.ad_campaign) &&
                city.equals(that.city) &&
                timeStamp.equals(that.timeStamp) &&
                userId.equals(that.userId) &&
                click_user_id.equals(that.click_user_id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(t_id, corpurin, host, device_type, ad_sources, ad_media, ad_campaign, city, timeStamp, userId, click_user_id);
    }

    @Override
    public String toString() {
        return "AdClickLog{" +
                "t_id=" + t_id +
                ", corpurin=" + corpurin +
                ", host='" + host + '\'' +
                ", device_type='" + device_type + '\'' +
                ", ad_sources='" + ad_sources + '\'' +
                ", ad_media='" + ad_media + '\'' +
                ", ad_campaign='" + ad_campaign + '\'' +
                ", city='" + city + '\'' +
                ", timeStamp='" + timeStamp + '\'' +
                ", userId='" + userId + '\'' +
                ", click_user_id='" + click_user_id + '\'' +
                '}';
    }
}
