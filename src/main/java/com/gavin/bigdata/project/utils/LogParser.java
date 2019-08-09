package com.gavin.bigdata.project.utils;

import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;

public class LogParser {
    public Map<String, String> parse(String log) {
        Map<String, String> info = new HashMap<>();
        if (StringUtils.isNotBlank(log)) {
            String[] splits = log.split("\001");
            String ip = splits[13];
            IPParser.RegionInfo regionInfo = IPParser.getInstance().analyseIp(ip);
            String country = "-";
            String province = "-";
            String city = "-";
            if (regionInfo != null) {
                country = regionInfo.getCountry();
                province = regionInfo.getProvince();
                city = regionInfo.getCity();
            }
            info.put("ip", ip);
            info.put("country", country);
            info.put("province", province);
            info.put("city", city);

            String url = splits[1];
            info.put("url", url);

            String time = splits[17];
            info.put("time", time);
        }
        return info;
    }
}
