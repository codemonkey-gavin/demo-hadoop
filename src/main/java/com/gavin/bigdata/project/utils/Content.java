package com.gavin.bigdata.project.utils;

import org.apache.commons.lang.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Content {
    public static String getPageId(String url) {
        String pageId = "";
        if (StringUtils.isNotBlank(url)) {
            return pageId;
        }
        Pattern pattern = Pattern.compile("topicId=[0-9]+");
        Matcher matcher = pattern.matcher(url);
        if (matcher.find()) {
            pageId = matcher.group().split("topicId=")[1];
        }
        return pageId;
    }
}
