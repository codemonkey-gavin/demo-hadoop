package com.gavin.bigdata.project.utils;

import org.junit.Test;

public class IPTest {
    @Test
    public void demo() {
        IPParser.RegionInfo regionInfo = IPParser.getInstance().analyseIp("161.117.82.125");
        System.out.println(regionInfo.getCountry());
        System.out.println(regionInfo.getProvince());
        System.out.println(regionInfo.getCity());
    }
}
