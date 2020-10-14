package com.atguigu.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.jcodings.util.Hash;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@RestController
public class PublisherController {

    @Autowired
    private PublisherService publisherService;

    @RequestMapping("realtime-total")
    public String getRealTimeTotal(@RequestParam("date") String date) {

        //1.查询日活总数
        Integer dauTotal = publisherService.getDauTotal(date);

        //2.创建集合用于存放最终的数据
        ArrayList<Map> result = new ArrayList<>();

        //3.创建日活Map并赋值
        HashMap<String, Object> dauMap = new HashMap<>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", dauTotal);

        //4.创建新增设备Map并赋值
        HashMap<String, Object> newMidMap = new HashMap<>();
        newMidMap.put("id", "new_mid");
        newMidMap.put("name", "新增设备");
        newMidMap.put("value", 233);

        //将map放入集合
        result.add(dauMap);
        result.add(newMidMap);

        //返回结果
        return JSON.toJSONString(result);
    }

    @RequestMapping("realtime-hours")
    public String getRealTimeHours(@RequestParam("id") String id, @RequestParam("date") String date) {

        //1.创建Map用于存放最终结果
        HashMap<String, Map> result = new HashMap<>();

        //2.查询今天的分时数据
        Map todayMap = publisherService.getDauHourTotal(date);

        //3.查询昨天的分时数据
        String yesterday = LocalDate.parse(date).plusDays(-1).toString();
        Map yesterdayMap = publisherService.getDauHourTotal(yesterday);

        //4.将今天以及昨天的分时数据放入result
        result.put("yesterday", yesterdayMap);
        result.put("today", todayMap);

        //5.转换为JSON数据做返回
        return JSON.toJSONString(result);
    }

}
