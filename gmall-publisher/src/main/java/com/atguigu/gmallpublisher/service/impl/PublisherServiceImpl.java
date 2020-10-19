package com.atguigu.gmallpublisher.service.impl;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmallpublisher.bean.Option;
import com.atguigu.gmallpublisher.bean.Stat;
import com.atguigu.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmallpublisher.mapper.OrderMapper;
import com.atguigu.gmallpublisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    private DauMapper dauMapper;

    @Autowired
    private OrderMapper orderMapper;

    @Autowired
    private JestClient jestClient;

    @Override
    public Integer getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauHourTotal(String date) {

        //1.获取Phoenix中的分时数据
        //        List{
        //            Map[(LH->09),(CT->645)]
        //            Map[(LH->17),(CT->413)]
        //        }
        List<Map> list = dauMapper.selectDauTotalHourMap(date);

        //==> Map[(09->645),(17->413)]
        HashMap<String, Long> result = new HashMap<>();
        for (Map map : list) {
            result.put((String) map.get("LH"), (Long) map.get("CT"));
        }

        //返回数据
        return result;
    }

    @Override
    public Double getOrderAmount(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map getOrderAmountHour(String date) {

        //1.获取Phoenix中的分时数据
        List<Map> list = orderMapper.selectOrderAmountHourMap(date);

        //==> Map[(09->645),(17->413)]
        HashMap<String, Double> result = new HashMap<>();
        for (Map map : list) {
            result.put((String) map.get("CREATE_HOUR"), (Double) map.get("SUM_AMOUNT"));
        }

        //返回数据
        return result;
    }

    @Override
    public String getSaleDetail(String date, int startpage, int size, String keyword) throws IOException {

        //1.编写DSL语句
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //1.1 添加全值匹配参数
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("dt", date);
        boolQueryBuilder.filter(termQueryBuilder);

        //1.2 添加分词匹配参数
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("sku_name", keyword).operator(MatchQueryBuilder.Operator.AND);
        boolQueryBuilder.must(matchQueryBuilder);

        searchSourceBuilder.query(boolQueryBuilder);

        //1.3 添加性别聚合组
        String genderGroup = "countByGender";
        TermsBuilder genderTerms = AggregationBuilders.terms(genderGroup);
        genderTerms.field("user_gender");
        genderTerms.size(10);
        searchSourceBuilder.aggregation(genderTerms);

        //1.4 添加年龄聚合组
        String ageGroup = "countByAge";
        TermsBuilder ageTerms = AggregationBuilders.terms(ageGroup);
        ageTerms.field("user_age");
        ageTerms.size(100);
        searchSourceBuilder.aggregation(ageTerms);

        //1.5 分页相关
        searchSourceBuilder.from((startpage - 1) * size);
        searchSourceBuilder.size(size);

        //2.执行查询
        System.out.println(searchSourceBuilder.toString());
        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex("gmall0523_sale_detail-query")
                .addType("_doc")
                .build();
        SearchResult searchResult = jestClient.execute(search);

        //3.解析结果
        //3.1 获取总数
        Long total = searchResult.getTotal();

        //3.2 获取数据明细
        List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
        ArrayList<Map> detail = new ArrayList<>();
        for (SearchResult.Hit<Map, Void> hit : hits) {
            detail.add(hit.source);
        }

        //3.3 解析性别聚合组
        MetricAggregation aggregations = searchResult.getAggregations();
        TermsAggregation genderAgg = aggregations.getTermsAggregation(genderGroup);
        //定义female人数
        Long femaleCount = 0L;
        for (TermsAggregation.Entry entry : genderAgg.getBuckets()) {
            if ("F".equals(entry.getKey())) {
                femaleCount = femaleCount + entry.getCount();
            }
        }
        //计算男女比例
        Double femaleRatio = Math.round(femaleCount * 1000D / total) / 10D;
        Double maleRatio = 100D - femaleRatio;
        //构建性别占比数据
        Option maleOpt = new Option("男", maleRatio);
        Option femaleOpt = new Option("女", femaleRatio);
        //将用户性别占比数据添加至集合
        ArrayList<Option> genderOptions = new ArrayList<>();
        genderOptions.add(maleOpt);
        genderOptions.add(femaleOpt);
        //构建用户性别占比饼图对象
        Stat genderStat = new Stat(genderOptions, "用户性别占比");

        //3.4 解析年龄聚合组
        TermsAggregation ageAgg = aggregations.getTermsAggregation(ageGroup);
        //定义年龄段人数
        long lower20 = 0L;
        long upper30 = 0L;
        for (TermsAggregation.Entry entry : ageAgg.getBuckets()) {
            if (Integer.parseInt(entry.getKey()) < 20) {
                lower20 = lower20 + entry.getCount();
            } else if (Integer.parseInt(entry.getKey()) >= 30) {
                upper30 = upper30 + entry.getCount();
            }
        }
        //计算年龄段占比
        double lower20Ratio = Math.round(lower20 * 1000D / total) / 10D;
        double upper30Ratio = Math.round(upper30 * 1000D / total) / 10D;
        double upper20to30 = Math.round((100D - lower20Ratio - upper30Ratio) * 10D) / 10D;
        //创建年龄的Option对象
        Option lower20Opt = new Option("20岁以下", lower20Ratio);
        Option upper20to30Opt = new Option("20岁到30岁", upper20to30);
        Option upper30Opt = new Option("30岁及30岁以上", upper30Ratio);
        ArrayList<Option> ageOptions = new ArrayList<>();
        ageOptions.add(lower20Opt);
        ageOptions.add(upper20to30Opt);
        ageOptions.add(upper30Opt);
        //创建年龄饼图对象
        Stat ageStat = new Stat(ageOptions, "用户年龄占比");

        //创建集合用于存放用户年龄和性别占比数据
        ArrayList<Stat> stats = new ArrayList<>();
        stats.add(ageStat);
        stats.add(genderStat);

        HashMap<String, Object> result = new HashMap<>();
        result.put("total", total);
        result.put("stat", stats);
        result.put("detail", detail);

        return JSON.toJSONString(result);
    }
}
