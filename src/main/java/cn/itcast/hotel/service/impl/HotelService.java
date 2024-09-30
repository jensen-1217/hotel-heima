package cn.itcast.hotel.service.impl;

import cn.itcast.hotel.mapper.HotelMapper;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;
import cn.itcast.hotel.service.IHotelService;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.DistanceUnit;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregate;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregation;
import co.elastic.clients.elasticsearch._types.aggregations.CompositeAggregationSource;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsBucket;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.FunctionBoostMode;
import co.elastic.clients.elasticsearch._types.query_dsl.FunctionScoreBuilders;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchAllQuery;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.*;
import co.elastic.clients.json.JsonData;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class HotelService extends ServiceImpl<HotelMapper, Hotel> implements IHotelService {

    @Autowired
    private ElasticsearchClient client;

    @Override
    public PageResult search(RequestParams params) {
        try {
            // 1.准备Request
            SearchRequest.Builder request = new SearchRequest.Builder().index("hotel");
            // 2.准备请求参数
            // 2.1.query
            buildBasicQuery(params, request);
            // 2.2.分页
            int page = params.getPage();
            int size = params.getSize();
            request.from((page - 1) * size).size(size);
            // 2.3.距离排序
            String location = params.getLocation();
            if (StringUtils.isNotBlank(location)) {
                String[] latLon = location.split(",");
                if (latLon.length == 2) {
                    double lat = Double.parseDouble(latLon[0]);
                    double lon = Double.parseDouble(latLon[1]);
                    request.sort(s -> s
                            .geoDistance(g -> g
                                    .field("location")
                                    .location(p -> p.latlon(ll -> ll.lon(lon).lat(lat)))
                                    .unit(DistanceUnit.Kilometers)
                                    .order(SortOrder.Asc)
                            ));
                }
            }
            // 3.发送请求
            // 执行查询
            SearchResponse<HotelDoc> response = client.search(request.build(), HotelDoc.class);
            // 4.解析响应
            return handleResponse(response);
        } catch (IOException e) {
            throw new RuntimeException("搜索数据失败", e);
        }
    }

    @Override
    public Map<String, List<String>> getFilters(RequestParams params) {
        try {
            // 1.准备Request
            SearchRequest.Builder request = new SearchRequest.Builder().index("hotel");
            // 2.准备请求参数
            // 2.1.query
            buildBasicQuery(params, request);
            // 2.2.设置size
            request.size(0);
            // 2.3.聚合
            buildAggregation(request);
            // 3.发出请求
            SearchResponse<HotelDoc> response = client.search(request.build(), HotelDoc.class);
            // 4.解析结果
            Map<String, List<String>> result = new HashMap<>();
            Map<String, Aggregate> aggregations = response.aggregations();
            // 4.1.根据品牌名称，获取品牌结果
            List<String> brandList = getAggByName(aggregations, "brandAgg");
            result.put("brand", brandList);
            // 4.2.根据品牌名称，获取品牌结果
            List<String> cityList = getAggByName(aggregations, "cityAgg");
            result.put("city", cityList);
            // 4.3.根据品牌名称，获取品牌结果
            List<String> starList = getAggByName(aggregations, "starAgg");
            result.put("starName", starList);
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> getSuggestions(String prefix) {
        try {
            // 1.准备Request
            SearchRequest.Builder request = new SearchRequest.Builder().index("hotel");
            // 1.准备DSL
            request.suggest(Suggester.of(su -> su
                    .suggesters("suggestions", sug -> sug
                            .completion(c -> c
                                    .field("suggestion")
//                                    .prefix(prefix)
                                    .skipDuplicates(true)
                                    .size(10)
                            )
                    .text(prefix))));
//        SearchRequest searchRequest = SearchRequest.of(s -> s
//                .index("hotel")
//                .suggest(Suggester.of(su->su
//                        .suggesters("suggestions",sug->sug
//                                .completion(c->c
//                                        .field("title")
//                                        .skipDuplicates(true)
//                                        .prefix(prefix)
//                                        .size(10)
//                                )))));
            // 3.发起请求
            SearchResponse<HotelDoc> response = client.search(request.build(), HotelDoc.class);
            // 4.解析结果
            // 4.1.根据补全查询名称，获取补全结果
            // 4.2.获取options
            List<CompletionSuggestOption<HotelDoc>> options = response.suggest().get("suggestions").get(0).completion().options();
            // 4.3.遍历
            List<String> list = new ArrayList<>(options.size());
            options.forEach(option->{
                String text = option.text();
                list.add(text);
                System.out.println(text);
            });
//            System.out.println("options = " + options);
            return list;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private List<String> getAggByName(Map<String, Aggregate> aggregations, String starAgg) {
        // 4.1.根据聚合名称获取聚合结果
        // 4.2.获取buckets
        List<StringTermsBucket> brandTerms = aggregations.get(starAgg).sterms().buckets().array();
        ArrayList<String> brandList = new ArrayList<>();
        // 4.3.遍历
        brandTerms.forEach(bucket -> {
            // 4.4.获取key
            brandList.add(bucket.key().stringValue());
        });
        return brandList;
    }

    private void buildAggregation(SearchRequest.Builder request) {
        request.aggregations("brandAgg", a -> a
                .terms(h -> h.
                        field("brand")
                        .size(20)));
        request.aggregations("cityAgg", a -> a
                .terms(h -> h.
                        field("city")
                        .size(20)));
        request.aggregations("starAgg", a -> a
                .terms(h -> h.
                        field("starName")
                        .size(20)));
    }

    private void buildBasicQuery(RequestParams params, SearchRequest.Builder request) {
        // 1. 准备Boolean查询
        String key = params.getKey();
        BoolQuery.Builder boolQuery = new BoolQuery.Builder();
        if (StringUtils.isNotBlank(key)) {
            boolQuery.must(m -> m.match(match -> match.field("all").query(key)));
        } else {
            boolQuery.must(m -> m.matchAll(MatchAllQuery.of(match -> match)));
        }

        // 1.2. 品牌
        String brand = params.getBrand();
        if (StringUtils.isNotBlank(brand)) {
            boolQuery.filter(f -> f.term(t -> t.field("brand").value(brand)));
        }

        // 1.3. 城市
        String city = params.getCity();
        if (StringUtils.isNotBlank(city)) {
            boolQuery.filter(f -> f.term(t -> t.field("city").value(city)));
        }

        // 1.4. 星级
        String starName = params.getStarName();
        if (StringUtils.isNotBlank(starName)) {
            boolQuery.filter(f -> f.term(t -> t.field("starName").value(starName)));
        }

        // 1.5. 价格范围
        Integer minPrice = params.getMinPrice();
        Integer maxPrice = params.getMaxPrice();
        if (minPrice != null && maxPrice != null) {
            maxPrice = maxPrice == 0 ? Integer.MAX_VALUE : maxPrice;
            Integer finalMaxPrice = maxPrice;
            boolQuery.filter(f -> f.range(r -> r.field("price").gte(JsonData.of(minPrice)).lte(JsonData.of(finalMaxPrice))));
        }

        // 2. 算分函数查询
        request.query(q -> q.functionScore(fs -> fs
                .query(b -> b.bool(boolQuery.build())) // 直接传入构建的布尔查询
                .functions(fn -> fn
                        .filter(f -> f.term(t -> t.field("isAD").value(true))) // 过滤条件
                        .weight(10.0) // 算分函数
                )
                .boostMode(FunctionBoostMode.Multiply) // 设定boost模式
        ));
    }

    private PageResult handleResponse(SearchResponse response) {
        HitsMetadata<HotelDoc> searchHits = response.hits();
        // 4.1.总条数
        long total = searchHits.total().value();
        // 4.2.获取文档数组
        List<Hit<HotelDoc>> hits = searchHits.hits();
        List<HotelDoc> hotels = new ArrayList<>(hits.size());
        // 4.3.遍历
        hits.forEach(hit -> {
            // 获取文档source
            HotelDoc hotelDoc = hit.source();
            // 检查高亮字段是否存在
            if (hit.highlight() != null && hit.highlight().containsKey("name")) {
                String name = hit.highlight().get("name").get(0); // 获取高亮结果
                hotelDoc.setName(name); // 设置高亮结果
            }
            System.out.println(hotelDoc);
            // 4.8.排序信息
            List<FieldValue> sort = hit.sort();
            if (sort.size() > 0) {
                hotelDoc.setDistance(sort.get(0)._get());
            }
            // 4.9.放入集合
            hotels.add(hotelDoc);

        });
        return new PageResult(total, hotels);
    }
}
