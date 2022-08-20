package cn.doitedu.rulemgmt.service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RestHighLevelClientBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.roaringbitmap.RoaringBitmap;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class ProfileConditionQueryServiceImpl implements ProfileConditionQueryService {
    private RestHighLevelClient client;
    SearchRequest request;

    // 构造es连接客户端
    public ProfileConditionQueryServiceImpl(){

        client = new RestHighLevelClient(RestClient.builder(new HttpHost("doitedu", 9200, "http")));
        request = new SearchRequest("doeusers");

    }

    // 接口文档：
    // [{"tagId":"tg01","compareType":"eq","compareValue":"3"},{"tagId":"tg04","compareType":"match","compareValue":"运动"}]
    @Override
    public RoaringBitmap queryProfileUsers(JSONArray jsonArray) throws IOException {

        // 构造一个组合条件查询参数构建器
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        for(int i = 0; i< jsonArray.size() ;i ++) {
            // {"tagId":"tg01","compareType":"eq","compareValue":"3"}
            JSONObject paramObject = jsonArray.getJSONObject(i);

            String tagId = paramObject.getString("tagId");
            String compareType = paramObject.getString("compareType");
            String compareValue = paramObject.getString("compareValue");

            switch (compareType){
                case "lt":
                    RangeQueryBuilder lt = QueryBuilders.rangeQuery(tagId).lt(compareValue);
                    if(StringUtils.isNumeric(compareValue)){
                        lt = QueryBuilders.rangeQuery(tagId).lt(Integer.parseInt(compareValue));
                    }
                    boolQueryBuilder.must(lt);
                    break;
                case "gt":
                    RangeQueryBuilder gt = QueryBuilders.rangeQuery(tagId).gt(compareValue);
                    if(StringUtils.isNumeric(compareValue)){
                        gt = QueryBuilders.rangeQuery(tagId).gt(Integer.parseInt(compareValue));
                    }
                    boolQueryBuilder.must(gt);
                    break;
                case "ge":
                    RangeQueryBuilder gte = QueryBuilders.rangeQuery(tagId).gte(compareValue);
                    if(StringUtils.isNumeric(compareValue)){
                        gte = QueryBuilders.rangeQuery(tagId).gte(Integer.parseInt(compareValue));
                    }
                    boolQueryBuilder.must(gte);
                    break;
                case "le":
                    RangeQueryBuilder lte = QueryBuilders.rangeQuery(tagId).lte(compareValue);
                    if(StringUtils.isNumeric(compareValue)){
                        lte = QueryBuilders.rangeQuery(tagId).lte(Integer.parseInt(compareValue));
                    }
                    boolQueryBuilder.must(lte);
                    break;
                case "between":
                    String[] fromTo = compareValue.split(",");
                    RangeQueryBuilder btw = QueryBuilders.rangeQuery(tagId).from(fromTo[0],true).to(fromTo[1],true);
                    if(StringUtils.isNumeric(fromTo[0]) && StringUtils.isNumeric(fromTo[1])){
                        btw = QueryBuilders.rangeQuery(tagId).from(Integer.parseInt(fromTo[0]),true).to(Integer.parseInt(fromTo[1]),true);
                    }
                    boolQueryBuilder.must(btw);
                    break;
                default:
                    MatchQueryBuilder matchQuery = QueryBuilders.matchQuery(tagId, compareValue);
                    boolQueryBuilder.must(matchQuery);
            }

        }

        // 将查询条件构造成一个查询请求
        request.source(new SearchSourceBuilder().query(boolQueryBuilder));

        // 用es客户端，向es发出查询请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();

        // System.out.println("耗时：" + response2.getTook());
        // System.out.println("命中条数：" + hits.getTotalHits());

        // 遍历搜索到的每一个结果,并添加到bitmap
        RoaringBitmap bitmap = RoaringBitmap.bitmapOf();
        for (SearchHit hit : hits) {
            bitmap.add(Integer.parseInt(hit.getId()));
        }

        return bitmap;
    }
}
