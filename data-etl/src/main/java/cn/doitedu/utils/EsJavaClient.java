package cn.doitedu.utils;


import org.apache.http.HttpHost;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

public class EsJavaClient {

    public static void main(String[] args) throws IOException {

        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("doitedu", 9200, "http")));

        //------------------根据docid获取文档------------------
        //GetRequest request = new GetRequest().index("docs").id("1");
        //客户端发送请求，获取响应对象
        //GetResponse response = client.get(request, RequestOptions.DEFAULT);
        //System.out.println("index:" + response.getIndex());
        //System.out.println("type:" + response.getType());
        //System.out.println("id:" + response.getId());
        //System.out.println("source:" + response.getSourceAsString());

        // -----------搜索条件查询-------
        SearchRequest request = new SearchRequest("docs");

        // 精确查询
        //request.source(new SearchSourceBuilder().query(QueryBuilders.termQuery("tg04", "幼儿园")));

        // 全文检索，或精确查询（基本类型值）
        //request.source(new SearchSourceBuilder().query(QueryBuilders.matchQuery("tg04", "幼儿园")));

        // 范围查询
        //request.source(new SearchSourceBuilder().query(QueryBuilders.rangeQuery("tg01").gt(4)));

        // 多条件查询
        MatchQueryBuilder matchQueryBuilder1 = QueryBuilders.matchQuery("tg04", "幼儿园");
        MatchQueryBuilder matchQueryBuilder2 = QueryBuilders.matchQuery("tg04", "城市");
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(matchQueryBuilder1).should(matchQueryBuilder2);
        request.source(new SearchSourceBuilder().query(boolQueryBuilder));


        SearchResponse response2 = client.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response2.getHits();

        System.out.println("耗时：" + response2.getTook());
        System.out.println("命中条数：" + hits.getTotalHits());

        for (SearchHit hit : hits) {
            System.out.println("------------------");
            System.out.println(hit.getSourceAsString());
        }

        // 关闭ES客户端
        client.close();

    }


}
