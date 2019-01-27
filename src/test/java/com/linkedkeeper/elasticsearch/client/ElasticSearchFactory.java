package com.jd.pop.datasearch.client;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

/**
 * Created by zhangsongran on 2016/9/19.
 */
public class ElasticSearchFactory implements ElasticSearchService {

    private static final Logger logger = Logger.getLogger(ElasticSearchFactory.class);

    // private Client client;
    private TransportClient client;

    private String clusterName;
    private Map<String, String> nodeInfo;

    public static final int PRE = 1;
    public static final int AFT = 2;

    /**
     * 创建es client
     * clusterName:集群名字
     * nodeIp:集群中节点的ip地址
     * nodePort:节点的端口
     */
    public void init() throws Exception {
        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", clusterName)
                .put("client.transport.sniff", false)
                .build();
        // 创建集群client
        client = TransportClient.builder().settings(settings).build();
        // 添加集群节点地址
        for (String nodeIp : nodeInfo.keySet()) {
            int nodePort = Integer.parseInt(nodeInfo.get(nodeIp));
            client.addTransportAddress(
                    new InetSocketTransportAddress(InetAddress.getByName(nodeIp), nodePort));
        }
        logger.info("init ElasticSearchConfig success. cluster.name :" + clusterName);
    }

    /**
     * 判断索引是否存在
     *
     * @param indexName
     * @return
     * @throws IOException
     */
    public boolean existIndex(String indexName) throws IOException {
        final IndicesExistsResponse iRes = client.admin().indices().prepareExists(indexName).execute().actionGet();
        return iRes.isExists();
    }

    private final String STORE_FIELD = "store";
    private final String TYPE_FIELD = "type";
    private final String INDEX_FIELD = "index";
    public final static String INDEX_NO_VALUE = "no";
    public final static String INDEX_NOT_ANALYZED_VALUE = "not_analyzed";
    public final static String INDEX_ANALYZED_VALUE = "analyzed";

    public final static String NO_ROUTING = null;
    public final static String NO_SORT = null;

    private final int START_INDEX = 0;
    private final int MAX_INDEX = 10;

    public void createIndex(String indexName, String documentType, Map<String, String> fields, int expire) throws IOException {
        List<IndexField> fields_ = new ArrayList<IndexField>();
        for (String name : fields.keySet()) {
            fields_.add(new IndexField(name, "string", fields.get(name)));
        }
        createIndex(indexName, documentType, fields_, expire, false);
    }

    public void createIndex(String indexName, String documentType, List<IndexField> fields, int expire) throws IOException {
        createIndex(indexName, documentType, fields, expire, false);
    }

    /**
     * 创建索引
     * 注意：在生产环节中通知es集群的owner去创建index
     */
    public void createIndex(String indexName, String documentType, List<IndexField> fields, int expire, boolean routing) throws IOException {
        createIndex(indexName, documentType, fields, expire, routing, 5, 1);
    }

    public void createIndex(String indexName, String documentType, List<IndexField> fields, int expire, boolean routing, int shards, int replicas) throws IOException {
        if (!existIndex(indexName)) {
            Settings settings = Settings.settingsBuilder().put("number_of_shards", shards).put("number_of_replicas", replicas).build();
            client.admin().indices().prepareCreate(indexName).setSettings(settings).execute().actionGet();
        }

        /**
         *  start
         */
        XContentBuilder mapping = jsonBuilder()
                .startObject()
                .startObject(documentType);

        /**
         * _all
         */
        mapping = mapping.startObject("_all")
                .field("enabled", true);
        mapping = mapping.endObject();
        /**
         * _timestamp
         */
        mapping = mapping.startObject("_timestamp")
                .field("enabled", true);
        mapping = mapping.endObject();
        /**
         * _ttl
         */
        if (expire > 0) {
            mapping = mapping.startObject("_ttl")
                    .field("enabled", true)
                    .field("default", expire + "d");
            mapping = mapping.endObject();
        }
        /**
         * _routing
         */
        if (routing) {
            mapping = mapping.startObject("_routing")
                    .field("required", true);
            mapping = mapping.endObject();
        }
        /**
         * properties
         */
        mapping = mapping.startObject("properties");
        for (IndexField indexField : fields) {
            mapping = mapping.startObject(indexField.getName())
                    .field(STORE_FIELD, true)
                    .field(TYPE_FIELD, indexField.getType())
                    .field(INDEX_FIELD, indexField.getIndex()).endObject();
        }
        mapping = mapping.endObject();
        /**
         * end
         */
        mapping = mapping.endObject()
                .endObject();

        logger.info("mapping -> " + mapping.string());

        client.admin().indices()
                .preparePutMapping(indexName)
                .setType(documentType)
                .setSource(mapping)
                .execute().actionGet();
    }

    /**
     * 删除索引
     */
    public void deleteIndex(String indexName) throws IOException {
        final IndicesExistsResponse iRes = client.admin().indices().prepareExists(indexName).execute().actionGet();
        if (iRes.isExists()) {
            client.admin().indices().prepareDelete(indexName).execute().actionGet();
        }
    }

    public void deleteType(String indexName, String typeName) throws IOException {
        TypesExistsResponse response = client.admin().indices().prepareTypesExists(indexName).setTypes(typeName).execute().actionGet();
        if (response.isExists()) {
            client.prepareDelete().setIndex(indexName).setType(typeName).execute().actionGet();
        }
    }

    public <T> boolean indexWithBean(String indexName, String documentType, T t) throws Exception {
        IndexRequestBuilder builder = client.prepareIndex(indexName, documentType);
        return $index(builder, t);
    }

    public <T> boolean indexWithBeanById(String id, String indexName, String documentType, T t) throws Exception {
        //指定索引名称，type名称和documentId(documentId可选，不设置则系统自动生成)创建document
        IndexRequestBuilder builder = client.prepareIndex(indexName, documentType, id);
        return $index(builder, t);
    }


    /**
     * 用javabean构建document
     */
    public <T> boolean indexWithBean(String indexName, String documentType, T t, String routingId) throws Exception {
        IndexRequestBuilder builder = client.prepareIndex(indexName, documentType);
        return $indexByRouting(builder, t, routingId);
    }

    /**
     * 用javabean构建document
     */
    public <T> boolean indexWithBeanById(String indexName, String documentType, String id, T t, String routingId) throws Exception {
        //指定索引名称，type名称和documentId(documentId可选，不设置则系统自动生成)创建document
        IndexRequestBuilder builder = client.prepareIndex(indexName, documentType, id);
        return $indexByRouting(builder, t, routingId);
    }

    private <T> boolean $index(IndexRequestBuilder builder, T t) throws Exception {
        //用javabean构建json对象
        ObjectMapper mapper = new ObjectMapper();
        byte[] json = mapper.writeValueAsBytes(t);

        IndexResponse response = builder.setSource(json)
                .execute()
                .actionGet();

        return $generateIndexResponse(response);
    }

    private <T> boolean $indexByRouting(IndexRequestBuilder builder, T t, String routingId) throws Exception {
        //用javabean构建json对象
        ObjectMapper mapper = new ObjectMapper();
        byte[] json = mapper.writeValueAsBytes(t);

        IndexResponse response = builder.setSource(json)
                .setRouting(routingId)
                .execute()
                .actionGet();

        return $generateIndexResponse(response);
    }

    private boolean $generateIndexResponse(IndexResponse response) {
        //response中返回索引名称，type名称，doc的Id和版本信息
//        String index = response.getIndex();
//        String type = response.getType();
//        String id = response.getId();
//        long version = response.getVersion();
        boolean created = response.isCreated();
        return created;
    }

    public List<Map<String, Object>> searchWithBooleanQuery(String indexName, String documentType, Map<String, String> termQuery) throws Exception {
        BoolQueryBuilder builder = $generateQueryBuilder(termQuery);
        SearchRequestBuilder searchRequestBuilder = $generateRequestBuilder(indexName, documentType, builder, START_INDEX, MAX_INDEX, NO_SORT, SortOrder.DESC, NO_ROUTING);
        return $searchWithBooleanQuery(searchRequestBuilder);
    }

    public List<Map<String, Object>> searchWithBooleanQuery(String indexName, String documentType, Map<String, String> termQuery, int startIndex, int maxIndex, String sortBy, SortOrder sortOrder) throws Exception {
        BoolQueryBuilder builder = $generateQueryBuilder(termQuery);
        SearchRequestBuilder searchRequestBuilder = $generateRequestBuilder(indexName, documentType, builder, startIndex, maxIndex, sortBy, sortOrder, NO_ROUTING);
        return $searchWithBooleanQuery(searchRequestBuilder);
    }

    /**
     * 根据索引分页查询并排序
     */
    public List<Map<String, Object>> searchWithBooleanQuery(String indexName, String documentType,
                                                            Map<String, String> termQuery, int startIndex, int maxIndex, String sortBy, SortOrder sortOrder, String routingId) throws Exception {
        BoolQueryBuilder builder = $generateQueryBuilder(termQuery);
        SearchRequestBuilder searchRequestBuilder = $generateRequestBuilder(indexName, documentType, builder, startIndex, maxIndex, sortBy, sortOrder, routingId);
        return $searchWithBooleanQuery(searchRequestBuilder);
    }

    /**
     * 在索引indexName
     */
    public List<Map<String, Object>> searchWithRangeQuery(String indexName, String documentType,
                                                          Map<String, String> termQuery, int startIndex, int maxIndex, String rangeName, long min, long max, int interval, String sortBy, SortOrder sortOrder, String routingId) throws Exception {
        BoolQueryBuilder builder = $generateRangeQueryBuilder(termQuery, rangeName, min, max, interval);
        SearchRequestBuilder searchRequestBuilder = $generateRequestBuilder(indexName, documentType, builder, startIndex, maxIndex, sortBy, sortOrder, routingId);
        return $searchWithBooleanQuery(searchRequestBuilder);
    }

    private SearchRequestBuilder $generateRequestBuilder(String indexName, String documentType, BoolQueryBuilder bq, int startIndex, int maxIndex, String sortBy, SortOrder sortOrder, String routingId) throws Exception {
        logger.error("ElasticSearchFactory $generateRequestBuilder -> client.prepareSearch : indexName -> " + indexName + ", documentType -> " + documentType + ", bq -> " + bq.toString());
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indexName).setTypes(documentType)
                //设置search type
                //常用search type用：query_then_fetch
                //query_then_fetch是先查到相关结构，然后聚合不同node上的结果后排序
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                //查询的termName和termValue
                .setQuery(bq);
        if (StringUtils.isNotBlank(routingId)) {
            //设置routing
            searchRequestBuilder.setRouting(routingId);
        }
        if (StringUtils.isNotBlank(sortBy)) {
            //设置排序field
            searchRequestBuilder.addSort(sortBy, sortOrder);
        }
        //设置分页
        searchRequestBuilder.setFrom(startIndex).setSize(maxIndex);

        return searchRequestBuilder;
    }

    private List<Map<String, Object>> $searchWithBooleanQuery(SearchRequestBuilder searchRequestBuilder) throws Exception {
        SearchResponse sResponse = searchRequestBuilder.execute().actionGet();
        return $generateSearchResponse(sResponse);
    }

    private List<Map<String, Object>> $generateSearchResponse(SearchResponse sResponse) {
//        int tShards = sResponse.getTotalShards();
//        long timeCost = sResponse.getTookInMillis();
//        int sShards = sResponse.getSuccessfulShards();
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        SearchHits hits = sResponse.getHits();
        long count = hits.getTotalHits();
        if (count > 0) {
            for (SearchHit hit : hits.getHits()) {
                Map<String, Object> map = new HashMap<String, Object>();
                Map<String, Object> fields = hit.getSource();
                for (String key : fields.keySet()) {
                    map.put(key, fields.get(key));
                }
                map.put("id", hit.getId());
                map.put("index", hit.getIndex());
                map.put("type", hit.getType());
                list.add(map);
            }
        }
        return list;
    }

    public long searchWithCount(String indexName, String documentType, Map<String, String> termQuery) throws Exception {
        BoolQueryBuilder bq = $generateQueryBuilder(termQuery);
        return $generateCountResponse(indexName, documentType, bq, NO_ROUTING);
    }

    public long searchWithCount(String indexName, String documentType,
                                Map<String, String> termQuery, String routingId) throws Exception {
        BoolQueryBuilder bq = $generateQueryBuilder(termQuery);
        return $generateCountResponse(indexName, documentType, bq, routingId);
    }

    public long searchWithRangeCount(String indexName, String documentType,
                                     Map<String, String> termQuery, String rangeName, long min, long max, int interval, String routingId) throws Exception {
        BoolQueryBuilder bq = $generateRangeQueryBuilder(termQuery, rangeName, min, max, interval);
        return $generateCountResponse(indexName, documentType, bq, routingId);
    }

    private long $generateCountResponse(String indexName, String documentType, BoolQueryBuilder bq, String routingId) {
        CountRequestBuilder builder = client.prepareCount(indexName).setTypes(documentType).setQuery(bq);
        if (StringUtils.isNotBlank(routingId))
            builder.setRouting(routingId);

        return builder.execute().actionGet().getCount();
    }

    private BoolQueryBuilder $generateQueryBuilder(Map<String, String> termQuery) {
        //构建boolean query
        BoolQueryBuilder bq = boolQuery();
        for (Map.Entry<String, String> term : termQuery.entrySet()) {
            String termName = term.getKey();
            String termValue = term.getValue();

            bq.must(termQuery(termName, termValue));
        }
        return bq;
    }

    private BoolQueryBuilder $generateRangeQueryBuilder(Map<String, String> termQuery, String rangeName, long min, long max, int interval) {
        BoolQueryBuilder builder = $generateQueryBuilder(termQuery);
        if (interval == PRE) {
            QueryBuilder range = QueryBuilders.rangeQuery(rangeName).gte(min).lt(max);
            builder.must(range);
        } else if (interval == AFT) {
            QueryBuilder range = QueryBuilders.rangeQuery(rangeName).gt(min).lte(max);
            builder.must(range);
        }
        return builder;
    }

    public Map<String, String> getNodeInfo() {
        return nodeInfo;
    }

    public void setNodeInfo(Map<String, String> nodeInfo) {
        this.nodeInfo = nodeInfo;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }
}
