package com.linkedkeeper.elasticsearch.client;

import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface ElasticSearchService {

    /**
     * 判断索引是否存在
     */
    boolean existIndex(String indexName) throws IOException;

    /**
     * 删除索引
     */
    void deleteIndex(String indexName) throws IOException;

    /**
     * 删除索引
     */
    void deleteType(String indexName, String typeName) throws IOException;

    /**
     * 创建索引
     * 注意：在生产环节中通知es集群的owner去创建index
     */
    void createIndex(String indexName, String documentType, List<IndexField> fields, int expire) throws IOException;

    void createIndex(String indexName, String documentType, List<IndexField> fields, int expire, boolean routing, int shards, int replicas) throws IOException;

    /**
     * 用javabean构建document
     */
    <T> boolean indexWithBean(String indexName, String documentType, T t) throws Exception;

    <T> boolean indexWithBean(String indexName, String documentType, T t, String routingId) throws Exception;

    /**
     * 用javabean构建document，并自定义ID
     */
    <T> boolean indexWithBeanById(String id, String indexName, String documentType, T t) throws Exception;

    <T> boolean indexWithBeanById(String indexName, String documentType, String id, T t, String routingId) throws Exception;

    /**
     * 在索引indexName
     */
    List<Map<String, Object>> searchWithBooleanQuery(String indexName, String documentType, Map<String, String> termQuery) throws Exception;

    /**
     * 在索引indexName
     */
    List<Map<String, Object>> searchWithBooleanQuery(String indexName, String documentType,
                                                     Map<String, String> termQuery, int startIndex, int maxIndex, String sortBy, SortOrder sortOrder) throws Exception;

    /**
     * 在索引indexName
     */
    List<Map<String, Object>> searchWithBooleanQuery(String indexName, String documentType,
                                                     Map<String, String> termQuery, int startIndex, int maxIndex, String sortBy, SortOrder sortOrder, String routingId) throws Exception;

    List<Map<String, Object>> searchWithRangeQuery(String indexName, String documentType,
                                                   Map<String, String> termQuery, int startIndex, int maxIndex, String rangeName, long min, long max, int interval, String sortBy, SortOrder sortOrder, String routingId) throws Exception;

    /**
     * 在索引indexName
     */
    long searchWithCount(String indexName, String documentType, Map<String, String> termQuery) throws Exception;

    long searchWithCount(String indexName, String documentType,
                         Map<String, String> termQuery, String routingId) throws Exception;

    long searchWithRangeCount(String indexName, String documentType,
                              Map<String, String> termQuery, String rangeName, long min, long max, int interval, String routingId) throws Exception;
}
