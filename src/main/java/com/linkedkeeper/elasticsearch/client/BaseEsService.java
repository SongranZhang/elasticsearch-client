package com.linkedkeeper.elasticsearch.client;

import java.util.List;
import java.util.Map;

public interface BaseEsService<T> {

    boolean deleteIndex() throws Exception;

    boolean createIndex() throws Exception;

    boolean indexWith(T obj, String routingId) throws Exception;

    boolean indexWith(String id, T obj, String routingId) throws Exception;

    List<Map<String, Object>> searchWithBooleanQuery(Map<String, String> termQuery, int page, int size, String sortBy, String routingId) throws Exception;

    List<Map<String, Object>> searchWithRangeQueryPre(Map<String, String> termQuery, int page, int size, String rangeName, long min, long max, String sortBy, String routingId) throws Exception;

    List<Map<String, Object>> searchWithRangeQueryAft(Map<String, String> termQuery, int page, int size, String rangeName, long min, long max, String sortBy, String routingId) throws Exception;

    long searchWithCount(Map<String, String> termQuery, String routingId) throws Exception;

    long searchWithCountRangePre(Map<String, String> termQuery, String rangeName, long min, long max, String routingId) throws Exception;

    long searchWithCountRangeAft(Map<String, String> termQuery, String rangeName, long min, long max, String routingId) throws Exception;

}
