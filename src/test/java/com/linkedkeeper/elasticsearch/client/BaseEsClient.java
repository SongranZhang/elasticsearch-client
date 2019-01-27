package com.jd.pop.datasearch.client;

import org.apache.log4j.Logger;
import org.elasticsearch.search.sort.SortOrder;

import java.util.List;
import java.util.Map;

/**
 * Created by zhangsongran on 2016/9/19.
 */
public abstract class BaseEsClient<T> {

    private static final Logger logger = Logger.getLogger(BaseEsClient.class);

    protected ElasticSearchFactory esFactory;

    public void setEsFactory(ElasticSearchFactory esFactory) {
        this.esFactory = esFactory;
    }

    protected boolean $deleteMsgIndex(String indexName) throws Exception {
        boolean success = false;
        try {
            esFactory.deleteIndex(indexName);
            success = true;
        } catch (Exception e) {
            logger.error("GwEsClient deleteMsgIndex e -> ", e);
        }
        return success;
    }

    protected boolean $indexWithMsg(String indexName, String documentType, T msg, String routingId) throws Exception {
        boolean success = false;
        try {
            success = esFactory.indexWithBean(indexName, documentType, msg, routingId);
        } catch (Exception e) {
            logger.error("GwEsClient indexWithMsg msg -> " + msg.toString() + ", e -> ", e);
        }
        return success;
    }

    protected boolean $indexWithMsg(String id, String indexName, String documentType, T msg, String routingId) throws Exception {
        boolean success = false;
        try {
            success = esFactory.indexWithBeanById(id, indexName, documentType, msg, routingId);
        } catch (Exception e) {
            logger.error("GwEsClient indexWithMsg id -> " + id + ", msg -> " + msg.toString() + ", e -> ", e);
        }
        return success;
    }

    protected List<Map<String, Object>> $searchWithBooleanQuery(String indexName, String documentType, Map<String, String> termQuery, int page, int size, String sortBy, String routingId) throws Exception {
        List<Map<String, Object>> data = null;
        try {
            int from = getFromPage(page, size);
            data = esFactory.searchWithBooleanQuery(indexName, documentType, termQuery, from, size, sortBy, SortOrder.DESC, routingId);
        } catch (Exception e) {
            logger.error("GwEsClient searchWithBooleanQueryPageOrderBy e -> ", e);
        }
        return data;
    }

    protected List<Map<String, Object>> $searchWithPageRangeQuery(String indexName, String documentType, Map<String, String> termQuery, int page, int size, String rangeName, long min, long max, int interval, String sortBy, String routingId) throws Exception {
        List<Map<String, Object>> data = null;
        try {
            int from = getFromPage(page, size);
            data = esFactory.searchWithRangeQuery(indexName, documentType, termQuery, from, size, rangeName, min, max, interval, sortBy, SortOrder.DESC, routingId);
        } catch (Exception e) {
            logger.error("GwEsClient searchWithBooleanQueryPageOrderByRange e -> ", e);
        }
        return data;
    }

    private int getFromPage(int page, int size) {
        int from = 0;
        if (page > 0 && size > 0) {
            from = (page - 1) * size;
        }
        return from;
    }

    protected long $searchWithCount(String indexName, String documentType, Map<String, String> termQuery, String routingId) throws Exception {
        return esFactory.searchWithCount(indexName, documentType, termQuery, routingId);
    }

    protected long $searchWithCountRange(String indexName, String documentType, Map<String, String> termQuery, String rangeName, long min, long max, int interval, String routingId) throws Exception {
        return esFactory.searchWithRangeCount(indexName, documentType, termQuery, rangeName, min, max, interval, routingId);
    }
}
