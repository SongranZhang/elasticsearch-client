package com.linkedkeeper.elasticsearch.client;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by zhangsongran@linkedkeeper.com on 2019/1/27.
 */
public class TestClient<T> extends BaseEsClient<T> implements BaseEsService<T> {

    private static final Logger logger = Logger.getLogger(TestClient.class);

    public static final String INDEXNAME = "linkedkeeper";
    public static final String DOCUMENTTYPE = "test";

    public boolean createIndex() throws Exception {
        boolean success = false;
        try {
            List<IndexField> fields = new ArrayList<IndexField>();

            fields.add(new IndexField("id", "long", ElasticSearchFactory.INDEX_NOT_ANALYZED_VALUE));
            fields.add(new IndexField("title", "title", ElasticSearchFactory.INDEX_NO_VALUE));

            // 创建索引
            elasticSearchFactory.createIndex(INDEXNAME, DOCUMENTTYPE, fields, 90, true);

            success = true;
        } catch (Exception e) {
            logger.error("ForumThreadEsClient createIndex e -> ", e);
        }
        return success;
    }

    public boolean deleteIndex() throws Exception {
        return $deleteMsgIndex(INDEXNAME);
    }

    public boolean indexWith(T msg, String routingId) throws Exception {
        return $indexWithMsg(INDEXNAME, DOCUMENTTYPE, msg, routingId);
    }

    public boolean indexWith(String id, T msg, String routingId) throws Exception {
        return $indexWithMsg(id, INDEXNAME, DOCUMENTTYPE, msg, routingId);
    }

    public List<Map<String, Object>> searchWithBooleanQuery(Map<String, String> termQuery, int page, int size, String sortBy, String routingId) throws Exception {
        return $searchWithBooleanQuery(INDEXNAME, DOCUMENTTYPE, termQuery, page, size, sortBy, routingId);
    }

    public List<Map<String, Object>> searchWithRangeQueryPre(Map<String, String> termQuery, int page, int size, String rangeName, long min, long max, String sortBy, String routingId) throws Exception {
        return $searchWithPageRangeQuery(INDEXNAME, DOCUMENTTYPE, termQuery, page, size, rangeName, min, max, ElasticSearchFactory.PRE, sortBy, routingId);
    }

    public List<Map<String, Object>> searchWithRangeQueryAft(Map<String, String> termQuery, int page, int size, String rangeName, long min, long max, String sortBy, String routingId) throws Exception {
        return $searchWithPageRangeQuery(INDEXNAME, DOCUMENTTYPE, termQuery, page, size, rangeName, min, max, ElasticSearchFactory.AFT, sortBy, routingId);
    }

    public long searchWithCount(Map<String, String> termQuery, String routingId) throws Exception {
        return $searchWithCount(INDEXNAME, DOCUMENTTYPE, termQuery, routingId);
    }

    public long searchWithCountRangePre(Map<String, String> termQuery, String rangeName, long min, long max, String routingId) throws Exception {
        return $searchWithCountRange(INDEXNAME, DOCUMENTTYPE, termQuery, rangeName, min, max, ElasticSearchFactory.PRE, routingId);
    }

    public long searchWithCountRangeAft(Map<String, String> termQuery, String rangeName, long min, long max, String routingId) throws Exception {
        return $searchWithCountRange(INDEXNAME, DOCUMENTTYPE, termQuery, rangeName, min, max, ElasticSearchFactory.AFT, routingId);
    }
}
