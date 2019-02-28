package com.neighborhood.aka.laplace.estuary.bean.support;

import com.neighborhood.aka.laplace.estuary.core.util.JavaCommonUtil;
import org.bson.BsonTimestamp;
import org.bson.Document;

/**
 * Created by z on 17-4-5.
 * ts：8字节的时间戳，由4字节unix timestamp + 4字节自增计数表示。
 * 这个值很重要，在选举(如master宕机时)新primary时，会选择ts最大的那个secondary作为新primary。
 * op：1字节的操作类型，例如i表示insert，d表示delete。
 * ns：操作所在的namespace。
 * o：操作所对应的document,即当前操作的内容（比如更新操作时要更新的的字段和值）
 * o2: 在执行更新操作时的where条件，仅限于update时才有该属性
 * 其中op，可以是如下几种情形之一：
 * "i"： insert
 * "u"： update
 * "d"： delete
 * "c"： db cmd
 * "db"：声明当前数据库 (其中ns 被设置成为=>数据库名称+ '.')
 * "n": no op,即空操作，其会定期执行以确保时效性
 */
public class Oplog {
    private Document originalDocument;
    private Long id;
    private BsonTimestamp timestamp;
    private String namespace;
    private String dbName;
    private String tableName;
    private String operateType;
    private Document currentDocument;
    private Document whereCondition;


    public Oplog(Document originalDocument) {
        this.originalDocument = originalDocument;
        init();
    }

//    Long h = (Long) doc.get("h");
//    BsonTimestamp ts = (BsonTimestamp) doc.get("ts");
//    String dbAndTable = (String) doc.get("ns");
//    LOG.info(dbAndTable);
//    String mongoOp = (String) doc.get("op");
//    Document o = (Document) doc.get("o");
//    doc.get("o2")

    protected void init() {
        if (this.originalDocument == null) {
            throw new IllegalArgumentException("originalDocument 数据必填，否则无法解析oplog数据");
        }
        this.id = (Long) this.originalDocument.get("h");
        this.timestamp = (BsonTimestamp) this.originalDocument.get("ts");
        this.namespace = (String) this.originalDocument.get("ns");
        this.operateType = (String) this.originalDocument.get("op");
        this.currentDocument = (Document) this.originalDocument.get("o");
        this.whereCondition = (Document) this.originalDocument.get("o2");

        if(JavaCommonUtil.hasText(this.namespace)){
            int index = this.namespace.indexOf('.');
            this.dbName = this.namespace.substring(0, index);
            this.tableName = this.namespace.substring(index + 1);
        }
    }

    public Document getOriginalDocument() {
        return originalDocument;
    }

    public Long getId() {
        return id;
    }

    public BsonTimestamp getTimestamp() {
        return timestamp;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getOperateType() {
        return operateType;
    }

    public Document getCurrentDocument() {
        return currentDocument;
    }

    public Document getWhereCondition() {
        return whereCondition;
    }
}
