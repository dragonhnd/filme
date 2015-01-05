
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.mongodb.WriteResult;
import org.jongo.Find;
import org.jongo.FindOne;
import org.jongo.MongoCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DBInteractor
{
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(DBInteractor.class);

    private final MongoCollection collection;
    
    public DBInteractor(final MongoCollection collection){
    	this.collection = collection;
    }
    
    protected MongoCollection getCollection() {
		return collection;
	}

	protected long count(DBParam...dbParams)
    {
        if(dbParams != null)
        {
            StringBuilder sb = new StringBuilder();
            sb.append(getQueryString(dbParams));
            Object[] paramValues = getParamValues(dbParams);
            if(paramValues.length > 0) 
                return collection.count(sb.toString(), paramValues);
            else 
                return collection.count(sb.toString());
        }else
        {
            return collection.count();
        }        
    }
        
    public WriteResult update(Map<String, Object> fieldsToUpdate, DBParam...queryParams)
    {
    	return update(fieldsToUpdate, true, queryParams);
    }
    
    public WriteResult update(Map<String, Object> fieldsToUpdate, boolean UpdateMultipleDocs, DBParam...queryParams)
    {
    	String query = getQueryString(queryParams);
        Object[] queryValues = getParamValues(queryParams);
        Object[] updateValues = new Object[fieldsToUpdate.size()];
        String update = getUpdateString(fieldsToUpdate, updateValues);
        if(UpdateMultipleDocs){
        	return collection.update(query, queryValues).multi().with(update, updateValues);
        }else{
            return collection.update(query, queryValues).with(update, updateValues);
        }
    }
    
    public void remove(DBParam...dbParams)
    {
        String query = getQueryString(dbParams);
        Object[] paramValues = getParamValues(dbParams);
        collection.remove(query, paramValues);
    }
    
    public FindOne findOne(List<String> fieldsToRetrieve, DBParam...dbParams)
    {
        String query = getQueryString(dbParams);
        Object[] paramValues = getParamValues(dbParams);
        return findOne(fieldsToRetrieve, query, paramValues);
    }
    
    public FindOne findOne(List<String> fieldsToRetrieve, String query, Object[] paramValues){
        FindOne findOne = collection.findOne(query, paramValues);
        if(fieldsToRetrieve != null && fieldsToRetrieve.size() > 0)
            findOne.projection(getFields(fieldsToRetrieve));
        return findOne;
    }
            
    public Find findAll(DBParam...dbParams)
    {
        String query = getQueryString(dbParams);
        Object[] paramValues = getParamValues(dbParams);
        return find(0, 0, null, null, query, paramValues);
    }
    
    public Find findAll(List<String> fieldsToRetrieve, Map<String, String> orderByFields, DBParam...dbParams)
    {
        String query = getQueryString(dbParams);
        Object[] paramValues = getParamValues(dbParams);
        return find(0, 0, fieldsToRetrieve, orderByFields, query, paramValues);
    }
    
    public Find find(int skip, int limit, List<String> fieldsToRetrieve, 
            Map<String, String> orderByFields, DBParam...dbParams)
    {
        String query = getQueryString(dbParams);
        Object[] paramValues = getParamValues(dbParams);
        //if(log.isDebugEnabled())log.debug("query {}, params {}", query, paramValues.length);
        return find(skip, limit, fieldsToRetrieve, orderByFields, query, paramValues);
    }
    
    public Find find(int skip, int limit, List<String> fieldsToRetrieve, 
            Map<String, String> orderByFields, String query, Object[] paramValues)
    {
        Find find = collection.find(query, paramValues);
        if(fieldsToRetrieve != null && fieldsToRetrieve.size() > 0)
            find.projection(getFields(fieldsToRetrieve));
        if(orderByFields != null && orderByFields.size() > 0)
            find.sort(getOrderBy(orderByFields));
        if(limit > 0)
        	return find.skip(skip).limit(limit);
        else
        	return find;
    }
     
    //"{$set: {"+fieldToUpdate+": #}}"
    public static String getUpdateString(Map<String, Object> fieldsToUpdate, Object[] updateValues)
    {
        int i = 0, size = fieldsToUpdate.size();
        StringBuilder sb = new StringBuilder();
        sb.append("{$set: {");
        for(String key: fieldsToUpdate.keySet())
        {
        	updateValues[i] = fieldsToUpdate.get(key);
            sb.append(key).append(": #");
            if(i < (size -1)) sb.append(", ");
            i++;
        }
        sb.append("}}");
        return sb.toString();
    }
            
    public static String getQueryString(DBParam...dbParams)
    {
        int i = 0, size = dbParams.length;
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for(DBParam param: dbParams)
        {
            sb.append(param.getParamName()).append(": ").append(param.getOperator().getQuerySign());
            if(i < (size -1)) sb.append(", ");
            i++;
        }
        sb.append("}");
        //if(log.isDebugEnabled())log.debug("query {}", sb.toString());
        return sb.toString();
    }
    
    public static Object[] getParamValues(DBParam...dbParams)
    {
        List<Object> paramValues = new ArrayList<Object>();
        for(DBParam param: dbParams)
        {
            paramValues.add(param.getParamValue());
        }
        return paramValues.toArray();
    }
    
    public static String getFields(List<String> fieldsToRetrieve)
    {
        int i = 0, size = fieldsToRetrieve.size();
        StringBuilder sb = new StringBuilder();
        sb.append("{ _id: 1, ");
        for(String field: fieldsToRetrieve)
        {
            sb.append(field).append(": 1");
            if(i < (size -1)) sb.append(", ");
            i++;
        }
        sb.append(" }");
        return sb.toString();
    }
    
    public static String getOrderBy(Map<String, String> orderByFields)
    {
        int i = 0, size = orderByFields.size();
        StringBuilder sb = new StringBuilder();
        sb.append("{ _id: 1, ");
        for(String field: orderByFields.keySet())
        {
            sb.append(field).append(": ").append(orderByFields.get(field));
            if(i < (size -1)) sb.append(", ");
            i++;
        }
        sb.append(" }");
        return sb.toString();
        
    }
}



public enum DBOperator
{
    
    EQ("=", "#"), NEQ("!=", "{ $ne : #}"), EXISTS("exists", "{$exists : #}"), IN("in", "{$in :#}"), GTE(">=","{$gte :#}"), NIN("not in", "{$nin :#}");

    public static final String ASC = "1";
    public static final String DESC = "-1";
    
    private final String userSign;
    private final String querySign;
    
    private DBOperator(String userSign, String querySign)
    {
        this.userSign = userSign;
        this.querySign = querySign;
    }

    public String getUserSign()
    {
        return userSign;
    }

    public String getQuerySign()
    {
        return querySign;
    }

    
}



public class DBParam
{
    private final String paramName;
    private final DBOperator operator;
    private final Object paramValue;
    
    public DBParam(String paramName, DBOperator operator, Object paramValue)
    {
        super();
        this.paramName = paramName;
        this.operator = operator;
        this.paramValue = paramValue;
    }

    public String getParamName()
    {
        return paramName;
    }

    public Object getParamValue()
    {
        return paramValue;
    }

    public DBOperator getOperator()
    {
        return operator;
    }
    
    
}



import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.net.ssl.SSLSocketFactory;

import org.jongo.Jongo;
import org.jongo.MongoCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
/*
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
*/
import com.mongodb.ServerAddress;
import com.mongodb.gridfs.GridFS;

public class DBStore {
    private Logger logger = LoggerFactory.getLogger(DBStore.class);
    
    private Jongo db;
    private final Map<String, MongoCollection> collections;
    private GridFS gridFS;
    public DBStore(String host, int port, String database) throws Exception {
    	if(logger.isInfoEnabled()) logger.info("Mongo connecting to db {} instance {}", host, database); 
        this.collections = new HashMap<String, MongoCollection>();
        this.db = new Jongo(new MongoClient(new ServerAddress(host, port)).getDB(database));
    }
    public DBStore(String host, int port, String database,String userName, String password) throws Exception {
        this.collections = new HashMap<String, MongoCollection>();
        String url = "mongodb://"+userName+":"+password+"@"+host+":"+port+"/?ssl=true&authMechanism=MONGODB-CR";
    	if(logger.isInfoEnabled()) logger.info("Mongo connecting to {}", url); 
    	
        MongoClientOptions options = new MongoClientOptions.Builder().socketFactory(SSLSocketFactory.getDefault()).build();
        MongoClient client = new MongoClient(new MongoClientURI(url));
        List<MongoCredential> credList = new ArrayList<MongoCredential>();
        credList.add(MongoCredential.createMongoCRCredential(userName, database, password.toCharArray()));
        //this.db = new Jongo(new MongoClient(new ServerAddress(host, port), credList, options).getDB(database)); 
        this.db = new Jongo(new MongoClient(new ServerAddress(host, port), credList).getDB(database));
    }
    public void shutdown()
    {
        this.db.getDatabase().getMongo().close();
    }
    
    public MongoCollection getCollection(String name){
    	if(!collections.containsKey(name)){
            this.collections.put(name, this.db.getCollection(name));
    	}
        return this.collections.get(name);
    }
    
    private void forFutureDBStore(String host, int port, String userName, String password, 
            String database, List<String> collectionNames) throws Exception {
        //String url = "mongodb://"+userName+":"+password+"@"+host+":"+port+"/?ssl=true&authMechanism=MONGODB-CR";
        //String url = "mongodb://"+userName+":"+password+"@"+host+":"+port+"/?ssl=true";
        //logger.info("mongo url {} database", url, database);
        
        /*
        MongoClientOptions options = new MongoClientOptions.Builder().socketFactory(SSLSocketFactory.getDefault()).build();
        MongoClient client = new MongoClient(new MongoClientURI(url));
        List<MongoCredential> credList = new ArrayList<MongoCredential>();
        credList.add(MongoCredential.createMongoCRCredential(userName, database, password.toCharArray()));
        //this.db = new Jongo(new MongoClient(new ServerAddress(host, port), credList, this.options).getDB(database)); 
        this.db = new Jongo(new MongoClient(new ServerAddress(host, port), credList).getDB(database));
        this.db = new Jongo(client.getDB(database));
        */
        /*
        Mongo mongo = new Mongo(host, port);
        DB tmpDB = mongo.getDB(database);
        if(userName != null && userName.length() > 0){
            tmpDB.authenticate(userName, password.toCharArray());
        }
        this.db = new Jongo(tmpDB);
        */
        //MongoClientOptions options = new MongoClientOptions.Builder().socketFactory(SSLSocketFactory.getDefault()).connectTimeout(30 * 1000).build();
        //this.db = new Jongo(new MongoClient(new ServerAddress(host, port)).getDB(database));
        //gridFS = new GridFS(db, "photo");
        //logger.info("mongo connected");
        //this.collections.get("scenarioholder").ensureIndex(DocScenarioHolder.SCENARIO_PAGE_ID);
    }

}



import com.citi.risk.rcast.scenario.manager.mongo.dao.db.DBInteractor;
import com.citi.risk.rcast.scenario.manager.mongo.dao.db.DBOperator;
import com.citi.risk.rcast.scenario.manager.mongo.dao.db.DBParam;
import com.citi.risk.rcast.scenario.manager.mongo.dao.db.DBStore;
import com.citi.risk.rcast.scenario.manager.mongo.model.base.DocIdEntity;

import com.mongodb.WriteResult;
import org.apache.commons.collections.CollectionUtils;
import org.jongo.MongoCursor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BaseDAO<T extends DocIdEntity> extends DBInteractor
{
	private final Class<T> claz;
    
    public BaseDAO(Class<T> claz, DBStore dbStore){
    	this(claz, dbStore, claz.getSimpleName().toLowerCase());
    }
    
    public BaseDAO(Class<T> claz, DBStore dbStore, String collectionName)
    {
    	super(dbStore.getCollection(collectionName));
        this.claz = claz;
    }
    
    public WriteResult save(T model)
    {
        return getCollection().save(model);
    }

    public void insert(Collection<T> models) { getCollection().insert(models.toArray());}

    public void delete(T model)
    {
        getCollection().remove("{_id: '"+model.getId()+"'}");
    }
    
    public void delete(String id)
    {
        getCollection().remove("{_id: '"+id+"'}");
    }
    
    public void delete(DBParam... dbParams)
    {
        remove(dbParams);
    }
    
    public long countById(T model)
    {
        return count(new DBParam("_id", DBOperator.EQ, model.getId()));
    }
    
    public long countByQuery(DBParam...dbParams)
    {
        return count(dbParams);
    }
    
    public T get(String id)
    {
        return findOne(null, new DBParam(DocIdEntity.ID, DBOperator.EQ, id)).as(claz);
    }
    
    public T get(String field, Object value)
    {
        return findOne(null, new DBParam(field, DBOperator.EQ, value)).as(claz);
    }
    
    public T get(String query, Object[] paramValues)
    {
        return findOne(null, query, paramValues).as(claz);
    }
    
    public T get(DBParam... dbParams)
    {
        return findOne(null, dbParams).as(claz);
    }
    
    public List<T> list(String field, Object value)
    {
        List<T> targetList = new ArrayList<T>();
        CollectionUtils.addAll(targetList, find(0, 0, null, null, new DBParam(field, DBOperator.EQ, value)).as(claz).iterator());
        return targetList;
    }
    
    public List<T> list(DBParam... dbParams)
    {
        List<T> targetList = new ArrayList<T>();
        CollectionUtils.addAll(targetList, find(0, 0, null, null, dbParams).as(claz).iterator());
        return targetList;
    }
    
    public List<T> listForPerformance(DBParam... dbParams)
    {
        List<T> targetList = new ArrayList<T>();
        MongoCursor<T> cursor = find(0, 0, null, null, dbParams).as(claz);
        int i=0;
        while(cursor.hasNext()){
        	i++;
        	log.debug("listForPerformance retrieving document start {}", i);
        	targetList.add(cursor.next());
        	log.debug("listForPerformance retrieving document end {}", i);
        }
        return targetList;
    }
    
    public List<T> list(String query, Object[] paramValues)
    {
        List<T> targetList = new ArrayList<T>();
        CollectionUtils.addAll(targetList, find(0, 0, null, null, query, paramValues).as(claz).iterator());
        return targetList;
    }
    
    public List<T> listAll(DBParam... dbParams) {
    	List<T> targetList = new ArrayList<T>();
    	CollectionUtils.addAll(targetList, findAll(dbParams).as(claz).iterator());
    	return targetList;
    }
    
    public Iterable<T> query(int skip, int limit)
    {
        return find(skip, limit, null, null).as(claz);
    }
    
    public Iterable<T> queryByDBParams(int skip, int limit, DBParam...dbParams)
    {
        return find(skip, limit, null, null, dbParams).as(claz);
    }
    
    public Iterable<T> queryByQueryString(int skip, int limit, String query, Object[] paramValues)
    {
        return find(skip, limit, null, null, query, paramValues).as(claz);
    }
    
    public List<T> queryListWithOrderByAndDBParams(Map<String, String> orderByFields, DBParam...dbParams){
    	List<T> targetList = new ArrayList<T>();
        CollectionUtils.addAll(targetList, findAll(null, orderByFields, dbParams).as(claz).iterator());
        return targetList;
    }
    public List<T> queryList(int skip, int limit)
    {
        List<T> targetList = new ArrayList<T>();
        CollectionUtils.addAll(targetList, find(skip, limit, null, null).as(claz).iterator());
        return targetList;
    }
    
    public List<T> queryCompleteListByDBParams(DBParam...dbParams)
    {
        List<T> targetList = new ArrayList<T>();
        CollectionUtils.addAll(targetList, findAll(dbParams).as(claz).iterator());
        return targetList;
    }
    
    public List<T> queryListByDBParams(int skip, int limit, DBParam...dbParams)
    {
        List<T> targetList = new ArrayList<T>();
        CollectionUtils.addAll(targetList, find(skip, limit, null, null, dbParams).as(claz).iterator());
        return targetList;
    }
    
    public List<T> queryCustomFieldsByDBParams(List<String> fieldsToRetrieve, DBParam...dbParams)
    {
        List<T> targetList = new ArrayList<T>();        
        CollectionUtils.addAll(targetList, findAll(fieldsToRetrieve, null, dbParams).as(claz).iterator());        
        return targetList;
    }
    
    public List<T> queryListByQueryString(int skip, int limit, String query, Object[] paramValues)
    {
        List<T> targetList = new ArrayList<T>();
        CollectionUtils.addAll(targetList, find(skip, limit, null, null, query, paramValues).as(claz).iterator());
        return targetList;
    }
    
    public void increment(String id, String fieldToIncrement, int incrementValue)
    {
        getCollection().update("{_id: '"+id+"'}").with("{$inc: {"+fieldToIncrement+": #}}", incrementValue);
    }
    
    public void decrement(String id, String fieldToIncrement, int decrementValue)
    {
        increment(id, fieldToIncrement, (decrementValue < 0)?decrementValue:-(decrementValue));
    }
    
    public void update(String id, String fieldToUpdate, Object updateValue)
    {
        getCollection().update("{_id: '"+id+"'}").with("{$set: {"+fieldToUpdate+": #}}", updateValue);
    }
    
    public void update(String id, T t)
    {
    	getCollection().update("{_id: '"+id+"'}").with(t);
    }
}
