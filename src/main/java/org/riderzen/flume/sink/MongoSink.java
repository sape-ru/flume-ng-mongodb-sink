package org.riderzen.flume.sink;

import com.google.common.base.Throwables;
import com.mongodb.*;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * User: guoqiang.li
 * Date: 12-9-12
 * Time: 下午3:31
 */
public class MongoSink extends AbstractSink implements Configurable {
    private static Logger logger = LoggerFactory.getLogger(MongoSink.class);

    private static DateTimeParser[] parsers = {
            DateTimeFormat.forPattern("yyyy-MM-dd").getParser(),
            DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").getParser(),
            DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").getParser(),
            DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss Z").getParser(),
            DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS Z").getParser(),
            DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ").getParser(),
            DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ").getParser(),
            DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssz").getParser(),
            DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSz").getParser(),
    };
    public static DateTimeFormatter dateTimeFormatter = new DateTimeFormatterBuilder().append(null, parsers).toFormatter();

    public static final String HOST = "host";
    public static final String PORT = "port";
    public static final String AUTHENTICATION_ENABLED = "authenticationEnabled";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String MODEL = "model";
    public static final String DB_NAME = "db";
    public static final String COLLECTION = "collection";
    public static final String NAME_PREFIX = "MongSink_";
    public static final String BATCH_SIZE = "batch";
    public static final String AUTO_WRAP = "autoWrap";
    public static final String WRAP_FIELD = "wrapField";
    public static final String TIMESTAMP_FIELD = "timestampField";
    public static final String OPERATION = "op";
    public static final String PK = "_id";
    public static final String OP_INC = "$inc";
    public static final String OP_SET = "$set";
    public static final String OP_SET_ON_INSERT = "$setOnInsert";
    public static final String OP_MAX = "$max";
    public static final String CONFIG_SERIALIZER = "serializer";

    public static final boolean DEFAULT_AUTHENTICATION_ENABLED = false;
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 27017;
    public static final String DEFAULT_DB = "events";
    public static final String DEFAULT_COLLECTION = "events";
    public static final int DEFAULT_BATCH = 100;
    private static final Boolean DEFAULT_AUTO_WRAP = false;
    public static final String DEFAULT_WRAP_FIELD = "log";
    public static final String DEFAULT_TIMESTAMP_FIELD = null;
    public static final char NAMESPACE_SEPARATOR = '.';
    public static final String OP_UPSERT = "upsert";
    public static final String EXTRA_FIELDS_PREFIX = "extraFields.";
    public static final String DEFAULT_SERIALIZER = "org.riderzen.flume.sink.MongoSimpleEventSerializer";

    private static AtomicInteger counter = new AtomicInteger();

    private Mongo mongo;
    private DB db;

    private String host;
    private int port;
    private boolean authentication_enabled;
    private String username;
    private String password;
    private CollectionModel model;
    private String dbName;
    private String collectionName;
    private int batchSize;
    boolean autoWrap;
    String wrapField;
    String timestampField;
    final Map<String, String> extraInfos = new ConcurrentHashMap<String, String>();
    private MongoEventSerializer serializer;
    @Override
    public void configure(Context context) {
        setName(NAME_PREFIX + counter.getAndIncrement());

        host = context.getString(HOST, DEFAULT_HOST);
        port = context.getInteger(PORT, DEFAULT_PORT);
        authentication_enabled = context.getBoolean(AUTHENTICATION_ENABLED, DEFAULT_AUTHENTICATION_ENABLED);
        if (authentication_enabled) {
            username = context.getString(USERNAME);
            password = context.getString(PASSWORD);
        } else {
            username = "";
            password = "";
        }
        model = CollectionModel.valueOf(context.getString(MODEL, CollectionModel.SINGLE.name()));
        dbName = context.getString(DB_NAME, DEFAULT_DB);
        collectionName = context.getString(COLLECTION, DEFAULT_COLLECTION);
        batchSize = context.getInteger(BATCH_SIZE, DEFAULT_BATCH);
        autoWrap = context.getBoolean(AUTO_WRAP, DEFAULT_AUTO_WRAP);
        wrapField = context.getString(WRAP_FIELD, DEFAULT_WRAP_FIELD);
        timestampField = context.getString(TIMESTAMP_FIELD, DEFAULT_TIMESTAMP_FIELD);
        extraInfos.putAll(context.getSubProperties(EXTRA_FIELDS_PREFIX));
        String eventSerializerType = context.getString(CONFIG_SERIALIZER, DEFAULT_SERIALIZER);
        try {
            Class<? extends MongoEventSerializer> clazz =
                    Class.forName(eventSerializerType).asSubclass(MongoEventSerializer.class);
            serializer = clazz.newInstance();
        } catch (Exception e) {
            logger.error("Could not instantiate event serializer.", e);
            throw new IllegalArgumentException("Could not instantiate event serializer", e);
        }
        logger.info("MongoSink {} context { host:{}, port:{}, authentication_enabled:{}, username:{}, password:{}, model:{}, dbName:{}, collectionName:{}, batch: {}, autoWrap: {}, wrapField: {}, timestampField: {}, serializer: {} }",
                new Object[]{getName(), host, port, authentication_enabled, username, password, model, dbName, collectionName, batchSize, autoWrap, wrapField, timestampField, serializer.getClass().getSimpleName()});
    }

    @Override
    public synchronized void start() {
        logger.info("Starting {}...", getName());
        try {
            mongo = new Mongo(host, port);
            db = mongo.getDB(dbName);
        } catch (UnknownHostException e) {
            logger.error("Can't connect to mongoDB", e);
            return;
        }
        if (authentication_enabled) {
            boolean result = db.authenticate(username, password.toCharArray());
            if (result) {
                logger.info("Authentication attempt successful.");
            } else {
                logger.error("CRITICAL FAILURE: Unable to authenticate. Check username and Password, or use another unauthenticated DB. Not starting MongoDB sink.\n");
                return;
            }
        }
        super.start();
        logger.info("Started {}.", getName());
    }

    @Override
    public Status process() throws EventDeliveryException {
        logger.debug("{} start to process event", getName());

        Status status = Status.READY;
        try {
            status = parseEvents();
        } catch (Exception e) {
            logger.error("can't process events", e);
        }
        logger.debug("{} processed event", getName());
        return status;
    }

    private void saveEvents(Map<String, List<DBObject>> eventMap) {
        if (eventMap.isEmpty()) {
            logger.debug("eventMap is empty");
            return;
        }

        for (Map.Entry<String, List<DBObject>> entry : eventMap.entrySet()) {
            List<DBObject> docs = entry.getValue();
            if (logger.isDebugEnabled()) {
                logger.debug("collection: {}, length: {}", entry.getKey(), docs.size());
            }
            int separatorIndex = entry.getKey().indexOf(NAMESPACE_SEPARATOR);
            String eventDb = entry.getKey().substring(0, separatorIndex);
            String collectionNameVar = entry.getKey().substring(separatorIndex + 1);

            //Warning: please change the WriteConcern level if you need high datum consistence.
            DB dbRef = mongo.getDB(eventDb);
            if (authentication_enabled) {
                boolean authResult = dbRef.authenticate(username, password.toCharArray());
                if (!authResult) {
                    logger.error("Failed to authenticate user: " + username + " with password: " + password + ". Unable to write events.");
                    return;
                }
            }
			try {
				CommandResult result = dbRef.getCollection(collectionNameVar)
						.insert(docs, WriteConcern.SAFE).getLastError();
				if (result.ok()) {
					String errorMessage = result.getErrorMessage();
					if (errorMessage != null) {
						logger.error("can't insert documents with error: {} ",
								errorMessage);
						logger.error("with exception", result.getException());
						throw new MongoException(errorMessage);
					}
				} else {
					logger.error("can't get last error");
				}
			} catch (Exception e) {
				if (!(e instanceof com.mongodb.MongoException.DuplicateKey)) {
					logger.error("can't process event batch ", e);
				    logger.debug("can't process doc:{}", docs);
				}
				for (DBObject doc : docs) {
					try {
						dbRef.getCollection(collectionNameVar).insert(doc,
								WriteConcern.SAFE);
					} catch (Exception ee) {
						if (!(e instanceof com.mongodb.MongoException.DuplicateKey)) {
							logger.error(doc.toString());
							logger.error("can't process events, drop it!", ee);
						}
					}
				}
			}
		}
	}

    private Status parseEvents() throws EventDeliveryException {
        Status status = Status.READY;
        Map<String, List<DBObject>> eventMap = new HashMap<String, List<DBObject>>();
        Map<String, List<DBObject>> upsertMap = new HashMap<String, List<DBObject>>();

        Channel channel = getChannel();
        Transaction tx = channel.getTransaction();
        tx.begin();
        try {
            for (int i = 0; i < batchSize; i++) {
                Event event = channel.take();
                if (event == null) {
                    status = Status.BACKOFF;
                    break;
                } else {
                    this.serializer.addHeaders(event);
                    String operation = event.getHeaders().get(OPERATION);
                    if (logger.isDebugEnabled()) {
                        logger.debug("event operation is {}", operation);
                    }
                    if (OP_UPSERT.equalsIgnoreCase(operation)) {
                        processEvent(upsertMap, event);
                    } else if (StringUtils.isNotBlank(operation)) {
                        logger.error("non-supports operation {}", operation);
                    } else {
                        processEvent(eventMap, event);
                    }
                }
            }
            if (!eventMap.isEmpty()) {
                saveEvents(eventMap);
            }
            if (!upsertMap.isEmpty()) {
                doUpsert(upsertMap);
            }

            tx.commit();
        } catch (Throwable e) {
            logger.error("can't process events, drop it!", e);

            try {
                tx.commit();// commit to drop bad event, otherwise it will enter dead loop.
            } catch (Exception e2) {
                logger.error("Exception in catch commit. Commit might not have been successful", e2);
            }

            if(e instanceof Error) {
                Throwables.propagate(e);
            } else {
                throw new EventDeliveryException(e);
            }

        } finally {
            tx.close();
        }

        return status;
    }

    private void doUpsert(Map<String, List<DBObject>> eventMap) {
        if (eventMap.isEmpty()) {
            logger.debug("eventMap is empty");
            return;
        }

        for (Map.Entry<String, List<DBObject>> entry : eventMap.entrySet()) {
            List<DBObject> docs = entry.getValue();
            if (logger.isDebugEnabled()) {
                logger.debug("collection: {}, length: {}", entry.getKey(), docs.size());
            }

            int separatorIndex = entry.getKey().indexOf(NAMESPACE_SEPARATOR);
            String eventDb = entry.getKey().substring(0, separatorIndex);
            String collectionName = entry.getKey().substring(separatorIndex + 1);

            //Warning: please change the WriteConcern level if you need high datum consistence.
            DB dbRef = mongo.getDB(eventDb);
            if (authentication_enabled) {
                boolean authResult = dbRef.authenticate(username, password.toCharArray());
                if (!authResult) {
                    logger.error("Failed to authenticate user: " + username + " with password: " + password + ". Unable to write events.");
                    return;
                }
            }
            DBCollection collection = dbRef.getCollection(collectionName);
	    for (DBObject doc : docs) {
		if (logger.isDebugEnabled()) {
			logger.debug("doc: {}", doc);
		}
		DBObject query = BasicDBObjectBuilder.start()
				.add(PK, doc.get(PK)).get();
		BasicDBObjectBuilder doc_builder = BasicDBObjectBuilder.start();
		if (doc.keySet().contains(OP_INC)) {
			doc_builder.add(OP_INC, doc.get(OP_INC));
		}
		if (doc.keySet().contains(OP_SET)) {
			doc_builder.add(OP_SET, doc.get(OP_SET));
		}
		if (doc.keySet().contains(OP_SET_ON_INSERT)) {
			doc_builder.add(OP_SET_ON_INSERT, doc.get(OP_SET_ON_INSERT));
		}
        if (doc.keySet().contains(OP_MAX)) {
            doc_builder.add(OP_MAX, doc.get(OP_MAX));
        }
		doc = doc_builder.get();
		//logger.debug("query: {}", query);
		//logger.debug("new doc: {}", doc);

		//CommandResult result = collection.update(query, doc, true, false, WriteConcern.SAFE).getLastError();
        collection.update(query, doc, true, false, WriteConcern.SAFE);

            /*
		if (result.ok()) {
			String errorMessage = result.getErrorMessage();
			if (errorMessage != null) {
				logger.error("can't upsert documents with error: {} ",
						errorMessage);
				logger.error("with exception", result.getException());
				throw new MongoException(errorMessage);
			}
		} else {
		    logger.error("can't get last error");
		}
		*/
	    }
        }
    }

    private void processEvent(Map<String, List<DBObject>> eventMap, Event event) {
        switch (model) {
            case SINGLE:
                putSingleEvent(eventMap, event);

                break;
            case DYNAMIC:
                putDynamicEvent(eventMap, event);

                break;
            default:
                logger.error("can't support model: {}, please check configuration.", model);
        }
    }

    private void putDynamicEvent(Map<String, List<DBObject>> eventMap, Event event) {
        String eventCollection;
        Map<String, String> headers = event.getHeaders();
        String eventDb = headers.get(DB_NAME);
        eventCollection = headers.get(COLLECTION);

        if (!StringUtils.isEmpty(eventDb)) {
            eventCollection = eventDb + NAMESPACE_SEPARATOR + eventCollection;
        } else {
            eventCollection = dbName + NAMESPACE_SEPARATOR + eventCollection;
        }

        if (!eventMap.containsKey(eventCollection)) {
            eventMap.put(eventCollection, new ArrayList<DBObject>());
        }

        List<DBObject> documents = eventMap.get(eventCollection);
        addEventToList(documents, event);
    }

    private void putSingleEvent(Map<String, List<DBObject>> eventMap, Event event) {
        String eventCollection;
        eventCollection = dbName + NAMESPACE_SEPARATOR + collectionName;
        if (!eventMap.containsKey(eventCollection)) {
            eventMap.put(eventCollection, new ArrayList<DBObject>());
        }

        List<DBObject> docs = eventMap.get(eventCollection);
        addEventToList(docs, event);
    }

    private List<DBObject> addEventToList(List<DBObject> documents, Event event) {
        if (documents == null) {
            documents = new ArrayList<DBObject>(batchSize);
        }

        try {
            DBObject eventJson = this.serializer.process(this, event);
            if (null != eventJson) {
                for(Map.Entry<String, String> entry : this.extraInfos.entrySet()) {
                    eventJson.put(entry.getKey(), entry.getValue());
                }
                documents.add(eventJson);
            }
        } catch (Exception e) {
            logger.error("Can't parse events: " + new String(event.getBody()), e);
        }

        return documents;
    }

    public enum CollectionModel {
        DYNAMIC, SINGLE
    }
}
