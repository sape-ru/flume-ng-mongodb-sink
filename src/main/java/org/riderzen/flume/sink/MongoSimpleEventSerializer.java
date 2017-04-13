package org.riderzen.flume.sink;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;

public class MongoSimpleEventSerializer implements MongoEventSerializer {

    private static Logger logger = LoggerFactory.getLogger(MongoSimpleEventSerializer.class);

    @Override
    public Event addHeaders(Event event) {
        return event;
    }

    @Override
    public DBObject process(MongoSink conf, Event event) throws Exception {
        String body = new String(event.getBody());

        DBObject eventJson = conf.autoWrap ? new BasicDBObject(conf.wrapField, body) : (DBObject)JSON.parse(body);
        if (!event.getHeaders().containsKey(MongoSink.OPERATION) && conf.timestampField != null) {
            Date timestamp;
            if (eventJson.containsField(MongoSink.TIMESTAMP_FIELD)) {
                try {
                    String dateText = (String) eventJson.get(MongoSink.TIMESTAMP_FIELD);
                    timestamp = MongoSink.dateTimeFormatter.parseDateTime(dateText).toDate();
                    eventJson.removeField(MongoSink.TIMESTAMP_FIELD);
                } catch (Exception e) {
                    logger.error("can't parse date ", e);
                    timestamp = new Date();
                }
            } else {
                timestamp = new Date();
            }
            eventJson.put(MongoSink.TIMESTAMP_FIELD, timestamp);
        }

        return eventJson;
    }
}
