package cn.doitedu.rtmk.tech_test.enjoy_test;

import java.util.Map;

public class EventBean{
    private String eventId;
    private Map<String,String> properties;

    public EventBean() {
    }

    public EventBean(String eventId, Map<String, String> properties) {
        this.eventId = eventId;
        this.properties = properties;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }
}