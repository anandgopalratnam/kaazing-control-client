package com.lc.df.controlclient.core;

/**
 * Created by anandgopalratnam on 09/04/2018.
 */
public class Parents {
    Long categoryKey;
    Long classKey;
    Long typeKey;
    Long eventKey;
    Long marketKey;

    public Long getCategoryKey() {
        return categoryKey;
    }

    public void setCategoryKey(Long categoryKey) {
        this.categoryKey = categoryKey;
    }

    public Long getClassKey() {
        return classKey;
    }

    public void setClassKey(Long classKey) {
        this.classKey = classKey;
    }

    public Long getTypeKey() {
        return typeKey;
    }

    public void setTypeKey(Long typeKey) {
        this.typeKey = typeKey;
    }

    public Long getEventKey() {
        return eventKey;
    }

    public void setEventKey(Long eventKey) {
        this.eventKey = eventKey;
    }

    public Long getMarketKey() {
        return marketKey;
    }

    public void setMarketKey(Long marketKey) {
        this.marketKey = marketKey;
    }

    @Override
    public String toString() {
        return "c."+categoryKey+":cl."+classKey+":t."+typeKey+":e."+eventKey+":m."+marketKey;
    }
}
