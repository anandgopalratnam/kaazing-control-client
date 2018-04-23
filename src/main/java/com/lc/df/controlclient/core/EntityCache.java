package com.lc.df.controlclient.core;

import com.lc.df.controlclient.utils.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by anandgopalratnam on 25/03/2018.
 */
public class EntityCache {

    private Map<Long,String> categoryCache = new HashMap<Long, String>();
    private Map<Long,String> classCache = new HashMap<Long, String>();
    private Map<Long,String> typeCache = new HashMap<Long, String>();
    private Map<Long,String> eventCache = new HashMap<Long, String>();
    private Map<Long,String> marketCache = new HashMap<Long, String>();
    private Map<Long,String> selectionCache = new HashMap<Long, String>();

    public String getCategory(Long key,String defaultValue){
        synchronized (categoryCache){
            if (key != null){
                String category = categoryCache.get(key);
                if (category == null){
                    return defaultValue;
                }
                return category;
            }
            return null;
        }
    }
    public void setCategory(Long key,String category){
        synchronized (categoryCache) {
            if (category == null){
                categoryCache.remove(key);
            }
            else {
                categoryCache.put(key,category);
            }
        }
    }

    public String getClass(Long key,String defaultValue){
        synchronized (classCache){
            if (key != null){
                String clazz = classCache.get(key);
                if (clazz == null){
                    return defaultValue;
                }
                return clazz;
            }
            return null;
        }
    }
    public void setClass(Long key,String clazz){
        synchronized (classCache) {
            if (clazz == null){
                classCache.remove(key);
            }
            else {
                classCache.put(key,clazz);
            }
        }
    }

    public String getType(Long key,String defaultValue){
        synchronized (typeCache){
            if (key !=  null){
                String type = typeCache.get(key);
                if (type == null){
                    return defaultValue;
                }
                return type;
            }
            return null;
        }
    }
    public void setType(Long key,String type){
        synchronized (typeCache) {
            if (type == null){
                typeCache.remove(key);
            }
            else {
                typeCache.put(key,type);
            }
        }
    }

    public String getSelection(Long key,String defaultValue){
        synchronized (selectionCache){
            if (key != null){
                String selection = selectionCache.get(key);
                if (selection == null){
                    return defaultValue;
                }
                return selection;
            }
            return null;
        }
    }
    public void setSelection(Long key,String selection){
        synchronized (selectionCache) {
            if (selection == null){
                selectionCache.remove(key);
            }
            else {
                selectionCache.put(key,selection);
            }
        }
    }

    public String getMarket(Long key,String defaultValue){
        synchronized (marketCache){
            if (key != null){
                String market = marketCache.get(key);
                if (market == null){
                    return defaultValue;
                }
                return market;
            }
            return null;
        }
    }
    public void setMarket(Long key,String market){
        synchronized (marketCache) {
            if (market == null){
                marketCache.remove(key);
            }
            else {
                marketCache.put(key,market);
            }
        }
    }

    public String getEvent(Long key,String defaultValue){
        synchronized (eventCache){
            if (key != null){
                String event = eventCache.get(key);
                if (event == null){
                    return defaultValue;
                }
                return event;
            }
            return null;
        }
    }
    public void setEvent(Long key,String event){
        synchronized (eventCache) {
            if (event == null){
                eventCache.remove(key);
            }
            else {
                eventCache.put(key,event);
            }
        }
    }


    public synchronized void printCacheCounts(){
        Logger.logInfoMessage("Category ["+categoryCache.size()+"]\n");
        Logger.logInfoMessage("Class ["+classCache.size()+"]\n");
        Logger.logInfoMessage("Type ["+typeCache.size()+"]\n");
        Logger.logInfoMessage("Event ["+eventCache.size()+"]\n");
        Logger.logInfoMessage("Market ["+marketCache.size()+"]\n");
        Logger.logInfoMessage("Selection ["+selectionCache.size()+"]\n");
    }
}
