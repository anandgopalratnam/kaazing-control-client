package com.lc.df.controlclient.core;

import com.lc.df.controlclient.utils.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by anandgopalratnam on 25/03/2018.
 */
public class EntityCache {

    private Map<String,String> categoryCache = new HashMap<String, String>();
    private Map<String,String> classCache = new HashMap<String, String>();
    private Map<String,String> typeCache = new HashMap<String, String>();
    private Map<String,String> eventCache = new HashMap<String, String>();
    private Map<String,String> marketCache = new HashMap<String, String>();
    private Map<String,String> selectionCache = new HashMap<String, String>();

    public String getCategory(String key,String defaultValue){
        synchronized (categoryCache){
            String category = categoryCache.get(key);
            if (category == null){
                return defaultValue;
            }
            return category;
        }
    }
    public void setCategory(String key,String category){
        synchronized (categoryCache) {
            if (category == null){
//                Logger.logInfoMessage("Removing key ["+key+"] From Category Cache Size["+categoryCache.size()+"]");
                categoryCache.remove(key);
//                Logger.logInfoMessage("Category Cache Size["+categoryCache.size()+"] after removal");
            }
            else {
                categoryCache.put(key,category);
            }
        }
    }

    public String getClass(String key,String defaultValue){
        synchronized (classCache){
            String clazz = classCache.get(key);
            if (clazz == null){
                return defaultValue;
            }
            return clazz;
        }
    }
    public void setClass(String key,String clazz){
        synchronized (classCache) {
            if (clazz == null){
//                Logger.logInfoMessage("Removing key ["+key+"] From Class Cache Size["+classCache.size()+"]");
                classCache.remove(key);
//                Logger.logInfoMessage("Class Cache Size["+classCache.size()+"] after removal");
            }
            else {
                classCache.put(key,clazz);
            }
        }
    }

    public String getType(String key,String defaultValue){
        synchronized (typeCache){
            String type = typeCache.get(key);
            if (type == null){
                return defaultValue;
            }
            return type;
        }
    }
    public void setType(String key,String type){
        synchronized (typeCache) {
            if (type == null){
//                Logger.logInfoMessage("Removing key ["+key+"] From Type Cache Size["+typeCache.size()+"]");
                typeCache.remove(key);
//                Logger.logInfoMessage("Type Cache Size["+typeCache.size()+"] after removal");
            }
            else {
                typeCache.put(key,type);
            }
        }
    }

    public String getSelection(String key,String defaultValue){
        synchronized (selectionCache){
            String selection = selectionCache.get(key);
            if (selection == null){
                return defaultValue;
            }
            return selection;
        }
    }
    public void setSelection(String key,String selection){
        synchronized (selectionCache) {
            if (selection == null){
//                Logger.logInfoMessage("Removing key ["+key+"] From Selection Cache Size["+selectionCache.size()+"]");
                selectionCache.remove(key);
//                Logger.logInfoMessage("Selection Cache Size["+selectionCache.size()+"] after removal");
            }
            else {
                selectionCache.put(key,selection);
            }
        }
    }

    public String getMarket(String key,String defaultValue){
        synchronized (marketCache){
            String market = marketCache.get(key);
            if (market == null){
                return defaultValue;
            }
            return market;
        }
    }
    public void setMarket(String key,String market){
        synchronized (marketCache) {
            if (market == null){
//                Logger.logInfoMessage("Removing key ["+key+"] From Market Cache Size["+marketCache.size()+"]");
                marketCache.remove(key);
//                Logger.logInfoMessage("Market Cache Size["+marketCache.size()+"] after removal");
            }
            else {
                marketCache.put(key,market);
            }
        }
    }

    public String getEvent(String key,String defaultValue){
        synchronized (eventCache){
            String event = eventCache.get(key);
            if (event == null){
                return defaultValue;
            }
            return event;
        }
    }
    public void setEvent(String key,String event){
        synchronized (eventCache) {
            if (event == null){
//                Logger.logInfoMessage("Removing key ["+key+"] From Event Cache Size["+eventCache.size()+"]");
                eventCache.remove(key);
//                Logger.logInfoMessage("Event Cache Size["+eventCache.size()+"] after removal");
            }
            else {
                eventCache.put(key,event);
            }
        }
    }


    public void printCacheCounts(){
        Logger.logInfoMessage("Category ["+categoryCache.size()+"]\n");
        Logger.logInfoMessage("Class ["+classCache.size()+"]\n");
        Logger.logInfoMessage("Type ["+typeCache.size()+"]\n");
        Logger.logInfoMessage("Event ["+eventCache.size()+"]\n");
        Logger.logInfoMessage("Market ["+marketCache.size()+"]\n");
        Logger.logInfoMessage("Selection ["+selectionCache.size()+"]\n");
    }
}
