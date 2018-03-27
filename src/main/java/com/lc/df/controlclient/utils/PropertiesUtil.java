package com.lc.df.controlclient.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class PropertiesUtil
{
	private static Properties properties = new Properties();
	
	public static void loadProperties()
	{
		try
		{
			String filename = System.getProperty("config")+File.separatorChar+"bridge.properties";
			properties.load(new FileInputStream(filename));
			Logger.logInfoMessage("Loaded All properties from the file [" + filename + "]");
		}
		catch (IOException ioe)
		{
			Logger.logErrorMessage(" IO Exception in reading property file ", ioe);
		}
		catch (NullPointerException npe)
		{
			Logger.logErrorMessage(" Null Pointer in reading property file ", npe);
		}
	}
	public static void loadProperties(String name)
	{
		try
		{
			String filename = System.getProperty("config")+File.separatorChar+name+".properties";
			properties.load(new FileInputStream(filename));
			Logger.logInfoMessage("Loaded All properties from the file [" + filename + "]");
		}
		catch (IOException ioe)
		{
			Logger.logErrorMessage(" IO Exception in reading property file ", ioe);
		}
		catch (NullPointerException npe)
		{
			Logger.logErrorMessage(" Null Pointer in reading property file ", npe);
		}
	}
	public static String getProperty(String key)
	{
		return properties.getProperty(key);
	}

	public static Map<String,String> getProperties(String prefix)
	{
		Map<String,String> map = new HashMap<String,String>();
		for (Iterator<Object> iter = properties.keySet().iterator(); iter.hasNext();)
		{
			String propertyName = (String) iter.next();
			if (!propertyName.startsWith(prefix))
				continue;
			map.put(propertyName.substring((prefix+".").length()),(String)properties.get(propertyName));
		}
		return map;
	}
	
	public static Set<Object> getKeySet()
	{
		return properties.keySet();
	}
}