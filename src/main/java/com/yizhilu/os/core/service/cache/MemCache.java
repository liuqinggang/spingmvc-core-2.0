package com.yizhilu.os.core.service.cache;

import com.yizhilu.os.core.util.ObjectUtils;
import net.spy.memcached.MemcachedClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

/**
 * 
 * @ClassName com.yizhilu.os.core.service.common.MemCache
 * @description memcache操作类
 * @author : qinggang.liu voo@163.com
 * @Create Date : 2013-12-25 上午11:55:52
 */
public class MemCache {
    private Logger logger= LoggerFactory.getLogger(MemCache.class);
    private static MemCacheService memCacheService = null;
    private static MemCache memCache = new MemCache();

    public static MemCache getInstance() {
        return memCache;
    }

    private MemCache() {
        memCacheService = MemCacheServiceImpl.getInstance();
    }

    /**
     * 获取
     * 
     * @param key
     * @return Object
     */
    public Object get(String key) {
        try {
            if (memCacheService != null) {
                return memCacheService.get(key);
            }
        } catch (Exception e) {
            handleException(e, key);
        }
        return null;
    };

    /**
     * 设置。默认时间为1天
     * 
     * @param key
     * @param value
     * @return
     */
    public boolean set(String key, Object value) {
        try {
            if (memCacheService != null) {
                return memCacheService.set(key, value);
            }
        } catch (Exception e) {
            handleException(e, key);
        }
        return false;
    }

    /**
     * 批量取
     * 
     * @param keys
     * @return
     */
    public Map<String, Object> getBulk(Set<String> keys) {
        try {
            if (memCacheService != null) {
                return memCacheService.getBulk(keys);
            }
        } catch (Exception e) {
            handleException(e, keys.toString());
        }
        return null;
    }

    /**
     * 根据key删除
     * 
     * @param key
     * @return
     */
    public boolean remove(String key) {
        try {
            if (memCacheService != null) {
                return memCacheService.remove(key);
            }
        } catch (Exception e) {
            handleException(e, key);
        }
        return false;
    }

    /**
     * 存,设置超时时间
     * 
     * @param key
     *            键
     * @param value值
     * @param exp
     *            时间（秒） 60*60为一小时
     * @return
     */
    public boolean set(String key, Object value, int exp) {
        try {
            if (memCacheService != null) {
                return memCacheService.set(key, value, exp);
            }
        } catch (Exception e) {
            handleException(e, key);
        }
        return false;
    }
    private void handleException(Exception e, String key) {
        logger.warn("spymemcached client receive an exception with key:" + key, e);
    }
    /**
     * 获取原生的MemcachedClient对象
     * @return
     */
    public MemcachedClient getMemcachedClient() {
        return memCacheService.getMemcachedClient();
    }

    /**
     * 获取值，并更新时间
     * @param key
     * @param exp
     * @return
     */
    public Object getAndTouch(String key, int exp){
        return memCacheService.getAndTouch(key,exp);
    }



}
