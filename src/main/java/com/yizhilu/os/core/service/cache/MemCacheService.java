package com.yizhilu.os.core.service.cache;

import net.spy.memcached.MemcachedClient;

import java.util.Map;
import java.util.Set;

/**
 * 
 * @ClassName MemCacheService
 * @package com.yizhilu.os.core.common.cache
 * @description
 * @author liuqinggang
 * @Create Date: 2013-5-25 下午5:37:27
 * 
 */
public interface MemCacheService {

    /**
     * 
     * @param key
     * @return
     */
    Object get(String key);

    /**
     * 
     * @param key
     * @param value
     * @return
     */
    boolean set(String key, Object value);

    /**
     * 批量取
     * 
     * @param keys
     * @return
     */
    Map<String, Object> getBulk(Set<String> keys);

    /**
     * 
     * @param key
     * @return
     */
    boolean remove(String key);

    /**
     * 存,设置超时时间
     * 
     * @param key
     * @param value
     * @param exp
     * @return
     */
    boolean set(String key, Object value, int exp);

    /**
     * 获取原生的MemcachedClient对象
     * @return
     */
    MemcachedClient getMemcachedClient();

    /**
     * 获取值，并更新时间
     * @param key
     * @param exp
     * @return
     */
    public Object getAndTouch(String key, int exp);

}
