package com.yizhilu.os.core.service.cache;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.yizhilu.os.core.util.ObjectUtils;
import net.spy.memcached.*;
import net.spy.memcached.protocol.binary.BinaryOperationFactory;
import net.spy.memcached.transcoders.SerializingTranscoder;

import com.yizhilu.os.core.util.PropertiesReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @ClassName MemCacheServiceImpl
 * @package com.yizhilu.os.core.common.cache
 * @description
 * @author liuqinggang
 * @Create Date: 2013-5-25 下午5:37:39
 * 
 */
public class MemCacheServiceImpl implements MemCacheService {
    /**
     * 一些常量
     */
    public static final String CACHE_PROP_FILE = "project";

    public static final String ENCODING = "UTF-8";

    // 日志
    private static Logger logger = LoggerFactory.getLogger(MemCacheServiceImpl.class);

    // 返回的实例
    // private static FcCacheService instance = new FcCacheServiceImpl();
    private static ConcurrentHashMap<String, MemCacheService> flyweights = new ConcurrentHashMap<String, MemCacheService>();

    // MemcachedClient 两组容错
    private MemcachedClient mc1 = null;

    private MemcachedClient mc2 = null;

    /**
     * Default operation timeout in seconds.
     */
    public static final int DEFAULT_MEMCACHED_TIMEOUT = 1;

    private int opTimeout = DEFAULT_MEMCACHED_TIMEOUT;

    /**
     * Default operation timeout in seconds.
     */
    public static final int DEFAULT_MEMCACHED_TIMEOUT_BATCH = 3;

    private int opTimeoutBulk = DEFAULT_MEMCACHED_TIMEOUT_BATCH;

    // 读buffer长度
    public static final int DEFAULT_READBUF_SIZE = 16384;
    private int readBufSize = DEFAULT_READBUF_SIZE;

    // 操作队列长度
    public static final int DEFAULT_OPQ_SIZE = 16384;
    private int opQueueLens = DEFAULT_MEMCACHED_RETRY;

    // 过期时间,默认定为24小时
    public static final int DEFAULT_MEMCACHED_EXP_HOURS = 24;
    private int expHour = DEFAULT_MEMCACHED_EXP_HOURS;

    // 重试次数
    public static final int DEFAULT_MEMCACHED_RETRY = 3;
    private int retry = DEFAULT_MEMCACHED_RETRY;

    /**
     * 获取SessionService实例
     * 
     * @return 一个实例
     * @throws IOException
     */
    public static MemCacheService getInstance(String prop_file) {
        if (!flyweights.containsKey(prop_file)) {
            synchronized (prop_file) {
                flyweights.put(prop_file, new MemCacheServiceImpl(prop_file));
            }
        }
        return flyweights.get(prop_file);
    }

    public MemCacheServiceImpl() {
        String prop_file = CACHE_PROP_FILE;
        if (!flyweights.containsKey(prop_file)) {
            synchronized (prop_file) {
                flyweights.put(prop_file, new MemCacheServiceImpl(prop_file));
            }
        }
    }

    public static MemCacheService getInstance() {
        return getInstance(CACHE_PROP_FILE);
    }

    /**
     * 私有构造方法,初始化memcached
     * 
     * @throws IOException
     * 
     */
    private MemCacheServiceImpl(String prop_file) {

        // ------------装载memchache信息-----------------------
        String server1 = PropertiesReader.getValue(prop_file, "server1");
        String server2 = PropertiesReader.getValue(prop_file, "server2");
        try {
            opTimeout = Integer.parseInt(PropertiesReader
                    .getValue(prop_file, "opTimeout"));
            opTimeoutBulk = Integer.parseInt(PropertiesReader.getValue(prop_file,
                    "opTimeoutBulk"));
            retry = Integer.parseInt(PropertiesReader.getValue(prop_file, "retry"));
            readBufSize = Integer.parseInt(PropertiesReader.getValue(prop_file,
                    "readBufSize"));
            opQueueLens = Integer.parseInt(PropertiesReader.getValue(prop_file,
                    "opQueueLen"));
            expHour = Integer.parseInt(PropertiesReader.getValue(prop_file, "expHour"));
        } catch (Exception e) {
            logger.error("loading properties fail, use default config!");
        }
        // 从配置文件中读取相应的配置信息
        try {
            mc1 = new MemcachedClient(new DefaultConnectionFactory() {

                @Override
                public long getOperationTimeout() {
                    return opTimeout * 1000;
                }

                @Override
                public int getReadBufSize() {
                    return readBufSize;
                }
                //默认返回new AsciiOperationFactory();
               /* @Override
                public OperationFactory getOperationFactory() {
                    return super.getOperationFactory();
                }*/
                public OperationFactory getOperationFactory() {
                    return new BinaryOperationFactory();
                }

                @Override
                public int getOpQueueLen() {
                    return opQueueLens;
                }

                @Override
                public boolean isDaemon() {
                    return true;
                }

            }, AddrUtil.getAddresses(server1));

            mc2 = new MemcachedClient(new DefaultConnectionFactory() {

                @Override
                public long getOperationTimeout() {
                    return opTimeout * 1000;
                }

                @Override
                public int getReadBufSize() {
                    return readBufSize;
                }

               /* @Override
                public OperationFactory getOperationFactory() {
                    return super.getOperationFactory();
                }*/
               public OperationFactory getOperationFactory() {
                   return new BinaryOperationFactory();
               }

                @Override
                public int getOpQueueLen() {
                    return opQueueLen;
                }

                @Override
                public boolean isDaemon() {
                    return true;
                }

            }, AddrUtil.getAddresses(server2));

        } catch (IOException e) {
            logger.error("DefaultConnectionFactory memcache error:", e);
        }
        // 使用Utf-8编码
        SerializingTranscoder x1 = (SerializingTranscoder) mc1.getTranscoder();
        x1.setCharset(ENCODING);
        SerializingTranscoder x2 = (SerializingTranscoder) mc2.getTranscoder();
        x2.setCharset(ENCODING);
    }

    /**
     * 获取一个对象(含重试机制)
     * 
     * @param key
     * @return piggie 2009-10-16 version 2.2.1
     */
    @Override
    public Object get(String key) {
        Object result = null;
        try {
            for (int i = 0; i < retry; i++) {
                result = _get(key);
                if (result == null) {
                    logger.debug("get info from cache failed begin to retry " + (i + 1));
                } else { break;
                }
            }
            if (result == null) {
                logger.debug("[FAIL] completely failed when getting info from cache after "
                        + retry + " times");
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return result;
    }

    /**
     * 获取一个对象
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     */
    private Object _get(String key) {
        // TODO Auto-generated method stub
        // logger.debug("[ACCESS] begin to get info from cache...");
        Object myObj = null;
        try {
            Future<Object> f = mc1.asyncGet(key);
            try {
                myObj = f.get(opTimeout, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                e.printStackTrace();
                f.cancel(false);
            } catch (InterruptedException e) {
                e.printStackTrace();
                f.cancel(false);
            } catch (ExecutionException e) {
                e.printStackTrace();
                f.cancel(false);
            }

            if (myObj == null) {
                Future<Object> f2 = mc2.asyncGet(key);
                try {
                    myObj = f2.get(opTimeout, TimeUnit.SECONDS);
                } catch (TimeoutException e) {
                    e.printStackTrace();
                    f2.cancel(false);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    f2.cancel(false);
                } catch (ExecutionException e) {
                    e.printStackTrace();
                    f2.cancel(false);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (myObj != null) {
            // logger.debug("MemCacheServiceImpl._get,key=" + key + ",object="
            // + myObj.getClass());
        } else {
            // logger.debug("MemCacheServiceImpl._get,key=" + key + ",object=null");
        }
        return myObj;

    }

    /**
     * 获取一批对象
     */
    @Override
    public Map<String, Object> getBulk(Set<String> keys) {
        // TODO Auto-generated method stub
        logger.debug("[ACCESS]begin to get info from cache in bulk...");
        Map<String, Object> ret = null;

        try {
            Future<Map<String, Object>> f = mc1.asyncGetBulk(keys);
            try {
                ret = f.get(opTimeoutBulk, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                // Since we don't need this, go ahead and cancel the operation.
                // This
                // is not strictly necessary, but it'll save some work on the
                // server.
                logger.debug("[FAIL]time out when getting objects from cache server1...");
                f.cancel(false);
                // Do other timeout related stuff
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                logger.debug("[FAIL]thread been interrupted while waiting when getting object from cache server1...");
                f.cancel(false);
            } catch (ExecutionException e) {
                // TODO Auto-generated catch block
                logger.debug("[FAIL]exception when getting object from cache server1...");
                f.cancel(false);
            }

            if (ret == null) {
                Future<Map<String, Object>> f2 = mc2.asyncGetBulk(keys);
                try {
                    ret = f2.get(opTimeoutBulk, TimeUnit.SECONDS);
                } catch (TimeoutException e) {
                    // Since we don't need this, go ahead and cancel the
                    // operation. This
                    // is not strictly necessary, but it'll save some work on
                    // the server.
                    logger.debug("[FAIL]time out when getting objects from cache server2...");
                    f2.cancel(false);
                    // Do other timeout related stuff
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    logger.debug("[FAIL]thread been interrupted while waiting when getting object from cache server2...");
                    f2.cancel(false);
                } catch (ExecutionException e) {
                    // TODO Auto-generated catch block
                    logger.debug("[FAIL]exception when getting object from cache server2...");
                    f2.cancel(false);
                }
            }
        } catch (Exception e) {
            logger.error(
                    "[ERROR]other exception when getting objects from voo@163.com cache...",
                    e);
        }

        if (ret != null) {
            for (String key : keys) {
                if (ret.get(key) != null) {
                    logger.debug("[GET]SHOOTED" + "\tKey=" + key + "\tValue="
                            + ret.get(key).toString());
                }
            }
        }

        return ret;
    }

    /**
     * 存入一个对象(含重试机制)
     *
     * @param key
     * @param value
     * @return piggie 2009-10-16 version 2.2.1
     */
    @Override
    public boolean set(String key, Object value) {
        boolean result = false;
        for (int i = 0; i < retry; i++) {
            result = _set(key, value);
            if (!result) {
                logger.debug("set info into cache failed begin to retry " + i);
            } else {
                break;
            }
        }
        if (!result) {
            logger.error("[FAIL] completely failed when setting info into cache after "
                    + retry + " times");
        }
        return result;
    }

    /**
     * 存入一个对象
     */
    private boolean _set(String key, Object value) {
        // mc1.delete(key);
        // mc2.delete(key);
        boolean ret = false;
        Future<Boolean> f = mc1.set(key, expHour * 60 * 60, value);
        Future<Boolean> f2 = mc2.set(key, expHour * 60 * 60, value);
        try {
            boolean fs1 = f.get(opTimeout, TimeUnit.SECONDS);
            boolean fs2 = f2.get(opTimeout, TimeUnit.SECONDS);
            ret = fs1 || fs2;

            if (!fs1) {
                logger.debug("[FAIL]CACHE SET FAIL:server1 set failed: " + "Key=" + key
                        + "\tValue=" + value.toString());
            } else if (!fs2) {
                logger.debug("[FAIL]CACHE SET FAIL:server2 set failed: " + "Key=" + key
                        + "\tValue=" + value.toString());
            }
        } catch (TimeoutException e) {
            // Since we don't need this, go ahead and cancel the
            // operation. This
            // is not strictly necessary, but it'll save some work on
            // the server.
            logger.debug("[FAIL]time out when getting objects from cache server2...");
            f.cancel(false);
            f2.cancel(false);
            // Do other timeout related stuff
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            logger.error(
                    "[ERROR]exception when setting voo@163.com cache - thread been interrupted...",
                    e);
            f.cancel(false);
            f2.cancel(false);
        } catch (ExecutionException e) {
            // TODO Auto-generated catch block
            logger.error(
                    "[ERROR]exception when setting voo@163.com cache - exception when getting status...",
                    e);
            f.cancel(false);
            f2.cancel(false);
        } catch (Exception e) {
            logger.error(
                    "[ERROR]exception when setting voo@163.com cache - other exceptions...",
                    e);
            f.cancel(false);
            f2.cancel(false);
        }

        if (value != null) {
            logger.debug("MemCacheServiceImpl.set,key=" + key + ",value=" + value.getClass());
        } else {
            logger.debug("MemCacheServiceImpl.set,key=" + key + ",value=null");
        }
        return ret;

    }

    /**
     * <p>
     * 移除一个对象
     * </p>
     *
     * @see
     * @param key
     * @param value
     * @return
     * @author futuremining
     * @date 2009-1-12
     * @version 1.0.0
     */
    @Override
    public boolean remove(String key) {
        boolean ret = false;

        Future<Boolean> f = mc1.delete(key);
        Future<Boolean> f2 = mc2.delete(key);

        try {
            ret = f.get(opTimeout, TimeUnit.SECONDS)
                    && f2.get(opTimeout, TimeUnit.SECONDS); // !!
                                                            // 该行的判断只限于2台不同的memcached服务器
        } catch (TimeoutException e) {
            // Since we don't need this, go ahead and cancel the
            // operation. This
            // is not strictly necessary, but it'll save some work on
            // the server.
            logger.debug("[FAIL]time out when getting objects from cache server2...");
            f.cancel(false);
            f2.cancel(false);
            // Do other timeout related stuff
        } catch (InterruptedException e) {
            logger.error(
                    "[ERROR]exception when deleting voo@163.com cache - thread been interrupted...",
                    e);
            f.cancel(false);
            f2.cancel(false);
            ret = false;
        } catch (ExecutionException e) {
            logger.error(
                    "[ERROR]exception when deleting voo@163.com cache - exception when getting status...",
                    e);
            f.cancel(false);
            f2.cancel(false);
            ret = false;
        } catch (Exception e) {
            logger.error(
                    "[ERROR]exception when deleting voo@163.com cache - other exceptions...",
                    e);
            f.cancel(false);
            f2.cancel(false);
            ret = false;
        }

        logger.debug("[REMOVE]" + ret + "\tKey=" + key);

        return ret; // 如果配了相同的，即使 remove成功
        // ，也会返回false，因此此返回值有意义仅当配置两台不同memcached服务器
    }

    @Override
    public boolean set(String key, Object value, int exp) {
        // mc1.delete(key);
        // mc2.delete(key);
        if (value == null)
            return false;

        boolean ret = false;
        Future<Boolean> f = mc1.set(key, exp, value);
        Future<Boolean> f2 = mc2.set(key, exp, value);

        try {
            boolean fs1 = f.get(opTimeout, TimeUnit.SECONDS);
            boolean fs2 = f2.get(opTimeout, TimeUnit.SECONDS);
            ret = fs1 || fs2;

            if (!fs1) {
                logger.debug("[FAIL]CACHE SET FAIL:server1 set failed: " + "Key=" + key
                        + ",Value=" + value.toString());
            } else if (!fs2) {
                logger.debug("[FAIL]CACHE SET FAIL:server2 set failed: " + "Key=" + key
                        + ",Value=" + value.toString());
            }
        } catch (Exception e) {
            if (!"LOGIN_IP".equalsIgnoreCase(key)) {
                logger.debug("MemCacheServiceImpl.set,key=" + key + ",value=" + value
                        + ",Exception");
            }
            e.printStackTrace();
            f.cancel(false);
            f2.cancel(false);
        }
        logger.debug("MemCacheServiceImpl.set,key=" + key + ",value=" + value.getClass());
        return ret;
    }
    /**
     * 获取原生的MemcachedClient对象
     * @return
     */
    public MemcachedClient getMemcachedClient() {
        if (ObjectUtils.isNotNull(mc1)) {
            return mc1;
        }
        return mc2;
    }

    /**
     * 获取值，并更新时间
     * @param key
     * @param exp
     * @return
     */
    public Object getAndTouch(String key, int exp) {
        Object result = null;
        try {
            //mc1
            CASValue casValue=mc1.getAndTouch(key,exp);
            if(casValue!=null){
                result=casValue.getValue();
            }
            if (result == null) {
                casValue=mc2.getAndTouch(key,exp);
                if(casValue!=null){
                    result=casValue.getValue();
                }
            }
            if (result == null) {
                logger.debug("[FAIL] completely failed when getAndTuch  from cache after "
                        + retry + " times");
            }
        } catch (Exception ex) {
            logger.error("[FAIL] completely failed when getAndTuch  from cache after "
                    ,ex);
        }
        return result;

    }




}