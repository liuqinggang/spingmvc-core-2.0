package com.yizhilu.os.core.controller;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.yizhilu.os.core.service.cache.MemCache;
import com.yizhilu.os.core.util.ObjectUtils;
import com.yizhilu.os.core.util.StringUtils;
import com.yizhilu.os.core.util.web.WebUtils;

import javax.servlet.http.HttpServletRequest;

/**
 * Created by Administrator on 2015/4/7.
 */
public class MemLoginUtils {

    static MemCache memCache = MemCache.getInstance();

    public static Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss")
            .create();
    public static JsonParser jsonParser = new JsonParser();

    /**
     * 获取登陆用户的id(前台用)
     *
     * @return int
     * @throws Exception
     */
    public static Long getLoginUserId(HttpServletRequest request)  {
        try {
            JsonObject useObject= getLoginUser(request);
            if (ObjectUtils.isNotNull(useObject)) {
                if ( StringUtils.isNotEmpty(useObject.get("id").toString())) {
                    return Long.valueOf(useObject.get("id").toString());
                } else {
                    return 0L;
                }
            } else {
                return 0L;
            }
        }catch (Exception e){
            return 0L;
        }
    }

    /**
     * 是否登录
     * @param request
     * @return
     */
    public static boolean isLogin(HttpServletRequest request) {
        try {
            if (getLoginUserId(request).intValue() > 0) {
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 获取登陆用户
     *
     * @return User
     * @throws Exception
     */
    public static JsonObject getLoginUser(HttpServletRequest request)  {
        String sid = WebUtils.getCookie(request, "sid");
        if (StringUtils.isNotEmpty(sid)) {
            Object ob =   memCache.get(sid);
            if(ObjectUtils.isNotNull(ob)){
                JsonObject user=  jsonParser.parse(ob.toString()).getAsJsonObject();
                return user;
            }
        }
        return null;
    }


}
