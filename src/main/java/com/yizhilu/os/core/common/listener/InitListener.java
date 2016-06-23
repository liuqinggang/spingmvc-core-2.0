package com.yizhilu.os.core.common.listener;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Map;

import javax.servlet.ServletContextEvent;

import com.yizhilu.os.core.util.web.WebUtils;
import org.springframework.web.context.ContextLoaderListener;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import com.yizhilu.os.core.util.PropertyUtil;
import com.yizhilu.os.core.util.Security.PurseSecurityUtils;

/**
 * @ClassName com.yizhilu.os.ssicore.common.InitListener
 * @description
 * @author : qinggang.liu voo@163.com
 * @Create Date : 2014-4-15 下午2:29:08
 */
public class InitListener extends ContextLoaderListener {

    public InitListener() {
    }

    public void contextInitialized(ServletContextEvent servletContextEvent) {
        try{
            super.contextInitialized(servletContextEvent);
        } catch (Exception e) {
            System.exit(4);
        }
    }

}