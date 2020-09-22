package com.reactiveminds.psi;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

public class SpringContextWrapper implements ApplicationContextAware {
    private static ApplicationContext applicationContext;
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        SpringContextWrapper.applicationContext = applicationContext;
    }

    public static ApplicationContext getContext(){
        Assert.notNull(applicationContext, "ApplicationContext not initialized!");
        return applicationContext;
    }
    public static <T> T getBean(Class<T> ofTYpe, Object ...args){
        return getContext().getBean(ofTYpe, args);
    }
    public static <T> T getBean(String ofName, Object ...args){
        return (T) getContext().getBean(ofName, args);
    }
}
