package com.dj.boot.canal.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.AnnotatedBeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.type.MethodMetadata;
import org.springframework.stereotype.Component;
import org.springframework.util.ClassUtils;

import java.lang.annotation.Annotation;
import java.util.*;

/**
 * <br>
 * <p></p>
 *
 * <br>
 *
 * @author mason
 * @version 1.0
 * @date 2021/7/27 上午11:08
 */
@Slf4j
@Component
public class BootBeanFactory implements ApplicationContextAware, DisposableBean {
    private static ApplicationContext applicationContext;

    public static String[] getBeanNamesForType(ConfigurableListableBeanFactory bf, Class<?> type){
        if(bf != null) {
            Iterator<String> it = bf.getBeanNamesIterator();
            List<String> bNames = new ArrayList<>();
            while (it.hasNext()) {
                String bName = it.next();
                if(bf.containsBeanDefinition(bName)) {
                    AbstractBeanDefinition bd = (AbstractBeanDefinition) bf.getBeanDefinition(bName);
                    Class clazz = null;
                    if (bd.hasBeanClass()) {
                        clazz = bd.getBeanClass();
                    } else if (bd instanceof AnnotatedBeanDefinition) {
                        MethodMetadata mm = ((AnnotatedBeanDefinition) bd).getFactoryMethodMetadata();
                        if (mm != null) {
                            try {
                                clazz = ClassUtils.forName(mm.getReturnTypeName(), Thread.currentThread().getContextClassLoader());
                            } catch (ClassNotFoundException e) {
                            }
                        }
                    }
                    if(clazz == null){
                        try {
                            clazz = ClassUtils.forName(bd.getBeanClassName(),Thread.currentThread().getContextClassLoader());
                        } catch (ClassNotFoundException e) {}
                    }

                    if (type.isAssignableFrom(clazz)) {
                        bNames.add(bName);
                    }
                }
            }
            return bNames.toArray(new String[]{});
        }
        return null;
    }

    public static String[] getBeanNamesForAnnotation(ConfigurableListableBeanFactory bf, Class<? extends Annotation> annotationClass){
        if(bf != null) {
            Iterator<String> it = bf.getBeanNamesIterator();
            List<String> bNames = new ArrayList<>();
            while (it.hasNext()) {
                String bName = it.next();
                if(bf.containsBeanDefinition(bName)) {
                    AbstractBeanDefinition bd = (AbstractBeanDefinition) bf.getBeanDefinition(bName);
                    Class clazz = null;
                    if (bd.hasBeanClass()) {
                        clazz = bd.getBeanClass();
                    } else if(bd instanceof AnnotatedBeanDefinition) {
                        MethodMetadata mm = ((AnnotatedBeanDefinition) bd).getFactoryMethodMetadata();
                        if (mm != null) {
                            try {
                                clazz = ClassUtils.forName(mm.getReturnTypeName(), Thread.currentThread().getContextClassLoader());
                            } catch (ClassNotFoundException e) {
                            }
                        }
                    }
                    if(clazz == null){
                        try {
                            clazz = ClassUtils.forName(bd.getBeanClassName(),Thread.currentThread().getContextClassLoader());
                        } catch (ClassNotFoundException e) {}
                    }
                    Annotation annotation = AnnotationUtils.findAnnotation(clazz, annotationClass);
                    if (annotation != null) {
                        bNames.add(bName);
                    }
                }
            }
            return bNames.toArray(new String[]{});
        }
        return null;
    }


    /**
     * 根据Bean名称获取实例
     *
     * @return bean实例
     * @throws BeansException BeansException
     */
    @SuppressWarnings("unchecked")
    public static <T> T getBean(String name) throws BeansException {
        assertContextInjected();
        return (T) applicationContext.getBean(name);
    }

    /**
     * 根据类型获取实例
     *
     * @param type 类型
     * @return bean实例
     * @throws BeansException BeansException
     */
    public static <T> T getBean(Class<T> type) throws BeansException {
        assertContextInjected();
        String beanName = StringUtils.uncapitalize(type.getSimpleName());
        T bean = applicationContext.getBean(beanName, type);
        if (null != bean) {
            return bean;
        }
        return applicationContext.getBean(type);
    }

    /**
     * 根据类型获取Bean,可能存在多个事例,默认取第一个
     *
     * @param type 类型
     * @param <T>  泛型
     * @return bean实例
     * @throws BeansException BeansException
     */
    public static <T> T getBeanByType(Class<T> type) throws BeansException {
        assertContextInjected();
        Map<String, T> beanMap = applicationContext.getBeansOfType(type);
        if (beanMap.values().iterator().hasNext()) {
            return beanMap.values().iterator().next();
        }

        return null;
    }

    public static <T> Collection<T> getBeansByType(Class<T> type) throws BeansException {
        assertContextInjected();
        Map<String, T> beanMap = applicationContext.getBeansOfType(type);
        return beanMap.values();
    }

    /**
     * 根据类型获取Spring Bean名称
     *
     * @param type type
     * @return Bean名称
     * @throws BeansException BeansException
     */
    public static String getBeanNamesForType(Class type) throws BeansException {
        assertContextInjected();
        String[] beanNames = applicationContext.getBeanNamesForType(type);
        if (ArrayUtils.isNotEmpty(beanNames)) {
            return beanNames[0];
        }

        return "";
    }

    /**
     * 根据注解获取Bean
     *
     * @param annotationType 注解类型
     * @return Bean map
     * @throws BeansException BeansException
     */
    public static Map<String, Object> getBeansWithAnnotation(Class<? extends Annotation> annotationType) throws BeansException {
        return applicationContext.getBeansWithAnnotation(annotationType);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        BootBeanFactory.applicationContext = applicationContext;
    }

    @Override
    public void destroy() throws Exception {
        BootBeanFactory.clear();
    }

    public static void clear() {
        if (log.isDebugEnabled()){
            log.debug("清除SpringContextHolder中的ApplicationContext:" + applicationContext);
        }
        applicationContext = null;
    }

    public static ApplicationContext getApplicationContext() {
        assertContextInjected();
        return applicationContext;
    }

    private static void assertContextInjected() {
        if (applicationContext == null) {
            throw new IllegalStateException("applicaitonContext未注入");
        }
    }


}
