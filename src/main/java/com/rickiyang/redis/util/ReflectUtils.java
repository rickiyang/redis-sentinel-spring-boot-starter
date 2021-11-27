package com.rickiyang.redis.util;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;

/**
 * @date: 2021/11/16 10:02 上午
 * @author: rickiyang
 * @Description:
 */
public class ReflectUtils {

    /**
     * 递归获取指定类的方法
     *
     * @param clz
     * @param methodName
     * @param recursion
     * @param parameterTypes
     * @return
     */
    public static Method getClassMethod(Class<?> clz, String methodName, boolean recursion,
                                        Class<?>... parameterTypes) {
        try {
            Method m = clz.getDeclaredMethod(methodName, parameterTypes);
            if (null != m) {
                return m;
            }
        } catch (Exception e) {

        }
        if (recursion) {
            Class<?> superclass = clz.getSuperclass();
            if (superclass != null) {
                return getClassMethod(superclass, methodName, recursion, parameterTypes);
            }
        }
        return null;
    }

    /**
     * 获取类里面的某个字段，在递归获取的情况下，子类定义的字段优先返回
     *
     * @param clz       类
     * @param fieldName 字段名
     * @param recursion 是否递归获取父类的字段
     * @return
     * @throws Exception
     */
    public static Field getClassField(Class<?> clz, String fieldName, boolean recursion) {
        Field[] declaredFields = clz.getDeclaredFields();
        for (Field field : declaredFields) {
            if (field.getName().equals(fieldName)) {
                return field;
            }
        }

        if (recursion) {
            Class<?> superclass = clz.getSuperclass();
            if (superclass != null) {
                return getClassField(superclass, fieldName, recursion);
            }
        }
        return null;
    }

    /**
     * 获取类定义的所有有指定注解的字段
     *
     * @param clz
     * @param recursion 是否递归获取父类的字段
     * @return
     * @throws Exception
     */
    public static List<Field> getAnnotationedClassFields(Class<?> clz, boolean recursion,
                                                         Class<? extends Annotation> annotation) {
        List<Field> fields = new ArrayList<Field>();
        getAnnotationedClassFields(clz, recursion, fields, annotation);
        return fields;
    }

    private static void getAnnotationedClassFields(Class<?> clz, boolean recursion, List<Field> fields,
                                                   Class<? extends Annotation> annotation) {
        Field[] declaredFields = clz.getDeclaredFields();
        for (Field f : declaredFields) {
            Annotation anno = f.getAnnotation(annotation);
            if (null != anno) {
                fields.add(f);
            }
        }
        if (recursion) {
            Class<?> superclass = clz.getSuperclass();
            if (superclass != null) {
                getAnnotationedClassFields(superclass, recursion, fields, annotation);
            }
        }
    }

    /**
     * 获取类定义的所有字段
     *
     * @param clz
     * @param recursion 是否递归获取父类的字段
     * @return
     * @throws Exception
     */
    public static List<Field> getClassFields(Class<?> clz, boolean recursion) {
        List<Field> fields = new ArrayList<Field>();
        getClassFields(clz, recursion, fields);
        return fields;
    }

    private static void getClassFields(Class<?> clz, boolean recursion, List<Field> fields) {
        Field[] declaredFields = clz.getDeclaredFields();
        for (Field f : declaredFields) {
            fields.add(f);
        }
        if (recursion) {
            Class<?> superclass = clz.getSuperclass();
            if (superclass != null) {
                getClassFields(superclass, recursion, fields);
            }
        }
    }

    /**
     * 获取某个字段的值
     *
     * @param obj
     * @param fieldName
     * @param recursion 是否递归获取父类的字段
     * @return
     * @throws NoSuchFieldException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     * @throws Exception
     */
    public static Object getFieldValue(Object obj, String fieldName, boolean recursion)
            throws NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
        Field f = getClassField(obj.getClass(), fieldName, recursion);
        if (null != f) {
            f.setAccessible(true);
            return f.get(obj);
        } else {
            throw new NoSuchFieldException();
        }
    }

    /**
     * 获取对象的字段的值，返回map，key为对象字段的名称
     *
     * @param obj
     * @param recursion 是否递归获取父类的字段
     * @return
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     * @throws Exception
     */
    public static Map<String, Object> getFieldsValueMap(Object obj, boolean recursion)
            throws IllegalArgumentException, IllegalAccessException {
        Map<String, Object> ret = new HashMap<String, Object>();
        List<Field> fields = getClassFields(obj.getClass(), recursion);
        if (fields.size() > 0) {
            Collections.reverse(fields); // 由于递归遍历，父类定义的字段会放后面，为了保证子类覆盖父类，反转list
            for (Field f : fields) {
                f.setAccessible(true);
                Object res = f.get(obj);
                ret.put(f.getName(), res);
            }
        }
        return ret;
    }

}
