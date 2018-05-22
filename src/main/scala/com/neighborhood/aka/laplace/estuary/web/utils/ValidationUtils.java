package com.neighborhood.aka.laplace.estuary.web.utils;

import org.springframework.util.Assert;

import java.util.List;

public class ValidationUtils {

    public static void notAllNull(String message, Object... obj) {
        int i = 0;
        for (Object o : obj) {
            if (o != null) {
                i++;
            }
        }
        Assert.isTrue(i > 0, message);
    }

    public static void notNull(Object obj, String message) {
        Assert.notNull(obj, message);
    }

    public static void notblank(String obj, String message) {
        Assert.isTrue(obj.trim() != "",message);
    }
    public static void notZero(int num,String message) {
        Assert.isTrue(num != 0,message);
    }
    public static void notEmpty(List obj, String message) {
        Assert.isTrue(!obj.isEmpty(),message);
    }
}
