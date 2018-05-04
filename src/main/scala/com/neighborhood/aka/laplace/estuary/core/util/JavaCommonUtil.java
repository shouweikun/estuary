package com.neighborhood.aka.laplace.estuary.core.util;

import java.util.regex.Pattern;

/**
 * Created by john_liu on 2018/5/4.
 */
public class JavaCommonUtil {
    public static boolean isInteger(String str) {
        Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");
        return pattern.matcher(str).matches();
    }
}
