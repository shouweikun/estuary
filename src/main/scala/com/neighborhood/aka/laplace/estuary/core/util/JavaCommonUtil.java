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

    public static boolean isEmpty(Object str) {
        return str == null || str.equals("");
    }

    public static boolean nonEmpty(String str) {
        return !isEmpty(str);
    }


    public static boolean hasLength(CharSequence str) {
        return str != null && str.length() > 0;
    }

    public static boolean hasLength(String str) {
        return str != null && !str.isEmpty();
    }

    public static boolean hasText(CharSequence str) {
        return hasLength(str) && containsText(str);
    }

    public static boolean hasText(String str) {
        return hasLength(str) && containsText(str);
    }

    private static boolean containsText(CharSequence str) {
        int strLen = str.length();

        for(int i = 0; i < strLen; ++i) {
            if(!Character.isWhitespace(str.charAt(i))) {
                return true;
            }
        }

        return false;
    }
}
