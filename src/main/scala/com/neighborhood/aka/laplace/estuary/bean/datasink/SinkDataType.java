package com.neighborhood.aka.laplace.estuary.bean.datasink;

import java.util.*;

public enum SinkDataType {
    KAFKA("KAFKA"), HBASE("HBASE"), MYSQL("MYSQL"), HDFS("HDFS");
    private String value;

    SinkDataType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }


    public int lengthOfLongestSubstring(String s) {
        Set<Character> set = new HashSet<>();
        int max = 1;
        int left = 0;
        int right = 0;
        while (right < s.length()) {
            if (!set.contains(s.charAt(right))) {
                set.add(s.charAt(right));
                Math.max(max, set.size());
                right++;
            } else {
                set.remove(s.charAt(left));
                left++;
            }
        }
        return left;
    }

    public String longestCommonPrefix(String[] strs) {
        if (strs.length == 0) return "";
        if (strs.length == 1) return strs[0];
        int count = 0;
        for (int i = 0; i < strs[0].length(); i++) {
            for (int j = 1; j < strs.length; j++) {
                if (count >= strs[j].length()) return count > 0 ? "" : strs[0].substring(0, count - 1);
                if (strs[j].charAt(count) != strs[j - 1].charAt(count))
                    return count > 0 ? "" : strs[0].substring(0, count - 1);
            }
            count = count + 1;

        }
        return strs[0];
    }


}
