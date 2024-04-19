package com.w36.working;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class test {
    public static void main(String[] args) {
        HashMap<String, List<String>> hashMap = new HashMap<>();

        ArrayList<String> list1 = new ArrayList<>();
        list1.add("111");
        list1.add("222");
        hashMap.put("AA", list1);
        List<String> aa = hashMap.get("AA");
        System.out.println(aa.get(0));
    }
}
