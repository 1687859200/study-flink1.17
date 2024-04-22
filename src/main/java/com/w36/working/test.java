package com.w36.working;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class test {
    public static void main(String[] args) {
//        HashMap<String, List<String>> hashMap = new HashMap<>();
//
//        ArrayList<String> list1 = new ArrayList<>();
//        list1.add("111");
//        list1.add("222");
//        hashMap.put("AA", list1);
//        List<String> aa = hashMap.get("AA");
//        System.out.println(aa.get(0));

        String s1 = "2024-04-19 10:41:19";
        String s2 = "2024-04-30 10:41:19";
        ArrayList<ArrayList<String>> arrayLists = dateCut(s1, s2);
        for (ArrayList<String> arrayList : arrayLists) {
            System.out.println(arrayList.toString());
        }
    }
    public static ArrayList<ArrayList<String>>dateCut(String start, String end){
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime startTime = LocalDateTime.parse(start, formatter);
        LocalDateTime endTime = LocalDateTime.parse(end, formatter);
        ArrayList<ArrayList<String>> result = new ArrayList<>();
        long days = Duration.between(startTime, endTime).toDays();
        String part = " 08:00:00";
        for (long l = 0; l < days+2; l++) {
            LocalDateTime plus = startTime.plusDays(l);
            String tmp = plus.toLocalDate().toString()+part;
            if(start.compareTo(tmp) < 0 && end.compareTo(tmp) > 0){
                ArrayList<String> list = new ArrayList<>();
                list.add(start);
                list.add(tmp);
                start = tmp;
                result.add(list);
            }
        }
        ArrayList<String> list1 = new ArrayList<>();
        list1.add(start);
        list1.add(end);
        result.add(list1);
        return result;
    }
}
