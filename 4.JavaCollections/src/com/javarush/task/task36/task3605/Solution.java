package com.javarush.task.task36.task3605;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.TreeSet;
import java.util.stream.Collectors;

/* 
The Validator doesn't take such solution to task3605
but it works properly and this is the main point =)
*/


public class Solution {
    public static void main(String[] args) throws IOException {
        TreeSet<String> listStr = new TreeSet<>(String::compareTo);
        List<String> str = Files.lines(Paths.get(args[0]))
                .map(s -> s.split(""))
                .flatMap(Arrays::stream)
                .map(s -> s = s.toLowerCase(Locale.ENGLISH))
                .distinct()
                .filter(s -> Character.isLetter(s.codePointAt(0)))
                .collect(Collectors.toList());
        listStr.addAll(str);
        listStr.stream()
                .limit(5)
                .forEach(System.out::print);
    }
}
