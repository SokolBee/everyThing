package com.javarush.task.task39.task3913;

import java.nio.file.Paths;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;

public class Solution {
    public static void main(String[] args) {
        LogParser logParser = new LogParser(Paths.get("C:\\Users\\waumo\\Documents\\project\\JavaRushTasks\\4.JavaCollections\\src\\com\\javarush\\task\\task39\\task3913\\logs"));
        logParser.getAllUsers().forEach(System.out::println);

        System.out.println(logParser.getAllSolvedTasksAndTheirNumber(null,null));
        System.out.println(logParser.getAllDoneTasksAndTheirNumber(null,null));
    }
}