package com.javarush.task.task39.task3913;

import com.javarush.task.task39.task3913.query.DateQuery;
import com.javarush.task.task39.task3913.query.EventQuery;
import com.javarush.task.task39.task3913.query.IPQuery;
import com.javarush.task.task39.task3913.query.UserQuery;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LogParser implements IPQuery, UserQuery, DateQuery, EventQuery {


    private final Path logDir;

    public LogParser(Path logDir) {
        this.logDir = logDir;
    }


    //---------------This block code in charge of return user's IP---------------//


    @Override
    public int getNumberOfUniqueIPs(Date after, Date before) {
        return getUniqueIPs(after, before).size();
    }

    @Override
    public Set<String> getUniqueIPs(Date after, Date before) {
        return getNeededTimeLapse(after, before)
                .map(logLine -> logLine.ip)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<String> getIPsForUser(String user, Date after, Date before) {
        return getNeededTimeLapse(after, before)
                .filter(logLine -> logLine.user.equals(user))
                .map(logLine -> logLine.ip)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<String> getIPsForEvent(Event event, Date after, Date before) {
        return getNeededTimeLapse(after, before)
                .filter(logLine -> logLine.event.equals(event))
                .map(logLine -> logLine.ip)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<String> getIPsForStatus(Status status, Date after, Date before) {
        return getNeededTimeLapse(after, before)
                .filter(logLine -> logLine.status.equals(status))
                .map(logLine -> logLine.ip)
                .collect(Collectors.toSet());

    }



    // -------The following block code is responsible for returning the user to the received attributes --------//


    @Override
    public Set<String> getAllUsers() {
        return Objects.requireNonNull(getEvents())
                .map(eventLog -> eventLog.user)
                .collect(Collectors.toSet());
    }

    @Override
    public int getNumberOfUsers(Date after, Date before) {
        return getNeededTimeLapse(after, before)
                .map(eventLog -> eventLog.user)
                .collect(Collectors.toSet()).size();
    }

    @Override
    public int getNumberOfUserEvents(String user, Date after, Date before) {
        return (int) getNeededTimeLapse(after, before)
                .parallel()
                .filter(eventLog -> eventLog.user.equals(user))
                .map(eventLog -> eventLog.event)
                .distinct()
                .count();
    }

    @Override
    public Set<String> getUsersForIP(String ip, Date after, Date before) {
        return getNeededTimeLapse(after, before)
                .filter(eventLog -> eventLog.ip.equals(ip))
                .map(eventLog -> eventLog.user)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<String> getLoggedUsers(Date after, Date before) {
        return getUserForEvent(Event.LOGIN, after, before);
    }

    @Override
    public Set<String> getDownloadedPluginUsers(Date after, Date before) {
        return getUserForEvent(Event.DOWNLOAD_PLUGIN, after, before);
    }

    @Override
    public Set<String> getWroteMessageUsers(Date after, Date before) {
        return getUserForEvent(Event.WRITE_MESSAGE, after, before);
    }

    @Override
    public Set<String> getSolvedTaskUsers(Date after, Date before) {
        return getUserForEvent(Event.SOLVE_TASK, after, before);
    }

    @Override
    public Set<String> getSolvedTaskUsers(Date after, Date before, int task) {
        return getNeededTimeLapse(after, before)
                .filter(eventLog -> eventLog.event.equals(Event.SOLVE_TASK))
                .filter(eventLog -> eventLog.numberTask.equals(task))
                .map(eventLog -> eventLog.user)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<String> getDoneTaskUsers(Date after, Date before) {
        return getUserForEvent(Event.DONE_TASK, after, before);
    }

    @Override
    public Set<String> getDoneTaskUsers(Date after, Date before, int task) {
        return getNeededTimeLapse(after, before)
                .filter(eventLog -> eventLog.event.equals(Event.DONE_TASK))
                .filter(eventLog -> eventLog.numberTask.equals(task))
                .map(eventLog -> eventLog.user)
                .collect(Collectors.toSet());
    }



    // -------The following block code is responsible for returning the Date to the received attributes --------//



    @Override
    public Set<Date> getDatesForUserAndEvent(String user, Event event, Date after, Date before) {
        return getNeededTimeLapse(after, before)
                .filter(eventEntity -> eventEntity.user.equals(user))
                .filter(eventEntity -> eventEntity.event.equals(event))
                .map(eventEntity -> eventEntity.date)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<Date> getDatesWhenSomethingFailed(Date after, Date before) {
        return getNeededTimeLapse(after, before)
                .filter(eventEntity -> eventEntity.status.equals(Status.FAILED))
                .map(eventEntity -> eventEntity.date)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<Date> getDatesWhenErrorHappened(Date after, Date before) {
        return getNeededTimeLapse(after, before)
                .filter(eventEntity -> eventEntity.status.equals(Status.ERROR))
                .map(eventEntity -> eventEntity.date)
                .collect(Collectors.toSet());
    }

    @Override
    public Date getDateWhenUserLoggedFirstTime(String user, Date after, Date before) {
        return getNeededTimeLapse(after, before)
                .filter(eventEntity -> eventEntity.user.equals(user))
                .filter(eventEntity -> eventEntity.event.equals(Event.LOGIN))
                .map(eventEntity -> eventEntity.date)
                .min(Date::compareTo)
                .orElse(null);
    }

    @Override
    public Date getDateWhenUserSolvedTask(String user, int task, Date after, Date before) {
        return getNeededTimeLapse(after, before)
                .filter(eventEntity -> eventEntity.user.equals(user))
                .filter(eventEntity -> eventEntity.event.equals(Event.SOLVE_TASK))
                .filter(eventEntity -> eventEntity.numberTask.equals(task))
                .map(eventEntity -> eventEntity.date)
                .min(Date::compareTo)
                .orElse(null);
    }

    @Override
    public Date getDateWhenUserDoneTask(String user, int task, Date after, Date before) {
        return getNeededTimeLapse(after, before)
                .filter(eventEntity -> eventEntity.user.equals(user))
                .filter(eventEntity -> eventEntity.event.equals(Event.DONE_TASK))
                .filter(eventEntity -> eventEntity.numberTask.equals(task))
                .map(eventEntity -> eventEntity.date)
                .min(Date::compareTo)
                .orElse(null);
    }

    @Override
    public Set<Date> getDatesWhenUserWroteMessage(String user, Date after, Date before) {
        return getNeededTimeLapse(after, before)
                .filter(eventEntity -> eventEntity.user.equals(user))
                .filter(eventEntity -> eventEntity.event.equals(Event.WRITE_MESSAGE))
                .map(eventEntity -> eventEntity.date)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<Date> getDatesWhenUserDownloadedPlugin(String user, Date after, Date before) {
        return getNeededTimeLapse(after, before)
                .filter(eventEntity -> eventEntity.user.equals(user))
                .filter(eventEntity -> eventEntity.event.equals(Event.DOWNLOAD_PLUGIN))
                .map(eventEntity -> eventEntity.date)
                .collect(Collectors.toSet());
    }


    //-----The following block code is responsible for returning the Event to the received attributes-----/


    @Override
    public int getNumberOfAllEvents(Date after, Date before) {
        return (int) getNeededTimeLapse(after, before)
                .map(eventEntity -> eventEntity.event)
                .distinct()
                .count();
    }

    @Override
    public Set<Event> getAllEvents(Date after, Date before) {
        return getNeededTimeLapse(after, before)
                .map(eventEntity -> eventEntity.event)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<Event> getEventsForIP(String ip, Date after, Date before) {
        return getNeededTimeLapse(after, before)
                .filter(eventEntity -> eventEntity.ip.equals(ip))
                .map(eventEntity -> eventEntity.event)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<Event> getEventsForUser(String user, Date after, Date before) {
        return getNeededTimeLapse(after, before)
                .filter(eventEntity -> eventEntity.user.equals(user))
                .map(eventEntity -> eventEntity.event)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<Event> getFailedEvents(Date after, Date before) {
        return getNeededTimeLapse(after, before)
                .filter(eventEntity -> eventEntity.status.equals(Status.FAILED))
                .map(eventEntity -> eventEntity.event)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<Event> getErrorEvents(Date after, Date before) {
        return getNeededTimeLapse(after, before)
                .filter(eventEntity -> eventEntity.status.equals(Status.ERROR))
                .map(eventEntity -> eventEntity.event)
                .collect(Collectors.toSet());
    }

    @Override
    public int getNumberOfAttemptToSolveTask(int task, Date after, Date before) {
        return (int) getNeededTimeLapse(after, before)
                .filter(eventEntity -> eventEntity.event.equals(Event.SOLVE_TASK))
                .filter(eventEntity -> eventEntity.numberTask.equals(task))
                .count();
    }

    @Override
    public int getNumberOfSuccessfulAttemptToSolveTask(int task, Date after, Date before) {
        return (int) getNeededTimeLapse(after, before)
                .filter(eventEntity -> eventEntity.event.equals(Event.DONE_TASK))
                .filter(eventEntity -> eventEntity.numberTask.equals(task))
                .count();
    }

    @Override
    public Map<Integer, Integer> getAllSolvedTasksAndTheirNumber(Date after, Date before) {
        return getNeededTimeLapse(after, before)
                .filter(eventEntity -> eventEntity.event.equals(Event.SOLVE_TASK))
                .filter(eventEntity -> eventEntity.numberTask != null)
                .collect(HashMap::new,(map, eventEntity)-> map.merge(eventEntity.numberTask,1,Integer::sum), HashMap::putAll);
    }

    @Override
    public Map<Integer, Integer> getAllDoneTasksAndTheirNumber(Date after, Date before) {
        return getNeededTimeLapse(after, before)
                .filter(eventEntity -> eventEntity.event.equals(Event.DONE_TASK))
                .filter(eventEntity -> eventEntity.numberTask != null)
                .collect(HashMap::new,(map, eventEntity)-> map.merge(eventEntity.numberTask,1,Integer::sum), HashMap::putAll);
    }


    //---------- The next classes and methods are auxiliary------------//

    /**
     * Util method which return all lines of log as List consist of objects logLine
     */

    private Stream<EventEntity> getEvents() {
        try {
           return Files.list(logDir)
                    .filter(path -> path.toString().endsWith(".log"))
                    .flatMap(path -> {
                        try {
                            return Files.lines(path)
                                    .map(s -> s.split("\t"))
                                    .map(sArray -> new EventEntity(sArray[0], sArray[1], sArray[2], sArray[3], sArray[4]));
                        } catch (IOException e) {
                            e.printStackTrace();
                            return null;
                        }
                    }).filter(Objects::nonNull);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * It goes without saying
     */

    private Set<String> getUserForEvent(Event event, Date after, Date before) {
        return getNeededTimeLapse(after, before)
                .filter(eventLog -> eventLog.event.equals(event))
                .map(eventLog -> eventLog.user)
                .collect(Collectors.toSet());
    }

    /**
     * This method returns list<LogLines> from specified time lapse
     **/

    private Stream<EventEntity> getNeededTimeLapse(Date after, Date before) {
        Stream<EventEntity> result = Objects.requireNonNull(getEvents());
        if (after != null && before != null) {
            return result
                    .filter(event -> (event.date.after(after) || event.date.equals(after)) && (event.date.before(before) || event.date.equals(before)));
        } else if (after == null && before != null) {
            return result
                    .filter(event -> event.date.before(before) || event.date.equals(before));
        } else if (after != null) {
            return result
                    .filter(event -> event.date.after(after) || event.date.equals(after));
        } else {
            return result;
        }
    }

    /**
     * This class represent 1 line of log with attributes as fields
     */

    private static class EventEntity {
        private String ip;
        private String user;
        private Date date;
        private Event event;
        private Integer numberTask;
        private Status status;

        public EventEntity(String ip, String user, String date, String event, String status) {
            this.ip = ip;
            this.user = user;
            try {
                this.date = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").parse(date);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            try {
                this.event = Event.valueOf(event);
            } catch (IllegalArgumentException e) {
                this.event = Event.valueOf(event.split(" ")[0]);
                this.numberTask = Integer.parseInt(event.split(" ")[1]);
            }
            this.status = Status.valueOf(status);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            EventEntity that = (EventEntity) o;
            return Objects.equals(ip, that.ip) &&
                    Objects.equals(user, that.user) &&
                    Objects.equals(date, that.date) &&
                    event == that.event &&
                    Objects.equals(numberTask, that.numberTask) &&
                    status == that.status;
        }

        @Override
        public int hashCode() {
            return Objects.hash(ip, user, date, event, numberTask, status);
        }

        @Override
        public String toString() {
            return "LogLine{" +
                    "ip='" + ip + '\'' +
                    ", user='" + user + '\'' +
                    ", date=" + date +
                    ", event=" + event +
                    ", numberTask=" + numberTask +
                    ", status=" + status +
                    '}';
        }
    }
}