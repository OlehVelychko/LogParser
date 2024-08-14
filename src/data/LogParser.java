package data;

import query.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public class LogParser implements IPQuery, UserQuery, DateQuery, EventQuery, QLQuery {
    private final Path logDir;
    private final List<LogEntity> logEntities = new ArrayList<>();
    private final DateFormat simpleDateFormat = new SimpleDateFormat("d.M.yyyy H:m:s");

    public LogParser(Path logDir) {
        this.logDir = logDir;
        readLogs();
    }

    private void readLogs() {
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(logDir)) {
            for (Path file : directoryStream) {
                if (file.toString().toLowerCase().endsWith(".log")) {
                    try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
                        String line;
                        while ((line = reader.readLine()) != null) {
                            String[] params = line.split("\t");

                            if (params.length != 5) {
                                continue;
                            }

                            String ip = params[0];
                            String user = params[1];
                            Date date = readDate(params[2]);
                            Event event = readEvent(params[3]);
                            int eventAdditionalParameter = -1;
                            if (event.equals(Event.SOLVE_TASK) || event.equals(Event.DONE_TASK)) {
                                eventAdditionalParameter = readAdditionalParameter(params[3]);
                            }
                            Status status = readStatus(params[4]);

                            LogEntity logEntity = new LogEntity(ip, user, date, event, eventAdditionalParameter, status);
                            logEntities.add(logEntity);
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Date readDate(String lineToParse) {
        Date date = null;
        try {
            date = simpleDateFormat.parse(lineToParse);
        } catch (ParseException e) {
        }
        return date;
    }

    private Event readEvent(String lineToParse) {
        if (lineToParse.contains("SOLVE_TASK")) return Event.SOLVE_TASK;
        if (lineToParse.contains("DONE_TASK")) return Event.DONE_TASK;
        return eventMap.get(lineToParse);
    }

    private static final Map<String, Event> eventMap = Map.of(
            "LOGIN", Event.LOGIN,
            "DOWNLOAD_PLUGIN", Event.DOWNLOAD_PLUGIN,
            "WRITE_MESSAGE", Event.WRITE_MESSAGE
    );

    private int readAdditionalParameter(String lineToParse) {
        return extractTaskParameter(lineToParse, Event.SOLVE_TASK) != -1 ?
                extractTaskParameter(lineToParse, Event.SOLVE_TASK) :
                extractTaskParameter(lineToParse, Event.DONE_TASK);
    }

    private int extractTaskParameter(String lineToParse, Event event) {
        if (lineToParse.contains(event.name())) {
            return Integer.parseInt(lineToParse.replace(event.name(), "").replaceAll(" ", ""));
        }
        return -1;
    }

    private Status readStatus(String lineToParse) {
        return statusMap.get(lineToParse);
    }

    private static final Map<String, Status> statusMap = Map.of(
            "OK", Status.OK,
            "FAILED", Status.FAILED,
            "ERROR", Status.ERROR
    );

    @Override
    public int getNumberOfUniqueIPs(Date after, Date before) {
        return getUniqueIPs(after, before).size();
    }

    @Override
    public Set<String> getUniqueIPs(Date after, Date before) {
        return logEntities.stream()
                .filter(logEntity -> isLogEntityValid(logEntity, null, null, null, after, before))
                .map(LogEntity::getIP)
                .collect(Collectors.toSet());
    }

    private boolean isLogEntityValid(LogEntity logEntity, String user, Event event, Status status, Date after, Date before) {
        boolean isUserMatch = (user == null || logEntity.getUser().equals(user));
        boolean isEventMatch = (event == null || logEntity.getEvent().equals(event));
        boolean isStatusMatch = (status == null || logEntity.getStatus().equals(status));
        boolean isDateMatch = (after == null || logEntity.getDate().after(after) || logEntity.getDate().equals(after)) &&
                (before == null || logEntity.getDate().before(before) || logEntity.getDate().equals(before));
        return isUserMatch && isEventMatch && isStatusMatch && isDateMatch;
    }

    @Override
    public Set<String> getIPsForUser(String user, Date after, Date before) {
        return logEntities.stream()
                .filter(logEntity -> isLogEntityValid(logEntity, user, null, null, after, before))
                .map(LogEntity::getIP)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<String> getIPsForStatus(Status status, Date after, Date before) {
        return logEntities.stream()
                .filter(logEntity -> logEntity.getStatus().equals(status) &&
                        (after == null || logEntity.getDate().after(after) || logEntity.getDate().equals(after)) &&
                        (before == null || logEntity.getDate().before(before) || logEntity.getDate().equals(before)))
                .map(LogEntity::getIP)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<String> getIPsForEvent(Event event, Date after, Date before) {
        return logEntities.stream()
                .filter(logEntity -> logEntity.getEvent().equals(event) &&
                        (after == null || logEntity.getDate().after(after) || logEntity.getDate().equals(after)) &&
                        (before == null || logEntity.getDate().before(before) || logEntity.getDate().equals(before)))
                .map(LogEntity::getIP)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<String> getAllUsers() {
        return logEntities.stream()
                .map(LogEntity::getUser)
                .collect(Collectors.toSet());
    }

    @Override
    public int getNumberOfUsers(Date after, Date before) {
        return logEntities.stream()
                .filter(logEntity -> (after == null || logEntity.getDate().after(after) || logEntity.getDate().equals(after)) &&
                        (before == null || logEntity.getDate().before(before) || logEntity.getDate().equals(before)))
                .map(LogEntity::getUser)
                .collect(Collectors.toSet()).size();
    }

    @Override
    public int getNumberOfUserEvents(String user, Date after, Date before) {
        return logEntities.stream()
                .filter(logEntity -> logEntity.getUser().equals(user) &&
                        (after == null || logEntity.getDate().after(after) || logEntity.getDate().equals(after)) &&
                        (before == null || logEntity.getDate().before(before) || logEntity.getDate().equals(before)))
                .map(LogEntity::getEvent)
                .collect(Collectors.toSet()).size();
    }

    @Override
    public Set<String> getUsersForIP(String ip, Date after, Date before) {
        return logEntities.stream()
                .filter(logEntity -> logEntity.getIP().equals(ip) &&
                        (after == null || logEntity.getDate().after(after) || logEntity.getDate().equals(after)) &&
                        (before == null || logEntity.getDate().before(before) || logEntity.getDate().equals(before)))
                .map(LogEntity::getUser)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<String> getLoggedUsers(Date after, Date before) {
        return logEntities.stream()
                .filter(logEntity -> logEntity.getEvent().equals(Event.LOGIN) &&
                        (after == null || logEntity.getDate().after(after) || logEntity.getDate().equals(after)) &&
                        (before == null || logEntity.getDate().before(before) || logEntity.getDate().equals(before)))
                .map(LogEntity::getUser)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<String> getDownloadedPluginUsers(Date after, Date before) {
        return logEntities.stream()
                .filter(logEntity -> logEntity.getEvent().equals(Event.DOWNLOAD_PLUGIN) &&
                        (after == null || logEntity.getDate().after(after) || logEntity.getDate().equals(after)) &&
                        (before == null || logEntity.getDate().before(before) || logEntity.getDate().equals(before)))
                .map(LogEntity::getUser)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<String> getWroteMessageUsers(Date after, Date before) {
        return logEntities.stream()
                .filter(logEntity -> logEntity.getEvent().equals(Event.WRITE_MESSAGE) &&
                        (after == null || logEntity.getDate().after(after) || logEntity.getDate().equals(after)) &&
                        (before == null || logEntity.getDate().before(before) || logEntity.getDate().equals(before)))
                .map(LogEntity::getUser)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<String> getSolvedTaskUsers(Date after, Date before) {
        return logEntities.stream()
                .filter(logEntity -> logEntity.getEvent().equals(Event.SOLVE_TASK) &&
                        (after == null || logEntity.getDate().after(after) || logEntity.getDate().equals(after)) &&
                        (before == null || logEntity.getDate().before(before) || logEntity.getDate().equals(before)))
                .map(LogEntity::getUser)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<String> getSolvedTaskUsers(Date after, Date before, int task) {
        return logEntities.stream()
                .filter(logEntity -> logEntity.getEvent().equals(Event.SOLVE_TASK) &&
                        logEntity.getEventAdditionalParameter() == task &&
                        (after == null || logEntity.getDate().after(after) || logEntity.getDate().equals(after)) &&
                        (before == null || logEntity.getDate().before(before) || logEntity.getDate().equals(before)))
                .map(LogEntity::getUser)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<String> getDoneTaskUsers(Date after, Date before) {
        return logEntities.stream()
                .filter(logEntity -> logEntity.getEvent().equals(Event.DONE_TASK) &&
                        (after == null || logEntity.getDate().after(after) || logEntity.getDate().equals(after)) &&
                        (before == null || logEntity.getDate().before(before) || logEntity.getDate().equals(before)))
                .map(LogEntity::getUser)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<String> getDoneTaskUsers(Date after, Date before, int task) {
        return logEntities.stream()
                .filter(logEntity -> logEntity.getEvent().equals(Event.DONE_TASK) &&
                        logEntity.getEventAdditionalParameter() == task &&
                        (after == null || logEntity.getDate().after(after) || logEntity.getDate().equals(after)) &&
                        (before == null || logEntity.getDate().before(before) || logEntity.getDate().equals(before)))
                .map(LogEntity::getUser)
                .collect(Collectors.toSet());
    }


    @Override
    public Set<Date> getDatesForUserAndEvent(String user, Event event, Date after, Date before) {
        return logEntities.stream()
                .filter(logEntity -> logEntity.getUser().equals(user) &&
                        logEntity.getEvent().equals(event) &&
                        (after == null || logEntity.getDate().after(after) || logEntity.getDate().equals(after)) &&
                        (before == null || logEntity.getDate().before(before) || logEntity.getDate().equals(before)))
                .map(LogEntity::getDate)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<Date> getDatesWhenSomethingFailed(Date after, Date before) {
        return logEntities.stream()
                .filter(logEntity -> logEntity.getStatus().equals(Status.FAILED) &&
                        (after == null || logEntity.getDate().after(after) || logEntity.getDate().equals(after)) &&
                        (before == null || logEntity.getDate().before(before) || logEntity.getDate().equals(before)))
                .map(LogEntity::getDate)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<Date> getDatesWhenErrorHappened(Date after, Date before) {
        return logEntities.stream()
                .filter(logEntity -> logEntity.getStatus().equals(Status.ERROR) &&
                        (after == null || logEntity.getDate().after(after) || logEntity.getDate().equals(after)) &&
                        (before == null || logEntity.getDate().before(before) || logEntity.getDate().equals(before)))
                .map(LogEntity::getDate)
                .collect(Collectors.toSet());
    }

    @Override
    public Date getDateWhenUserLoggedFirstTime(String user, Date after, Date before) {
        return logEntities.stream()
                .filter(logEntity -> logEntity.getUser().equals(user) &&
                        logEntity.getEvent().equals(Event.LOGIN) &&
                        (after == null || logEntity.getDate().after(after) || logEntity.getDate().equals(after)) &&
                        (before == null || logEntity.getDate().before(before) || logEntity.getDate().equals(before)))
                .map(LogEntity::getDate)
                .min(Date::compareTo)
                .orElse(null);
    }


    @Override
    public Date getDateWhenUserSolvedTask(String user, int task, Date after, Date before) {
        return logEntities.stream()
                .filter(logEntity -> logEntity.getUser().equals(user) &&
                        logEntity.getEvent().equals(Event.SOLVE_TASK) &&
                        logEntity.getEventAdditionalParameter() == task &&
                        (after == null || logEntity.getDate().after(after) || logEntity.getDate().equals(after)) &&
                        (before == null || logEntity.getDate().before(before) || logEntity.getDate().equals(before)))
                .map(LogEntity::getDate)
                .min(Date::compareTo)
                .orElse(null);
    }

    @Override
    public Date getDateWhenUserDoneTask(String user, int task, Date after, Date before) {
        return logEntities.stream()
                .filter(logEntity -> logEntity.getUser().equals(user) &&
                        logEntity.getEvent().equals(Event.DONE_TASK) &&
                        logEntity.getEventAdditionalParameter() == task &&
                        (after == null || logEntity.getDate().after(after) || logEntity.getDate().equals(after)) &&
                        (before == null || logEntity.getDate().before(before) || logEntity.getDate().equals(before)))
                .map(LogEntity::getDate)
                .min(Date::compareTo)
                .orElse(null);
    }

    @Override
    public Set<Date> getDatesWhenUserWroteMessage(String user, Date after, Date before) {
        return logEntities.stream()
                .filter(logEntity -> logEntity.getUser().equals(user) &&
                        logEntity.getEvent().equals(Event.WRITE_MESSAGE) &&
                        (after == null || logEntity.getDate().after(after) || logEntity.getDate().equals(after)) &&
                        (before == null || logEntity.getDate().before(before) || logEntity.getDate().equals(before)))
                .map(LogEntity::getDate)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<Date> getDatesWhenUserDownloadedPlugin(String user, Date after, Date before) {
        return logEntities.stream()
                .filter(logEntity -> logEntity.getUser().equals(user) &&
                        logEntity.getEvent().equals(Event.DOWNLOAD_PLUGIN) &&
                        (after == null || logEntity.getDate().after(after) || logEntity.getDate().equals(after)) &&
                        (before == null || logEntity.getDate().before(before) || logEntity.getDate().equals(before)))
                .map(LogEntity::getDate)
                .collect(Collectors.toSet());
    }

    @Override
    public int getNumberOfAllEvents(Date after, Date before) {
        return getAllEvents(after, before).size();
    }

    @Override
    public Set<Event> getAllEvents(Date after, Date before) {
        return logEntities.stream()
                .filter(logEntity -> (after == null || logEntity.getDate().after(after) || logEntity.getDate().equals(after)) &&
                        (before == null || logEntity.getDate().before(before) || logEntity.getDate().equals(before)))
                .map(LogEntity::getEvent)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<Event> getEventsForIP(String ip, Date after, Date before) {
        return logEntities.stream()
                .filter(logEntity -> logEntity.getIP().equals(ip) &&
                        (after == null || logEntity.getDate().after(after) || logEntity.getDate().equals(after)) &&
                        (before == null || logEntity.getDate().before(before) || logEntity.getDate().equals(before)))
                .map(LogEntity::getEvent)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<Event> getEventsForUser(String user, Date after, Date before) {
        return logEntities.stream()
                .filter(logEntity -> logEntity.getUser().equals(user) &&
                        (after == null || logEntity.getDate().after(after) || logEntity.getDate().equals(after)) &&
                        (before == null || logEntity.getDate().before(before) || logEntity.getDate().equals(before)))
                .map(LogEntity::getEvent)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<Event> getFailedEvents(Date after, Date before) {
        return logEntities.stream()
                .filter(logEntity -> logEntity.getStatus().equals(Status.FAILED) &&
                        (after == null || logEntity.getDate().after(after) || logEntity.getDate().equals(after)) &&
                        (before == null || logEntity.getDate().before(before) || logEntity.getDate().equals(before)))
                .map(LogEntity::getEvent)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<Event> getErrorEvents(Date after, Date before) {
        return logEntities.stream()
                .filter(logEntity -> logEntity.getStatus().equals(Status.ERROR) &&
                        (after == null || logEntity.getDate().after(after) || logEntity.getDate().equals(after)) &&
                        (before == null || logEntity.getDate().before(before) || logEntity.getDate().equals(before)))
                .map(LogEntity::getEvent)
                .collect(Collectors.toSet());
    }

    @Override
    public int getNumberOfAttemptToSolveTask(int task, Date after, Date before) {
        return (int) logEntities.stream()
                .filter(logEntity -> logEntity.getEventAdditionalParameter() == task &&
                        logEntity.getEvent().equals(Event.SOLVE_TASK) &&
                        (after == null || logEntity.getDate().after(after) || logEntity.getDate().equals(after)) &&
                        (before == null || logEntity.getDate().before(before) || logEntity.getDate().equals(before)))
                .count();
    }

    @Override
    public int getNumberOfSuccessfulAttemptToSolveTask(int task, Date after, Date before) {
        return (int) logEntities.stream()
                .filter(logEntity -> logEntity.getEventAdditionalParameter() == task &&
                        logEntity.getEvent().equals(Event.DONE_TASK) &&
                        (after == null || logEntity.getDate().after(after) || logEntity.getDate().equals(after)) &&
                        (before == null || logEntity.getDate().before(before) || logEntity.getDate().equals(before)))
                .count();
    }

    @Override
    public Map<Integer, Integer> getAllSolvedTasksAndTheirNumber(Date after, Date before) {
        return logEntities.stream()
                .filter(logEntity -> logEntity.getEvent().equals(Event.SOLVE_TASK) &&
                        (after == null || logEntity.getDate().after(after) || logEntity.getDate().equals(after)) &&
                        (before == null || logEntity.getDate().before(before) || logEntity.getDate().equals(before)))
                .map(LogEntity::getEventAdditionalParameter)
                .collect(Collectors.toMap(task -> task, task -> 1, Integer::sum));
    }

    @Override
    public Map<Integer, Integer> getAllDoneTasksAndTheirNumber(Date after, Date before) {
        return logEntities.stream()
                .filter(logEntity -> logEntity.getEvent().equals(Event.DONE_TASK) &&
                        (after == null || logEntity.getDate().after(after) || logEntity.getDate().equals(after)) &&
                        (before == null || logEntity.getDate().before(before) || logEntity.getDate().equals(before)))
                .map(LogEntity::getEventAdditionalParameter)
                .collect(Collectors.toMap(task -> task, task -> 1, Integer::sum));
    }

    @Override
    public Set<?> execute(String query) {
        return switch (query) {
            case "get ip" -> new HashSet<>(getUniqueIPs(null, null));
            case "get user" -> new HashSet<>(getAllUsers());
            case "get date" -> new HashSet<>(getAllDates());
            case "get event" -> new HashSet<>(getAllEvents(null, null));
            case "get status" -> new HashSet<>(getAllStatuses());
            default -> null;
        };
    }


    private Set<Status> getAllStatuses() {
        return EnumSet.allOf(Status.class);
    }

    private Set<Date> getAllDates() {
        return logEntities.stream()
                .map(LogEntity::getDate)
                .collect(Collectors.toSet());
    }

    private class LogEntity {
        private final String IP;
        private final String user;
        private final Date date;
        private final Event event;
        private final int eventAdditionalParameter;
        private final Status status;

        public LogEntity(String IP, String user, Date date, Event event, int eventAdditionalParameter, Status status) {
            this.IP = IP;
            this.user = user;
            this.date = date;
            this.event = event;
            this.eventAdditionalParameter = eventAdditionalParameter;
            this.status = status;
        }

        public String getIP() {
            return IP;
        }

        public String getUser() {
            return user;
        }

        public Date getDate() {
            return date;
        }

        public Event getEvent() {
            return event;
        }

        public int getEventAdditionalParameter() {
            return eventAdditionalParameter;
        }

        public Status getStatus() {
            return status;
        }
    }
}