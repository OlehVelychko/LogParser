package task;

import query.IPQuery;
import query.UserQuery;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class LogParser implements IPQuery, UserQuery {
    private Path logDir;
    private List<LogEntity> logEntities = new ArrayList<>();
    private DateFormat simpleDateFormat = new SimpleDateFormat("d.M.yyyy H:m:s");

    public LogParser(Path logDir) {
        this.logDir = logDir;
        readLogs();
    }

    private void readLogs() {
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(logDir)) {
            for (Path file : directoryStream) {
                if (file.toString().toLowerCase().endsWith(".log")) {
                    try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
                        String line = null;
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
        Event event = null;
        if (lineToParse.contains("SOLVE_TASK")) {
            event = Event.SOLVE_TASK;
        } else if (lineToParse.contains("DONE_TASK")) {
            event = Event.DONE_TASK;
        } else {
            switch (lineToParse) {
                case "LOGIN": {
                    event = Event.LOGIN;
                    break;
                }
                case "DOWNLOAD_PLUGIN": {
                    event = Event.DOWNLOAD_PLUGIN;
                    break;
                }
                case "WRITE_MESSAGE": {
                    event = Event.WRITE_MESSAGE;
                    break;
                }
            }
        }
        return event;
    }

    private int readAdditionalParameter(String lineToParse) {
        if (lineToParse.contains("SOLVE_TASK")) {
            lineToParse = lineToParse.replace("SOLVE_TASK", "").replaceAll(" ", "");
            return Integer.parseInt(lineToParse);
        } else {
            lineToParse = lineToParse.replace("DONE_TASK", "").replaceAll(" ", "");
            return Integer.parseInt(lineToParse);
        }
    }

    private Status readStatus(String lineToParse) {
        Status status = null;
        switch (lineToParse) {
            case "OK": {
                status = Status.OK;
                break;
            }
            case "FAILED": {
                status = Status.FAILED;
                break;
            }
            case "ERROR": {
                status = Status.ERROR;
                break;
            }
        }
        return status;
    }

    private boolean dateBetweenDates(Date current, Date after, Date before) {
        if (after == null) {
            after = new Date(0);
        }
        if (before == null) {
            before = new Date(Long.MAX_VALUE);
        }
        return current.after(after) && current.before(before);
    }

    @Override
    public int getNumberOfUniqueIPs(Date after, Date before) {
        return getUniqueIPs(after, before).size();
    }

    @Override
    public Set<String> getUniqueIPs(Date after, Date before) {
        return logEntities.stream()
                .filter(logEntity -> (after == null || logEntity.getDate().after(after) || logEntity.getDate().equals(after)) &&
                        (before == null || logEntity.getDate().before(before) || logEntity.getDate().equals(before)))
                .map(LogEntity::getIP)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<String> getIPsForUser(String user, Date after, Date before) {
        return logEntities.stream()
                .filter(logEntity -> logEntity.getUser().equals(user) &&
                        (after == null || logEntity.getDate().after(after) || logEntity.getDate().equals(after)) &&
                        (before == null || logEntity.getDate().before(before) || logEntity.getDate().equals(before)))
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

    private class LogEntity {
        private String IP;
        private String user;
        private Date date;
        private Event event;
        private int eventAdditionalParameter;
        private Status status;

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