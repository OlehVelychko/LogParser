import query.IPQuery;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class LogParser implements IPQuery {
    private Path logDir;
    private List<LogEntity> logEntities = new ArrayList<>();
    private DateFormat simpleDateFormat = new SimpleDateFormat("d.M.yyyy H:m:s");

    public LogParser(Path logDir) {
        this.logDir = logDir;
        readLogs();
    }

    private void readLogs() {
        try {
            Files.walk(logDir)
                    .filter(Files::isRegularFile)
                    .forEach(file -> {
                        try {
                            Files.lines(file)
                                    .forEach(this::parseLogLine);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void parseLogLine(String line) {
        // Define a regular expression pattern to match the log line format
        String regex = "(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(\\S+)";

        // Create a Pattern object
        Pattern pattern = Pattern.compile(regex);

        // Match the pattern against the input line
        Matcher matcher = pattern.matcher(line);

        // If the line matches the pattern, create a LogEntity object and add it to logEntities list
        if (matcher.matches()) {
            String IP = matcher.group(1);
            String user = matcher.group(2);
            Date date;
            try {
                date = simpleDateFormat.parse(matcher.group(3) + " " + matcher.group(4));
            } catch (ParseException e) {
                e.printStackTrace();
                return; // Skip parsing this line if date parsing fails
            }
            Event event = Event.valueOf(matcher.group(5));
            int eventAdditionalParameter = 0;
            if (matcher.group(6).matches("\\d+")) {
                eventAdditionalParameter = Integer.parseInt(matcher.group(6));
            }
            Status status = Status.valueOf(matcher.group(7));

            LogEntity logEntity = new LogEntity(IP, user, date, event, eventAdditionalParameter, status);
            logEntities.add(logEntity);
        }
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
                        (before ==null || logEntity.getDate().before(before) || logEntity.getDate().equals(before)))
                .map(LogEntity::getIP)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<String> getIPsForStatus(Status status, Date after, Date before) {
        return logEntities.stream()
                .filter(logEntity -> logEntity.getStatus().equals(status) &&
                        (after == null || logEntity.getDate().after(after) || logEntity.getDate().equals(after)) &&
                        (before ==null || logEntity.getDate().before(before) || logEntity.getDate().equals(before)))
                .map(LogEntity::getIP)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<String> getIPsForEvent(Event event, Date after, Date before) {
        return logEntities.stream()
                .filter(logEntity -> logEntity.getEvent().equals(event) &&
                        (after == null || logEntity.getDate().after(after) || logEntity.getDate().equals(after)) &&
                        (before ==null || logEntity.getDate().before(before) || logEntity.getDate().equals(before)))
                .map(LogEntity::getIP)
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
