package query;

import data.Event;
import data.Status;


import java.util.Date;
import java.util.Set;

public interface IPQuery {
    int getNumberOfUniqueIPs(Date after, Date before);

    Set<String> getUniqueIPs(Date after, Date before);

    Set<String> getIPsForUser(String user, Date after, Date before);

    Set<String> getIPsForStatus(Status status, Date after, Date before);

    Set<String> getIPsForEvent(Event event, Date after, Date before);

}
