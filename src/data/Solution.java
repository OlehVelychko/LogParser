package data;

import java.io.IOException;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Solution {
    public static void main(String[] args) throws IOException, ParseException {
        /*SimpleDateFormat dateFormat = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
        Date afterDate = dateFormat.parse("21.10.2021 19:45:25");*/

        LogParser logParser = new LogParser(Paths.get("/Users/narsil/Documents/GitHub/LogParser/src/logs"));

        System.out.println("--------------------");
//        System.out.println(logParser.getIPsForStatus(Status.OK, afterDate, null));

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
        Date afterDate = simpleDateFormat.parse("21.10.2013 19:45:25");
        Date beforeDate = simpleDateFormat.parse("21.10.2022 19:45:25");

        System.out.println(logParser.getNumberOfUsers(afterDate, beforeDate));
    }
}
