import java.nio.file.Paths;
import java.util.Date;

public class Solution {
    public static void main(String[] args) {
        LogParser logParser = new LogParser(Paths.get("example.log"));
        System.out.println(logParser.getNumberOfUniqueIPs(null, new Date()));
    }
}
