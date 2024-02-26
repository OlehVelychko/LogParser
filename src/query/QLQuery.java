package query;

import java.util.Set;

public interface QLQuery {
    Set<?> execute(String query);
}
