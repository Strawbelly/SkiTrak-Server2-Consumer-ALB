package server;

import com.google.gson.Gson;
import com.google.gson.JsonParseException;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalInt;

/**
 * Loads and provides an immutable lookup of lift IDs to vertical distances
 * (in feet) from the lift_vertical.json resource.
 */
public class LiftVerticalMap {
    public static final String DEFAULT_RESOURCE = "lift_vertical.json";

    private final Map<Integer, Integer> verticals;

    public LiftVerticalMap(Map<Integer, Integer> verticals) {
        this.verticals = Collections.unmodifiableMap(new HashMap<>(verticals));
    }

    public static LiftVerticalMap fromClasspath() {
        return fromClasspath(DEFAULT_RESOURCE);
    }

    public static LiftVerticalMap fromClasspath(String resource) {
        InputStream in = LiftVerticalMap.class.getClassLoader().getResourceAsStream(resource);
        if (in == null) {
            throw new IllegalStateException("resource not found: " + resource);
        }
        try (Reader reader = new InputStreamReader(in, StandardCharsets.UTF_8)) {
            return fromJson(reader);
        } catch (IOException e) {
            throw new IllegalStateException("failed to read " + resource, e);
        }
    }

    static LiftVerticalMap fromJson(Reader reader) {
        LiftTable table;
        try {
            table = new Gson().fromJson(reader, LiftTable.class);
        } catch (JsonParseException e) {
            throw new IllegalStateException("invalid lift vertical JSON", e);
        }
        if (table == null || table.lifts == null || table.lifts.isEmpty()) {
            throw new IllegalStateException("lift vertical JSON has no \"lifts\" entries");
        }
        Map<Integer, Integer> verticals = new HashMap<>();
        for (Map.Entry<String, Integer> e : table.lifts.entrySet()) {
            try {
                verticals.put(Integer.parseInt(e.getKey()), e.getValue());
            } catch (NumberFormatException ex) {
                throw new IllegalStateException("invalid liftID key: " + e.getKey(), ex);
            }
        }
        return new LiftVerticalMap(verticals);
    }

    public OptionalInt getVertical(int liftID) {
        Integer vertical = verticals.get(liftID);
        return vertical == null ? OptionalInt.empty() : OptionalInt.of(vertical);
    }

    public int size() {
        return verticals.size();
    }

    // Shape of lift_vertical.json
    private static class LiftTable {
        Map<String, Integer> lifts;
    }
}
