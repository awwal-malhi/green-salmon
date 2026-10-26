package com.greensalmon.connectors.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Read-only view of the {@code source} section of a run's config. */
public final class PluginConfig {

    private final Map<String, Object> values;

    public PluginConfig(Map<String, Object> values) {
        this.values = values == null
                ? Collections.<String, Object>emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<String, Object>(values));
    }

    public Map<String, Object> asMap() {
        return values;
    }

    public boolean has(String key) {
        return values.get(key) != null;
    }

    public String getString(String key) {
        Object value = values.get(key);
        if (value == null) {
            throw new IllegalArgumentException("Missing required plugin option '" + key + "'");
        }
        return value.toString();
    }

    public String getString(String key, String defaultValue) {
        Object value = values.get(key);
        return value == null ? defaultValue : value.toString();
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        Object value = values.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        return Boolean.parseBoolean(value.toString());
    }

    /** Returns a list option; an absent key gives an empty list. */
    public List<String> getStringList(String key) {
        Object value = values.get(key);
        List<String> result = new ArrayList<String>();
        if (value == null) {
            return result;
        }
        if (!(value instanceof List)) {
            throw new IllegalArgumentException("Plugin option '" + key + "' must be a list");
        }
        for (Object item : (List<?>) value) {
            result.add(String.valueOf(item));
        }
        return result;
    }

    /** Returns a map option with string values; an absent key gives an empty map. */
    public Map<String, String> getStringMap(String key) {
        Object value = values.get(key);
        Map<String, String> result = new LinkedHashMap<String, String>();
        if (value == null) {
            return result;
        }
        if (!(value instanceof Map)) {
            throw new IllegalArgumentException("Plugin option '" + key + "' must be an object");
        }
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
            result.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
        }
        return result;
    }
}
