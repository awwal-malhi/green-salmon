package com.greensalmon.shared.notifications;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Stub notifier: records each notification instead of sending it. Notifications are always kept
 * in memory and, when a directory is given, also written there as one JSON file per notification
 * (one file each because Unity Catalog volumes do not support appending to a file).
 */
public final class RecordingNotifier implements Notifier {

    private final Path directory;
    private final List<Notification> recorded = Collections.synchronizedList(new ArrayList<Notification>());

    /** Records in memory only. */
    public RecordingNotifier() {
        this(null);
    }

    public RecordingNotifier(Path directory) {
        this.directory = directory;
    }

    @Override
    public void send(Notification notification) {
        recorded.add(notification);
        if (directory == null) {
            return;
        }
        String fileName = notification.createdAt().toEpochMilli() + "-"
                + notification.severity().name().toLowerCase() + "-" + UUID.randomUUID() + ".json";
        try {
            Files.createDirectories(directory);
            Files.write(directory.resolve(fileName), notification.toJson().getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new IllegalStateException("Could not record notification in " + directory, e);
        }
    }

    public List<Notification> recorded() {
        synchronized (recorded) {
            return new ArrayList<Notification>(recorded);
        }
    }
}
