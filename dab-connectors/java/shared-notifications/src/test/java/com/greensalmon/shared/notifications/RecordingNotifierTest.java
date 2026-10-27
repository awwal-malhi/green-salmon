package com.greensalmon.shared.notifications;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RecordingNotifierTest {

    @Test
    void recordsInMemoryAndAsOneFilePerNotification(@TempDir Path dir) throws Exception {
        RecordingNotifier notifier = new RecordingNotifier(dir.resolve("notifications"));
        notifier.send(new Notification(Instant.parse("2026-01-01T00:00:00Z"), Notification.Severity.WARNING,
                "bad rows", "2 rows quarantined", Collections.<String, Object>singletonMap("run_id", "r1")));
        notifier.send(new Notification(Instant.parse("2026-01-01T00:00:01Z"), Notification.Severity.ERROR,
                "failed", "boom", null));

        assertEquals(2, notifier.recorded().size());
        List<Path> files;
        try (Stream<Path> s = Files.list(dir.resolve("notifications"))) {
            files = s.sorted().collect(Collectors.toList());
        }
        assertEquals(2, files.size());
        String first = new String(Files.readAllBytes(files.get(0)), StandardCharsets.UTF_8);
        assertTrue(first.contains("\"severity\":\"WARNING\""), first);
        assertTrue(first.contains("\"run_id\":\"r1\""), first);
    }

    @Test
    void inMemoryOnlyWhenNoDirectory() {
        RecordingNotifier notifier = new RecordingNotifier();
        notifier.send(new Notification(Instant.now(), Notification.Severity.INFO, "ok", "done", null));
        assertEquals("ok", notifier.recorded().get(0).subject());
    }
}
