package com.greensalmon.connectors.core;

import com.greensalmon.connectors.api.SourcePlugin;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

/**
 * Loads a Java plugin from its JAR in a class loader of its own, so two versions of one plugin
 * (same class names) can be used side by side in one JVM. The plugin API classes come from the
 * parent loader, so the plugin JAR must not bundle them, and plugin JARs must not be on the
 * cluster classpath (they would shadow the isolated copy).
 */
public final class PluginLoader {

    private PluginLoader() {
    }

    public static LoadedPlugin load(Path jar, String expectedName, String expectedVersion) {
        if (!Files.isRegularFile(jar)) {
            throw new IllegalArgumentException("Plugin JAR not found: " + jar);
        }
        URLClassLoader loader;
        try {
            loader = new URLClassLoader(new URL[] {jar.toUri().toURL()}, SourcePlugin.class.getClassLoader());
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Bad plugin JAR path " + jar, e);
        }
        try {
            List<SourcePlugin> own = new ArrayList<SourcePlugin>();
            List<String> shadowed = new ArrayList<String>();
            for (SourcePlugin candidate : ServiceLoader.load(SourcePlugin.class, loader)) {
                if (candidate.getClass().getClassLoader() == loader) {
                    own.add(candidate);
                } else {
                    shadowed.add(candidate.getClass().getName());
                }
            }
            if (own.size() != 1) {
                throw new IllegalArgumentException("Expected exactly one SourcePlugin in " + jar + ", found "
                        + own.size() + (shadowed.isEmpty() ? "" : "; classes also on the parent classpath "
                        + "(remove them from it): " + shadowed));
            }
            SourcePlugin plugin = own.get(0);
            if (!plugin.name().equals(expectedName) || !plugin.version().equals(expectedVersion)) {
                throw new IllegalArgumentException("JAR " + jar + " contains plugin " + plugin.name() + " "
                        + plugin.version() + ", expected " + expectedName + " " + expectedVersion);
            }
            return new LoadedPlugin(plugin, loader);
        } catch (RuntimeException | Error e) {
            closeQuietly(loader);
            throw e;
        }
    }

    private static void closeQuietly(URLClassLoader loader) {
        try {
            loader.close();
        } catch (IOException ignored) {
            // nothing useful to do
        }
    }

    /** A plugin together with its class loader; close it when the run is over. */
    public static final class LoadedPlugin implements AutoCloseable {
        private final SourcePlugin plugin;
        private final URLClassLoader loader;

        private LoadedPlugin(SourcePlugin plugin, URLClassLoader loader) {
            this.plugin = plugin;
            this.loader = loader;
        }

        public SourcePlugin plugin() {
            return plugin;
        }

        @Override
        public void close() {
            closeQuietly(loader);
        }
    }
}
