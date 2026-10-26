package com.greensalmon.connectors.core;

import com.greensalmon.connectors.api.PluginConfig;
import com.greensalmon.connectors.api.SourcePlugin;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Builds throwaway plugin JARs with the same class name and different versions. */
class PluginLoaderTest {

    private static final String CLASS_NAME = "com.example.plugins.DemoPlugin";

    @Test
    void twoVersionsOfOnePluginLoadSideBySide(@TempDir Path dir) throws Exception {
        Path v1 = buildPluginJar(dir.resolve("v1"), "demo", "1.0.0", true);
        Path v2 = buildPluginJar(dir.resolve("v2"), "demo", "2.0.0", true);

        try (PluginLoader.LoadedPlugin one = PluginLoader.load(v1, "demo", "1.0.0");
             PluginLoader.LoadedPlugin two = PluginLoader.load(v2, "demo", "2.0.0")) {
            assertEquals(CLASS_NAME, one.plugin().getClass().getName());
            assertEquals(CLASS_NAME, two.plugin().getClass().getName());
            assertNotSame(one.plugin().getClass(), two.plugin().getClass());

            SparkSession spark = SparkTestSupport.spark();
            PluginConfig config = new PluginConfig(null);
            assertEquals("1.0.0", one.plugin().read(spark, config).first().getString(0));
            assertEquals("2.0.0", two.plugin().read(spark, config).first().getString(0));
        }
    }

    @Test
    void versionInJarMustMatchRequestedVersion(@TempDir Path dir) throws Exception {
        Path v1 = buildPluginJar(dir, "demo", "1.0.0", true);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> PluginLoader.load(v1, "demo", "2.0.0"));
        assertTrue(e.getMessage().contains("expected demo 2.0.0"), e.getMessage());
    }

    @Test
    void jarWithoutServiceEntryIsRejected(@TempDir Path dir) throws Exception {
        Path jar = buildPluginJar(dir, "demo", "1.0.0", false);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> PluginLoader.load(jar, "demo", "1.0.0"));
        assertTrue(e.getMessage().contains("found 0"), e.getMessage());
    }

    @Test
    void missingJarIsRejected(@TempDir Path dir) {
        assertThrows(IllegalArgumentException.class, () -> PluginLoader.load(dir.resolve("nope.jar"), "x", "1"));
    }

    static Path buildPluginJar(Path dir, String name, String version, boolean withServiceFile) throws Exception {
        Path src = dir.resolve("src/com/example/plugins/DemoPlugin.java");
        Files.createDirectories(src.getParent());
        String code = "package com.example.plugins;\n"
                + "import org.apache.spark.sql.*;\n"
                + "import com.greensalmon.connectors.api.*;\n"
                + "public class DemoPlugin implements SourcePlugin {\n"
                + "  public String name() { return \"" + name + "\"; }\n"
                + "  public String version() { return \"" + version + "\"; }\n"
                + "  public Dataset<Row> read(SparkSession spark, PluginConfig config) {\n"
                + "    return spark.sql(\"SELECT '" + version + "' AS version\");\n"
                + "  }\n"
                + "}\n";
        Files.write(src, code.getBytes(StandardCharsets.UTF_8));
        Path classes = dir.resolve("classes");
        Files.createDirectories(classes);

        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        java.io.ByteArrayOutputStream errors = new java.io.ByteArrayOutputStream();
        int rc = compiler.run(null, null, errors, "-classpath", compileClasspath(), "-d", classes.toString(),
                src.toString());
        assertEquals(0, rc, "test plugin did not compile: " + errors.toString("UTF-8"));

        Path jar = dir.resolve("demo-plugin-" + version + ".jar");
        try (JarOutputStream out = new JarOutputStream(Files.newOutputStream(jar))) {
            addEntry(out, "com/example/plugins/DemoPlugin.class",
                    Files.readAllBytes(classes.resolve("com/example/plugins/DemoPlugin.class")));
            if (withServiceFile) {
                addEntry(out, "META-INF/services/" + SourcePlugin.class.getName(),
                        (CLASS_NAME + "\n").getBytes(StandardCharsets.UTF_8));
            }
        }
        return jar;
    }

    private static void addEntry(JarOutputStream out, String name, byte[] bytes) throws Exception {
        out.putNextEntry(new JarEntry(name));
        OutputStream os = out;
        os.write(bytes);
        out.closeEntry();
    }

    private static String compileClasspath() {
        return System.getProperty("java.class.path");
    }
}
