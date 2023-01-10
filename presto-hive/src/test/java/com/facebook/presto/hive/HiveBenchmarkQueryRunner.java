/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.hive;

import com.facebook.presto.Session;
import com.facebook.presto.benchmark.BenchmarkSuite;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.security.PrincipalType;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.hive.HiveQueryRunner.METASTORE_CONTEXT;
import static com.facebook.presto.hive.TestHiveUtil.createTestingFileHiveMetastore;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tests.AbstractTestQueries.TEST_CATALOG_PROPERTIES;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.util.Objects.requireNonNull;

public final class HiveBenchmarkQueryRunner
{
    private static final String TPCH_SCHEMA = "tpch";

    private HiveBenchmarkQueryRunner()
    {
    }

    public static void main(String[] args)
            throws IOException
    {
        String outputDirectory = requireNonNull(System.getProperty("outputDirectory"), "Must specify -DoutputDirectory=...");
        File tempDir = Files.createTempDir();
        try (LocalQueryRunner localQueryRunner = createLocalQueryRunner(tempDir)) {
            new BenchmarkSuite(localQueryRunner, outputDirectory).runAllBenchmarks();
        }
        finally {
            deleteRecursively(tempDir.toPath(), ALLOW_INSECURE);
        }
    }

    public static LocalQueryRunner createLocalQueryRunner(File tempDir)
    {
        return createLocalQueryRunner(tempDir, ImmutableMap.of(), ImmutableMap.of());
    }

    public static LocalQueryRunner createLocalQueryRunner(File tempDir, Map<String, String> systemProperties, Map<String, String> hiveProperties)
    {
        SessionPropertyManager sessionPropertyManager = new SessionPropertyManager();
        sessionPropertyManager.addConnectorSessionProperties(new ConnectorId("hive"), TEST_CATALOG_PROPERTIES);
        Session.SessionBuilder sessionBuilder = testSessionBuilder(sessionPropertyManager)
                .setCatalog("hive")
                .setSchema(TPCH_SCHEMA);

        systemProperties.forEach(sessionBuilder::setSystemProperty);
        Session session = sessionBuilder.build();
        LocalQueryRunner localQueryRunner = new LocalQueryRunner(session);

        // add tpch
        localQueryRunner.createCatalog("tpch", new TpchConnectorFactory(1), hiveProperties);

        // add hive
        File hiveDir = new File(tempDir, "hive_data");
        ExtendedHiveMetastore metastore = createTestingFileHiveMetastore(hiveDir);
        HiveConnectorFactory hiveConnectorFactory = new HiveConnectorFactory(
                "hive",
                HiveBenchmarkQueryRunner.class.getClassLoader(),
                Optional.of(metastore));

        Map<String, String> hiveCatalogConfig = ImmutableMap.<String, String>builder()
                .put("hive.max-split-size", "10GB")
                .build();

        localQueryRunner.createCatalog("hive", hiveConnectorFactory, hiveCatalogConfig);

        if (!metastore.getDatabase(METASTORE_CONTEXT, TPCH_SCHEMA).isPresent()) {
            metastore.createDatabase(METASTORE_CONTEXT, Database.builder()
                    .setDatabaseName(TPCH_SCHEMA)
                    .setOwnerName("public")
                    .setOwnerType(PrincipalType.ROLE)
                    .build());

            localQueryRunner.execute("CREATE TABLE orders AS SELECT * FROM tpch.sf1.orders");
            localQueryRunner.execute("CREATE TABLE customer AS SELECT * FROM tpch.sf1.customer");
            localQueryRunner.execute("CREATE TABLE nation AS SELECT * FROM tpch.sf1.nation");
            localQueryRunner.execute("CREATE TABLE region AS SELECT * FROM tpch.sf1.region");
            localQueryRunner.execute("CREATE TABLE lineitem AS SELECT * FROM tpch.sf1.lineitem");
        }

        return localQueryRunner;
    }
}
