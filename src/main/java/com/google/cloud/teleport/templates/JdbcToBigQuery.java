/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.teleport.templates;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.io.DynamicJdbcIO;
import com.google.cloud.teleport.templates.common.JdbcConverters;
import com.google.cloud.teleport.util.KMSEncryptedNestedValueProvider;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

/**
 * A template that copies data from a relational database using JDBC to an existing BigQuery table.
 */
public class JdbcToBigQuery {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcToBigQuery.class);

  private static class ModifyDateTime extends DoFn<TableRow, TableRow> {

    ValueProvider<String> timestampFields;
    ValueProvider<String> dateFields;
    ValueProvider<String> datetimeFields;
    SimpleDateFormat df; // date format
    SimpleDateFormat dtf; // datetime format

    public ModifyDateTime(ValueProvider<String> tsFields, ValueProvider<String> dFields, ValueProvider<String> dtFields) {
      this.timestampFields = tsFields;
      this.dateFields = dFields;
      this.datetimeFields = dtFields;

      df = new SimpleDateFormat("yyyy-MM-dd");
      dtf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
    }

    @ProcessElement
    public void processElement(@Element TableRow row, OutputReceiver<TableRow> out) {
      Set<String> timestampFields = new HashSet<>(Arrays.asList(this.timestampFields.toString().split(";")));
      Set<String> dateFields = new HashSet<>(Arrays.asList(this.dateFields.toString().split(";")));
      Set<String> datetimeFields = new HashSet<>(Arrays.asList(this.dateFields.toString().split(";")));

      /*
       * Handle TIMESTAMP, DATE, DATETIME (BigQuery) fields which are causing the pipelines to fail
       * TIMESTAMP -> EPOCH(TIMESTAMP) / 1000
       * DATETIME -> EPOCH(DATE) -> 'yyyy-MM-dd hh:mm:ss; format
       * DATE -> EPOCH(DATE) -> 'yyyy-MM-dd' format
       */
      for (String field : row.keySet()) {
        if (row.get(field) == null) continue;

        if (timestampFields.contains(field)) row.set(field, (Long) row.get(field) / 1000);
        if (dateFields.contains(field)) row.set(field, df.format(new Date((Long) row.get(field))));
        if (datetimeFields.contains(field)) row.set(field, dtf.format(new Date((Long) row.get(field))));
      }
      out.output(row);
    }
  }

  private static ValueProvider<String> maybeDecrypt(
      ValueProvider<String> unencryptedValue, ValueProvider<String> kmsKey) {
    return new KMSEncryptedNestedValueProvider(unencryptedValue, kmsKey);
  }

  /**
   * Main entry point for executing the pipeline. This will run the pipeline asynchronously. If
   * blocking execution is required, use the {@link
   * JdbcToBigQuery#run(JdbcConverters.JdbcToBigQueryOptions)} method to start the pipeline and
   * invoke {@code result.waitUntilFinish()} on the {@link PipelineResult}
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {

    // Parse the user options passed from the command-line
    JdbcConverters.JdbcToBigQueryOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(JdbcConverters.JdbcToBigQueryOptions.class);

    run(options);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  private static PipelineResult run(JdbcConverters.JdbcToBigQueryOptions options) {
    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    /*
     * Steps: 1) Read records via JDBC and convert to TableRow via RowMapper
     *        2) Append TableRow to BigQuery via BigQueryIO
     */
    pipeline
        /*
         * Step 1: Read records via JDBC and convert to TableRow
         *         via {@link org.apache.beam.sdk.io.jdbc.JdbcIO.RowMapper}
         */
        .apply(
            "Read from JdbcIO",
            DynamicJdbcIO.<TableRow>read()
                .withDataSourceConfiguration(
                    DynamicJdbcIO.DynamicDataSourceConfiguration.create(
                            options.getDriverClassName(),
                            maybeDecrypt(options.getConnectionURL(), options.getKMSEncryptionKey()))
                        .withUsername(
                            maybeDecrypt(options.getUsername(), options.getKMSEncryptionKey()))
                        .withPassword(
                            maybeDecrypt(options.getPassword(), options.getKMSEncryptionKey()))
                        .withDriverJars(options.getDriverJars())
                        .withConnectionProperties(options.getConnectionProperties()))
                .withQuery(options.getQuery())
                .withCoder(TableRowJsonCoder.of())
                .withRowMapper(JdbcConverters.getResultSetToTableRow()))
        /*
         * Intermediate ParDo operation to fix UNIX dateTime read from DynamicJDBCIO
         * from milliseconds to seconds for BigQueryIO to consume
         */
        .apply(
                "Convert DateTime to correct format",
                ParDo.of(new ModifyDateTime(options.getTimestamp(), options.getDate(), options.getDatetime())))
        /*
         * Step 2: Append TableRow to an existing BigQuery table
         */
        .apply(
            "Write to BigQuery",
            BigQueryIO.writeTableRows()
                .withoutValidation()
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCustomGcsTempLocation(options.getBigQueryLoadingTemporaryDirectory())
                .to(options.getOutputTable()));

    // Execute the pipeline and return the result.
    return pipeline.run();
  }
}
