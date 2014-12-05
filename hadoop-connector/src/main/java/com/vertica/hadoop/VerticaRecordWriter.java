/*
Copyright (c) 2005 - 2012 Vertica, an HP company -*- Java -*-
Copyright 2013, Twitter, Inc.


Licensed under the Apache License, Version 2.0 (the "License");

you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,

WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.vertica.hadoop;

import com.vertica.jdbc.VerticaConnection;
import com.vertica.jdbc.VerticaCopyStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.*;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Executors;

public class VerticaRecordWriter extends RecordWriter<Text, VerticaRecord> {
    private static final Log LOG = LogFactory.getLog("com.vertica.hadoop");

    Connection connection = null;
    PreparedStatement statement = null;
    long numRecords = 0;
    private Thread workerThread;
    private BufferedWriter outputStreamWriter;
    private volatile boolean errorHappened;

    public VerticaRecordWriter(Connection conn, VerticaConfiguration configuration)
            throws Exception {
        final String copyStatement = buildCopyStatementSqlString(configuration.getOutputTableName(),
                configuration.isDirect(), configuration.getMaxRejects());
        this.connection = conn;
        PipedOutputStream pipedOutputStream = new PipedOutputStream();
        final PipedInputStream pipedInputStream = new PipedInputStream(pipedOutputStream);
        outputStreamWriter = new BufferedWriter(new OutputStreamWriter(pipedOutputStream, Charset.forName("UTF-8")));
        initWorker(copyStatement, pipedInputStream);
    }

    private void initWorker(final String copyStatement, final PipedInputStream pipedInputStream) {
        workerThread = Executors.defaultThreadFactory().newThread(new Runnable() {
            @Override
            public void run() {
                try {
                    VerticaCopyStream stream = new VerticaCopyStream((VerticaConnection) connection, copyStatement);
                    stream.start();
                    stream.addStream(pipedInputStream);
                    stream.execute();
                    stream.finish();
                    LOG.info("ROWS LOADED: " + stream.getRowCount());
                    LOG.info("ROWS REJECTED: " + stream.getRejects().size());
                    connection.commit();
                } catch (SQLException e) {
                    errorHappened = true;
                    if (e.getCause() instanceof InterruptedIOException) {
                        LOG.warn("SQL statement interrupted by halt of transformation");
                    } else {
                        LOG.error("SQL Error during statement execution.", e);
                    }
                }
            }
        });

        workerThread.start();

    }

    @Override
    public void close(TaskAttemptContext context) throws IOException {
        outputStreamWriter.close();
        if (workerThread != null && workerThread.isAlive()) {
            try {
                workerThread.join(30000);
                if (errorHappened)
                    throw new RuntimeException("Job failed dute to the errors while processing copy statement");
                if (workerThread.isAlive()) {
                    workerThread.interrupt();
                }
            } catch (InterruptedException e) {
            }
        }
    }

    @Override
    public void write(Text table, VerticaRecord record) throws IOException {
        try {
            record.write(outputStreamWriter);
            numRecords++;
            if (numRecords % 100000 == 0) {
                LOG.info("Rows loaded: " + numRecords);
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private String buildCopyStatementSqlString(String tableName, boolean isDirect, int maxRejects) {

        StringBuilder sb = new StringBuilder(150);
        sb.append("COPY ");
        sb.append(tableName);
        sb.append(" FROM STDIN NO ESCAPE DELIMITER E'\\t' ");
        if (isDirect) {
            sb.append("DIRECT ");
        }

        if (0 != maxRejects) {
            sb.append("REJECTMAX ").append(maxRejects);
        }

        sb.append(" NO COMMIT ");

        // XXX: I believe the right thing to do here is always use NO COMMIT since we want Kettle's configuration to drive.
        // NO COMMIT does not seem to work even when the transformation setting 'make the transformation database transactional' is on
        // sb.append("NO COMMIT"););
        return sb.toString();
    }
}
