/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.service.log;

import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.plugin.task.api.utils.LogUtils;
import org.apache.dolphinscheduler.remote.command.Command;
import org.apache.dolphinscheduler.remote.command.CommandType;
import org.apache.dolphinscheduler.remote.command.log.GetAppIdRequestCommand;
import org.apache.dolphinscheduler.remote.command.log.GetAppIdResponseCommand;
import org.apache.dolphinscheduler.remote.command.log.GetLogBytesRequestCommand;
import org.apache.dolphinscheduler.remote.command.log.GetLogBytesResponseCommand;
import org.apache.dolphinscheduler.remote.command.log.RemoveTaskLogRequestCommand;
import org.apache.dolphinscheduler.remote.command.log.RemoveTaskLogResponseCommand;
import org.apache.dolphinscheduler.remote.command.log.RollViewLogRequestCommand;
import org.apache.dolphinscheduler.remote.command.log.RollViewLogResponseCommand;
import org.apache.dolphinscheduler.remote.command.log.ViewLogRequestCommand;
import org.apache.dolphinscheduler.remote.command.log.ViewLogResponseCommand;
import org.apache.dolphinscheduler.remote.processor.NettyRequestProcessor;
import org.apache.dolphinscheduler.remote.utils.Constants;
import org.apache.dolphinscheduler.remote.utils.NamedThreadFactory;
import org.apache.dolphinscheduler.service.utils.LoggerUtils;

import org.apache.commons.lang3.StringUtils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import io.netty.channel.Channel;

/**
 * logger request process logic
 */
@Component
public class LoggerRequestProcessor implements NettyRequestProcessor {

    private final Logger logger = LoggerFactory.getLogger(LoggerRequestProcessor.class);

    private final ExecutorService executor;

    public LoggerRequestProcessor() {
        this.executor = Executors.newFixedThreadPool(Constants.CPUS * 2 + 1,
                new NamedThreadFactory("Log-Request-Process-Thread"));
    }

    @Override
    public void process(Channel channel, Command command) {
        logger.info("received command : {}", command);

        // request task log command type
        final CommandType commandType = command.getType();
        switch (commandType) {
            case GET_LOG_BYTES_REQUEST:
                GetLogBytesRequestCommand getLogRequest = JSONUtils.parseObject(
                        command.getBody(), GetLogBytesRequestCommand.class);
                String path = getLogRequest.getPath();
                if (!checkPathSecurity(path)) {
                    throw new IllegalArgumentException("Illegal path: " + path);
                }
                byte[] bytes = getFileContentBytes(path);
                GetLogBytesResponseCommand getLogResponse = new GetLogBytesResponseCommand(bytes);
                channel.writeAndFlush(getLogResponse.convert2Command(command.getOpaque()));
                break;
            case VIEW_WHOLE_LOG_REQUEST:
                ViewLogRequestCommand viewLogRequest = JSONUtils.parseObject(
                        command.getBody(), ViewLogRequestCommand.class);
                String viewLogPath = viewLogRequest.getPath();
                if (!checkPathSecurity(viewLogPath)) {
                    throw new IllegalArgumentException("Illegal path: " + viewLogPath);
                }
                String msg = LoggerUtils.readWholeFileContent(viewLogPath);
                ViewLogResponseCommand viewLogResponse = new ViewLogResponseCommand(msg);
                channel.writeAndFlush(viewLogResponse.convert2Command(command.getOpaque()));
                break;
            case ROLL_VIEW_LOG_REQUEST:
                RollViewLogRequestCommand rollViewLogRequest = JSONUtils.parseObject(
                        command.getBody(), RollViewLogRequestCommand.class);

                String rollViewLogPath = rollViewLogRequest.getPath();
                if (!checkPathSecurity(rollViewLogPath)) {
                    throw new IllegalArgumentException("Illegal path: " + rollViewLogPath);
                }
                List<String> lines;
                if (StringUtils.isEmpty(rollViewLogRequest.getFilter())) {
                    lines = readPartFileContent(rollViewLogPath,
                            rollViewLogRequest.getSkipLineNum(), rollViewLogRequest.getLimit());
                } else {
                    lines = readTailPartFileContent(rollViewLogPath,
                            rollViewLogRequest.getFilter(), rollViewLogRequest.getLimit());
                }
                StringBuilder builder = new StringBuilder();
                final int MaxResponseLogSize = 65535;
                int totalLogByteSize = 0;
                for (String line : lines) {
                    // If a single line of log is exceed max response size, cut off the line
                    final int lineByteSize = line.getBytes(StandardCharsets.UTF_8).length;
                    if (lineByteSize >= MaxResponseLogSize) {
                        builder.append(line, 0, MaxResponseLogSize)
                                .append(" [this line's size ").append(lineByteSize).append(" bytes is exceed ")
                                .append(MaxResponseLogSize).append(" bytes, so only ")
                                .append(MaxResponseLogSize).append(" characters are reserved for performance reasons.]")
                                .append("\r\n");
                    } else {
                        builder.append(line).append("\r\n");
                    }
                    totalLogByteSize += lineByteSize;
                    if (totalLogByteSize >= MaxResponseLogSize) {
                        break;
                    }
                }
                RollViewLogResponseCommand rollViewLogRequestResponse =
                        new RollViewLogResponseCommand(builder.toString());
                channel.writeAndFlush(rollViewLogRequestResponse.convert2Command(command.getOpaque()));
                break;
            case REMOVE_TAK_LOG_REQUEST:
                RemoveTaskLogRequestCommand removeTaskLogRequest = JSONUtils.parseObject(
                        command.getBody(), RemoveTaskLogRequestCommand.class);

                String taskLogPath = removeTaskLogRequest.getPath();
                if (!checkPathSecurity(taskLogPath)) {
                    throw new IllegalArgumentException("Illegal path: " + taskLogPath);
                }
                File taskLogFile = new File(taskLogPath);
                boolean status = true;
                try {
                    if (taskLogFile.exists()) {
                        status = taskLogFile.delete();
                    }
                } catch (Exception e) {
                    status = false;
                }

                RemoveTaskLogResponseCommand removeTaskLogResponse = new RemoveTaskLogResponseCommand(status);
                channel.writeAndFlush(removeTaskLogResponse.convert2Command(command.getOpaque()));
                break;
            case GET_APP_ID_REQUEST:
                GetAppIdRequestCommand getAppIdRequestCommand =
                        JSONUtils.parseObject(command.getBody(), GetAppIdRequestCommand.class);
                String logPath = getAppIdRequestCommand.getLogPath();
                if (!checkPathSecurity(logPath)) {
                    throw new IllegalArgumentException("Illegal path");
                }
                List<String> appIds = LogUtils.getAppIdsFromLogFile(logPath);
                channel.writeAndFlush(
                        new GetAppIdResponseCommand(appIds).convert2Command(command.getOpaque()));
                break;
            default:
                throw new IllegalArgumentException("unknown commandType: " + commandType);
        }
    }

    /**
     * LogServer only can read the logs dir.
     * @param path
     * @return
     */
    private boolean checkPathSecurity(String path) {
        String dsHome = System.getProperty("DOLPHINSCHEDULER_WORKER_HOME");
        if (StringUtils.isBlank(dsHome)) {
            dsHome = System.getProperty("user.dir");
        }
        if (StringUtils.isBlank(path)) {
            logger.warn("path is null");
            return false;
        } else {
            return path.startsWith(dsHome) && !path.contains("../") && path.endsWith(".log");
        }
    }

    public ExecutorService getExecutor() {
        return this.executor;
    }

    /**
     * get files content bytes for download file
     *
     * @param filePath file path
     * @return byte array of file
     */
    private byte[] getFileContentBytes(String filePath) {
        try (
                InputStream in = new FileInputStream(filePath);
                ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            byte[] buf = new byte[1024];
            int len;
            while ((len = in.read(buf)) != -1) {
                bos.write(buf, 0, len);
            }
            return bos.toByteArray();
        } catch (IOException e) {
            logger.error("get file bytes error", e);
        }
        return new byte[0];
    }

    /**
     * read part file content，can skip any line and read some lines
     *
     * @param filePath file path
     * @param skipLine skip line
     * @param limit read lines limit
     * @return part file content
     */
    private List<String> readPartFileContent(String filePath,
                                             int skipLine,
                                             int limit) {
        File file = new File(filePath);
        if (file.exists() && file.isFile()) {
            try (Stream<String> stream = Files.lines(Paths.get(filePath))) {
                return stream.skip(skipLine).limit(limit).collect(Collectors.toList());
            } catch (IOException e) {
                logger.error("read file error", e);
            }
        } else {
            logger.info("file path: {} not exists", filePath);
        }
        return Collections.emptyList();
    }

    /**
     * read part file content，can skip any line and read some lines
     *
     * @param filePath file path
     * @param limit read lines limit
     * @return part file content
     */
    private List<String> readTailPartFileContent(String filePath,
                                                 String filter,
                                                 int limit) {
        File file = new File(filePath);
        if (file.exists() && file.isFile()) {
            try (Stream<String> stream = Files.lines(Paths.get(filePath))) {
                return stream
                        .filter(line -> line.contains(filter))
                        .limit(limit)
                        .collect(Collectors.toList());
            } catch (IOException e) {
                logger.error("read file error", e);
            }
        } else {
            logger.info("file path: {} not exists", filePath);
        }
        return Collections.emptyList();
    }

    /**
     * read  file content，find ERROR message
     * log :
     * [WARN] 2023-11-21 04:42:35.129 +0800 - HikariPool-101 - Connection com.mysql.cj.jdbc.ConnectionImpl@6a9c65b5 marked as broken because of SQLSTATE(08S01), ErrorCode(0)
     * com.mysql.cj.jdbc.exceptions.CommunicationsException: Communications link failure
     *
     * The last packet successfully received from the server was 1,996 milliseconds ago.  The last packet sent successfully to the server was 1,995 milliseconds ago.
     * 	at com.mysql.cj.jdbc.exceptions.SQLError.createCommunicationsException(SQLError.java:172)
     * 	at com.mysql.cj.jdbc.exceptions.SQLExceptionsMapping.translateException(SQLExceptionsMapping.java:64)
     * 	at com.mysql.cj.jdbc.ClientPreparedStatement.executeInternal(ClientPreparedStatement.java:960)
     * 	at com.mysql.cj.jdbc.ClientPreparedStatement.executeUpdateInternal(ClientPreparedStatement.java:1116)
     * 	at com.mysql.cj.jdbc.ClientPreparedStatement.executeUpdateInternal(ClientPreparedStatement.java:1066)
     * 	at com.mysql.cj.jdbc.ClientPreparedStatement.executeLargeUpdate(ClientPreparedStatement.java:1396)
     * 	at com.mysql.cj.jdbc.ClientPreparedStatement.executeUpdate(ClientPreparedStatement.java:1051)
     * 	at com.zaxxer.hikari.pool.ProxyPreparedStatement.executeUpdate(ProxyPreparedStatement.java:61)
     * 	at com.zaxxer.hikari.pool.HikariProxyPreparedStatement.executeUpdate(HikariProxyPreparedStatement.java)
     * 	at org.apache.dolphinscheduler.plugin.task.sql.SqlTask.executeUpdate(SqlTask.java:411)
     * 	at org.apache.dolphinscheduler.plugin.task.sql.SqlTask.executeFuncAndSql(SqlTask.java:245)
     * 	at org.apache.dolphinscheduler.plugin.task.sql.SqlTask.handle(SqlTask.java:184)
     * 	at org.apache.dolphinscheduler.server.worker.runner.DefaultWorkerDelayTaskExecuteRunnable.executeTask(DefaultWorkerDelayTaskExecuteRunnable.java:49)
     * 	at org.apache.dolphinscheduler.server.worker.runner.WorkerTaskExecuteRunnable.run(WorkerTaskExecuteRunnable.java:173)
     * 	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:515)
     * 	at com.google.common.util.concurrent.TrustedListenableFutureTask$TrustedFutureInterruptibleTask.runInterruptibly(TrustedListenableFutureTask.java:131)
     * 	at com.google.common.util.concurrent.InterruptibleTask.run(InterruptibleTask.java:74)
     * 	at com.google.common.util.concurrent.TrustedListenableFutureTask.run(TrustedListenableFutureTask.java:82)
     * 	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
     * 	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
     * 	at java.base/java.lang.Thread.run(Thread.java:829)
     * Caused by: com.mysql.cj.exceptions.CJCommunicationsException: Communications link failure
     *
     * The last packet successfully received from the server was 1,996 milliseconds ago.  The last packet sent successfully to the server was 1,995 milliseconds ago.
     * 	at jdk.internal.reflect.GeneratedConstructorAccessor88.newInstance(Unknown Source)
     * 	at java.base/jdk.internal.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)
     * 	at java.base/java.lang.reflect.Constructor.newInstance(Constructor.java:490)
     * 	at com.mysql.cj.exceptions.ExceptionFactory.createException(ExceptionFactory.java:59)
     * 	at com.mysql.cj.exceptions.ExceptionFactory.createException(ExceptionFactory.java:103)
     * 	at com.mysql.cj.exceptions.ExceptionFactory.createException(ExceptionFactory.java:149)
     * 	at com.mysql.cj.exceptions.ExceptionFactory.createCommunicationsException(ExceptionFactory.java:165)
     * 	at com.mysql.cj.protocol.a.NativeProtocol.readMessage(NativeProtocol.java:563)
     * 	at com.mysql.cj.protocol.a.NativeProtocol.checkErrorMessage(NativeProtocol.java:735)
     * 	at com.mysql.cj.protocol.a.NativeProtocol.sendCommand(NativeProtocol.java:674)
     * 	at com.mysql.cj.protocol.a.NativeProtocol.sendQueryPacket(NativeProtocol.java:966)
     * 	at com.mysql.cj.NativeSession.execSQL(NativeSession.java:1165)
     * 	at com.mysql.cj.jdbc.ClientPreparedStatement.executeInternal(ClientPreparedStatement.java:937)
     * 	... 18 common frames omitted
     * Caused by: java.net.SocketException: Socket closed
     * 	at java.base/java.net.SocketInputStream.read(SocketInputStream.java:183)
     * 	at java.base/java.net.SocketInputStream.read(SocketInputStream.java:140)
     * 	at com.mysql.cj.protocol.ReadAheadInputStream.fill(ReadAheadInputStream.java:107)
     * 	at com.mysql.cj.protocol.ReadAheadInputStream.readFromUnderlyingStreamIfNecessary(ReadAheadInputStream.java:150)
     * 	at com.mysql.cj.protocol.ReadAheadInputStream.read(ReadAheadInputStream.java:180)
     * 	at java.base/java.io.FilterInputStream.read(FilterInputStream.java:133)
     * 	at com.mysql.cj.protocol.FullReadInputStream.readFully(FullReadInputStream.java:64)
     * 	at com.mysql.cj.protocol.a.SimplePacketReader.readHeader(SimplePacketReader.java:63)
     * 	at com.mysql.cj.protocol.a.SimplePacketReader.readHeader(SimplePacketReader.java:45)
     * 	at com.mysql.cj.protocol.a.TimeTrackingPacketReader.readHeader(TimeTrackingPacketReader.java:52)
     * 	at com.mysql.cj.protocol.a.TimeTrackingPacketReader.readHeader(TimeTrackingPacketReader.java:41)
     * 	at com.mysql.cj.protocol.a.MultiPacketReader.readHeader(MultiPacketReader.java:54)
     * 	at com.mysql.cj.protocol.a.MultiPacketReader.readHeader(MultiPacketReader.java:44)
     * 	at com.mysql.cj.protocol.a.NativeProtocol.readMessage(NativeProtocol.java:557)
     * 	... 23 common frames omitted
     * [ERROR] 2023-11-21 04:42:35.129 +0800 - execute sql error: Communications link failure
     *
     * The last packet successfully received from the server was 1,996 milliseconds ago.  The last packet sent successfully to the server was 1,995 milliseconds ago.
     * [ERROR] 2023-11-21 04:42:35.129 +0800 - sql task error
     * com.mysql.cj.jdbc.exceptions.CommunicationsException: Communications link failure
     *
     * The last packet successfully received from the server was 1,996 milliseconds ago.  The last packet sent successfully to the server was 1,995 milliseconds ago.
     * 	at com.mysql.cj.jdbc.exceptions.SQLError.createCommunicationsException(SQLError.java:172)
     * 	at com.mysql.cj.jdbc.exceptions.SQLExceptionsMapping.translateException(SQLExceptionsMapping.java:64)
     * 	at com.mysql.cj.jdbc.ClientPreparedStatement.executeInternal(ClientPreparedStatement.java:960)
     * 	at com.mysql.cj.jdbc.ClientPreparedStatement.executeUpdateInternal(ClientPreparedStatement.java:1116)
     * 	at com.mysql.cj.jdbc.ClientPreparedStatement.executeUpdateInternal(ClientPreparedStatement.java:1066)
     * 	at com.mysql.cj.jdbc.ClientPreparedStatement.executeLargeUpdate(ClientPreparedStatement.java:1396)
     * 	at com.mysql.cj.jdbc.ClientPreparedStatement.executeUpdate(ClientPreparedStatement.java:1051)
     * 	at com.zaxxer.hikari.pool.ProxyPreparedStatement.executeUpdate(ProxyPreparedStatement.java:61)
     * 	at com.zaxxer.hikari.pool.HikariProxyPreparedStatement.executeUpdate(HikariProxyPreparedStatement.java)
     * 	at org.apache.dolphinscheduler.plugin.task.sql.SqlTask.executeUpdate(SqlTask.java:411)
     * 	at org.apache.dolphinscheduler.plugin.task.sql.SqlTask.executeFuncAndSql(SqlTask.java:245)
     * 	at org.apache.dolphinscheduler.plugin.task.sql.SqlTask.handle(SqlTask.java:184)
     * 	at org.apache.dolphinscheduler.server.worker.runner.DefaultWorkerDelayTaskExecuteRunnable.executeTask(DefaultWorkerDelayTaskExecuteRunnable.java:49)
     * 	at org.apache.dolphinscheduler.server.worker.runner.WorkerTaskExecuteRunnable.run(WorkerTaskExecuteRunnable.java:173)
     * 	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:515)
     * 	at com.google.common.util.concurrent.TrustedListenableFutureTask$TrustedFutureInterruptibleTask.runInterruptibly(TrustedListenableFutureTask.java:131)
     * 	at com.google.common.util.concurrent.InterruptibleTask.run(InterruptibleTask.java:74)
     * 	at com.google.common.util.concurrent.TrustedListenableFutureTask.run(TrustedListenableFutureTask.java:82)
     * 	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
     * 	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
     * 	at java.base/java.lang.Thread.run(Thread.java:829)
     * Caused by: com.mysql.cj.exceptions.CJCommunicationsException: Communications link failure
     *
     * The last packet successfully received from the server was 1,996 milliseconds ago.  The last packet sent successfully to the server was 1,995 milliseconds ago.
     * 	at jdk.internal.reflect.GeneratedConstructorAccessor88.newInstance(Unknown Source)
     * 	at java.base/jdk.internal.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)
     * 	at java.base/java.lang.reflect.Constructor.newInstance(Constructor.java:490)
     * 	at com.mysql.cj.exceptions.ExceptionFactory.createException(ExceptionFactory.java:59)
     * 	at com.mysql.cj.exceptions.ExceptionFactory.createException(ExceptionFactory.java:103)
     * 	at com.mysql.cj.exceptions.ExceptionFactory.createException(ExceptionFactory.java:149)
     * 	at com.mysql.cj.exceptions.ExceptionFactory.createCommunicationsException(ExceptionFactory.java:165)
     * 	at com.mysql.cj.protocol.a.NativeProtocol.readMessage(NativeProtocol.java:563)
     * 	at com.mysql.cj.protocol.a.NativeProtocol.checkErrorMessage(NativeProtocol.java:735)
     * 	at com.mysql.cj.protocol.a.NativeProtocol.sendCommand(NativeProtocol.java:674)
     * 	at com.mysql.cj.protocol.a.NativeProtocol.sendQueryPacket(NativeProtocol.java:966)
     * 	at com.mysql.cj.NativeSession.execSQL(NativeSession.java:1165)
     * 	at com.mysql.cj.jdbc.ClientPreparedStatement.executeInternal(ClientPreparedStatement.java:937)
     * 	... 18 common frames omitted
     * Caused by: java.net.SocketException: Socket closed
     * 	at java.base/java.net.SocketInputStream.read(SocketInputStream.java:183)
     * 	at java.base/java.net.SocketInputStream.read(SocketInputStream.java:140)
     * 	at com.mysql.cj.protocol.ReadAheadInputStream.fill(ReadAheadInputStream.java:107)
     * 	at com.mysql.cj.protocol.ReadAheadInputStream.readFromUnderlyingStreamIfNecessary(ReadAheadInputStream.java:150)
     * 	at com.mysql.cj.protocol.ReadAheadInputStream.read(ReadAheadInputStream.java:180)
     * 	at java.base/java.io.FilterInputStream.read(FilterInputStream.java:133)
     * 	at com.mysql.cj.protocol.FullReadInputStream.readFully(FullReadInputStream.java:64)
     * 	at com.mysql.cj.protocol.a.SimplePacketReader.readHeader(SimplePacketReader.java:63)
     * 	at com.mysql.cj.protocol.a.SimplePacketReader.readHeader(SimplePacketReader.java:45)
     * 	at com.mysql.cj.protocol.a.TimeTrackingPacketReader.readHeader(TimeTrackingPacketReader.java:52)
     * 	at com.mysql.cj.protocol.a.TimeTrackingPacketReader.readHeader(TimeTrackingPacketReader.java:41)
     * 	at com.mysql.cj.protocol.a.MultiPacketReader.readHeader(MultiPacketReader.java:54)
     * 	at com.mysql.cj.protocol.a.MultiPacketReader.readHeader(MultiPacketReader.java:44)
     * 	at com.mysql.cj.protocol.a.NativeProtocol.readMessage(NativeProtocol.java:557)
     * 	... 23 common frames omitted
     * [ERROR] 2023-11-21 04:42:35.129 +0800 - Task execute failed, due to meet an exception
     * org.apache.dolphinscheduler.plugin.task.api.TaskException: Execute sql task failed
     * 	at org.apache.dolphinscheduler.plugin.task.sql.SqlTask.handle(SqlTask.java:191)
     * 	at org.apache.dolphinscheduler.server.worker.runner.DefaultWorkerDelayTaskExecuteRunnable.executeTask(DefaultWorkerDelayTaskExecuteRunnable.java:49)
     * 	at org.apache.dolphinscheduler.server.worker.runner.WorkerTaskExecuteRunnable.run(WorkerTaskExecuteRunnable.java:173)
     * 	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:515)
     * 	at com.google.common.util.concurrent.TrustedListenableFutureTask$TrustedFutureInterruptibleTask.runInterruptibly(TrustedListenableFutureTask.java:131)
     * 	at com.google.common.util.concurrent.InterruptibleTask.run(InterruptibleTask.java:74)
     * 	at com.google.common.util.concurrent.TrustedListenableFutureTask.run(TrustedListenableFutureTask.java:82)
     * 	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
     * 	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
     * 	at java.base/java.lang.Thread.run(Thread.java:829)
     * Caused by: com.mysql.cj.jdbc.exceptions.CommunicationsException: Communications link failure
     *
     * The last packet successfully received from the server was 1,996 milliseconds ago.  The last packet sent successfully to the server was 1,995 milliseconds ago.
     * 	at com.mysql.cj.jdbc.exceptions.SQLError.createCommunicationsException(SQLError.java:172)
     * 	at com.mysql.cj.jdbc.exceptions.SQLExceptionsMapping.translateException(SQLExceptionsMapping.java:64)
     * 	at com.mysql.cj.jdbc.ClientPreparedStatement.executeInternal(ClientPreparedStatement.java:960)
     * 	at com.mysql.cj.jdbc.ClientPreparedStatement.executeUpdateInternal(ClientPreparedStatement.java:1116)
     * 	at com.mysql.cj.jdbc.ClientPreparedStatement.executeUpdateInternal(ClientPreparedStatement.java:1066)
     * 	at com.mysql.cj.jdbc.ClientPreparedStatement.executeLargeUpdate(ClientPreparedStatement.java:1396)
     * 	at com.mysql.cj.jdbc.ClientPreparedStatement.executeUpdate(ClientPreparedStatement.java:1051)
     * 	at com.zaxxer.hikari.pool.ProxyPreparedStatement.executeUpdate(ProxyPreparedStatement.java:61)
     * 	at com.zaxxer.hikari.pool.HikariProxyPreparedStatement.executeUpdate(HikariProxyPreparedStatement.java)
     * 	at org.apache.dolphinscheduler.plugin.task.sql.SqlTask.executeUpdate(SqlTask.java:411)
     * 	at org.apache.dolphinscheduler.plugin.task.sql.SqlTask.executeFuncAndSql(SqlTask.java:245)
     * 	at org.apache.dolphinscheduler.plugin.task.sql.SqlTask.handle(SqlTask.java:184)
     * 	... 9 common frames omitted
     * Caused by: com.mysql.cj.exceptions.CJCommunicationsException: Communications link failure
     *
     * The last packet successfully received from the server was 1,996 milliseconds ago.  The last packet sent successfully to the server was 1,995 milliseconds ago.
     * 	at jdk.internal.reflect.GeneratedConstructorAccessor88.newInstance(Unknown Source)
     * 	at java.base/jdk.internal.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)
     * 	at java.base/java.lang.reflect.Constructor.newInstance(Constructor.java:490)
     * 	at com.mysql.cj.exceptions.ExceptionFactory.createException(ExceptionFactory.java:59)
     * 	at com.mysql.cj.exceptions.ExceptionFactory.createException(ExceptionFactory.java:103)
     * 	at com.mysql.cj.exceptions.ExceptionFactory.createException(ExceptionFactory.java:149)
     * 	at com.mysql.cj.exceptions.ExceptionFactory.createCommunicationsException(ExceptionFactory.java:165)
     * 	at com.mysql.cj.protocol.a.NativeProtocol.readMessage(NativeProtocol.java:563)
     * 	at com.mysql.cj.protocol.a.NativeProtocol.checkErrorMessage(NativeProtocol.java:735)
     * 	at com.mysql.cj.protocol.a.NativeProtocol.sendCommand(NativeProtocol.java:674)
     * 	at com.mysql.cj.protocol.a.NativeProtocol.sendQueryPacket(NativeProtocol.java:966)
     * 	at com.mysql.cj.NativeSession.execSQL(NativeSession.java:1165)
     * 	at com.mysql.cj.jdbc.ClientPreparedStatement.executeInternal(ClientPreparedStatement.java:937)
     * 	... 18 common frames omitted
     * Caused by: java.net.SocketException: Socket closed
     * 	at java.base/java.net.SocketInputStream.read(SocketInputStream.java:183)
     * 	at java.base/java.net.SocketInputStream.read(SocketInputStream.java:140)
     * 	at com.mysql.cj.protocol.ReadAheadInputStream.fill(ReadAheadInputStream.java:107)
     * 	at com.mysql.cj.protocol.ReadAheadInputStream.readFromUnderlyingStreamIfNecessary(ReadAheadInputStream.java:150)
     * 	at com.mysql.cj.protocol.ReadAheadInputStream.read(ReadAheadInputStream.java:180)
     * 	at java.base/java.io.FilterInputStream.read(FilterInputStream.java:133)
     * 	at com.mysql.cj.protocol.FullReadInputStream.readFully(FullReadInputStream.java:64)
     * 	at com.mysql.cj.protocol.a.SimplePacketReader.readHeader(SimplePacketReader.java:63)
     * 	at com.mysql.cj.protocol.a.SimplePacketReader.readHeader(SimplePacketReader.java:45)
     * 	at com.mysql.cj.protocol.a.TimeTrackingPacketReader.readHeader(TimeTrackingPacketReader.java:52)
     * 	at com.mysql.cj.protocol.a.TimeTrackingPacketReader.readHeader(TimeTrackingPacketReader.java:41)
     * 	at com.mysql.cj.protocol.a.MultiPacketReader.readHeader(MultiPacketReader.java:54)
     * 	at com.mysql.cj.protocol.a.MultiPacketReader.readHeader(MultiPacketReader.java:44)
     * 	at com.mysql.cj.protocol.a.NativeProtocol.readMessage(NativeProtocol.java:557)
     * 	... 23 common frames omitted
     * [INFO] 2023-11-21 04:42:35.131 +0800 - Get a exception when execute the task, will send the task execute result to master, the current task execute result is TaskExecutionStatus{code=6, desc='failure'}
     * @param filePath file path
     * @param limit read lines limit
     * @return part file content
     */
    public List<String> readErrorFileContent(String filePath,
                                             int limit) {
        File file = new File(filePath);
        if (file.exists() && file.isFile()) {
            try (Stream<String> stream = Files.lines(Paths.get(filePath))) {
                // 错误日志在 execute sql error的下面
                return stream
                        .filter(line -> line.contains("ERROR"))
                        .limit(limit)
                        .collect(Collectors.toList());
            } catch (IOException e) {
                logger.error("read file error", e);
            }
        } else {
            logger.info("file path: {} not exists", filePath);
        }
        return Collections.emptyList();
    }
}
