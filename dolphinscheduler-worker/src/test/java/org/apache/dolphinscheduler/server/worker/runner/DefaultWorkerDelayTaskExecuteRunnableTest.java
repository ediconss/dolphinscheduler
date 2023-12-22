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

package org.apache.dolphinscheduler.server.worker.runner;

import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.enums.TaskExecutionStatus;
import org.apache.dolphinscheduler.server.worker.config.WorkerConfig;
import org.apache.dolphinscheduler.server.worker.rpc.WorkerMessageSender;
import org.apache.dolphinscheduler.service.alert.AlertClientService;
import org.apache.dolphinscheduler.service.storage.StorageOperate;
import org.apache.dolphinscheduler.service.task.TaskPluginManager;

import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;

public class DefaultWorkerDelayTaskExecuteRunnableTest {

    private TaskExecutionContext taskExecutionContext = Mockito.mock(TaskExecutionContext.class);

    private WorkerConfig workerConfig = Mockito.mock(WorkerConfig.class);

    private String masterAddress = "localhost:5678";

    private WorkerMessageSender workerMessageSender = Mockito.mock(WorkerMessageSender.class);

    private AlertClientService alertClientService = Mockito.mock(AlertClientService.class);

    private TaskPluginManager taskPluginManager = Mockito.mock(TaskPluginManager.class);

    private StorageOperate storageOperate = Mockito.mock(StorageOperate.class);

    public String getTrackingUrl(String errorMsg) {
        String trackingUrl = "";
        String regex = "tracking_url=(.*)";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(errorMsg);
        if (matcher.find()) {
            trackingUrl = matcher.group(1);
        }
        return trackingUrl;
    }
    @Test
    public void test() {
        String trackingUrl = getTrackingUrl("errCode = 2, detailMessage = Unknown column 'a' in 'table list'\n");
        System.out.println(trackingUrl);
    }

    @Test
    public void testDryRun() {
        TaskExecutionContext taskExecutionContext = TaskExecutionContext.builder()
                .dryRun(Constants.DRY_RUN_FLAG_YES)
                .taskInstanceId(0)
                .processDefineId(0)
                .firstSubmitTime(new Date())
                .taskLogName("TestLogName")
                .build();
        WorkerTaskExecuteRunnable workerTaskExecuteRunnable = new DefaultWorkerDelayTaskExecuteRunnable(
                taskExecutionContext,
                workerConfig,
                masterAddress,
                workerMessageSender,
                alertClientService,
                taskPluginManager,
                storageOperate);

        Assertions.assertAll(workerTaskExecuteRunnable::run);
        Assertions.assertEquals(TaskExecutionStatus.SUCCESS, taskExecutionContext.getCurrentExecutionStatus());
    }

}
