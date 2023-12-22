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

package org.apache.dolphinscheduler.server.master.runner;

import static org.apache.dolphinscheduler.common.constants.CommandKeyConstants.CMD_PARAM_COMPLEMENT_DATA_END_DATE;
import static org.apache.dolphinscheduler.common.constants.CommandKeyConstants.CMD_PARAM_COMPLEMENT_DATA_START_DATE;
import static org.apache.dolphinscheduler.common.constants.CommandKeyConstants.CMD_PARAM_RECOVERY_START_NODE_STRING;
import static org.apache.dolphinscheduler.common.constants.CommandKeyConstants.CMD_PARAM_START_NODES;
import static org.powermock.api.mockito.PowerMockito.mock;

import org.apache.dolphinscheduler.common.enums.CommandType;
import org.apache.dolphinscheduler.common.enums.Flag;
import org.apache.dolphinscheduler.common.enums.ProcessExecutionTypeEnum;
import org.apache.dolphinscheduler.common.enums.WorkflowExecutionStatus;
import org.apache.dolphinscheduler.common.graph.DAG;
import org.apache.dolphinscheduler.common.utils.DateUtils;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinition;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.Schedule;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.repository.ProcessInstanceDao;
import org.apache.dolphinscheduler.server.master.config.MasterConfig;
import org.apache.dolphinscheduler.server.master.dispatch.executor.NettyExecutorManager;
import org.apache.dolphinscheduler.service.alert.ProcessAlertManager;
import org.apache.dolphinscheduler.service.bean.SpringApplicationContext;
import org.apache.dolphinscheduler.service.expand.CuringParamsService;
import org.apache.dolphinscheduler.service.process.ProcessService;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.context.ApplicationContext;

/**
 * test for WorkflowExecuteThread
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({WorkflowExecuteRunnable.class})
public class WorkflowExecuteRunnableTest {

    private WorkflowExecuteRunnable workflowExecuteThread;

    private ProcessInstance processInstance;

    private ProcessService processService;

    private ProcessInstanceDao processInstanceDao;

    private MasterConfig config;

    private ApplicationContext applicationContext;

    private StateWheelExecuteThread stateWheelExecuteThread;

    private CuringParamsService curingGlobalParamsService;

    @Before
    public void init() throws Exception {
        applicationContext = mock(ApplicationContext.class);
        SpringApplicationContext springApplicationContext = new SpringApplicationContext();
        springApplicationContext.setApplicationContext(applicationContext);

        config = new MasterConfig();
        Mockito.when(applicationContext.getBean(MasterConfig.class)).thenReturn(config);

        processService = mock(ProcessService.class);
        Mockito.when(applicationContext.getBean(ProcessService.class)).thenReturn(processService);

        processInstanceDao = mock(ProcessInstanceDao.class);

        processInstance = mock(ProcessInstance.class);
        Mockito.when(processInstance.getState()).thenReturn(WorkflowExecutionStatus.SUCCESS);
        Mockito.when(processInstance.getHistoryCmd()).thenReturn(CommandType.COMPLEMENT_DATA.toString());
        Mockito.when(processInstance.getIsSubProcess()).thenReturn(Flag.NO);
        Mockito.when(processInstance.getScheduleTime()).thenReturn(DateUtils.stringToDate("2020-01-01 00:00:00"));
        Map<String, String> cmdParam = new HashMap<>();
        cmdParam.put(CMD_PARAM_COMPLEMENT_DATA_START_DATE, "2020-01-01 00:00:00");
        cmdParam.put(CMD_PARAM_COMPLEMENT_DATA_END_DATE, "2020-01-20 23:00:00");
        ProcessDefinition processDefinition = new ProcessDefinition();
        processDefinition.setGlobalParamMap(Collections.emptyMap());
        processDefinition.setGlobalParamList(Collections.emptyList());
        Mockito.when(processInstance.getProcessDefinition()).thenReturn(processDefinition);

        stateWheelExecuteThread = mock(StateWheelExecuteThread.class);
        curingGlobalParamsService = mock(CuringParamsService.class);
        NettyExecutorManager nettyExecutorManager = mock(NettyExecutorManager.class);
        ProcessAlertManager processAlertManager = mock(ProcessAlertManager.class);
        workflowExecuteThread = PowerMockito.spy(
                new WorkflowExecuteRunnable(processInstance, processService, processInstanceDao, nettyExecutorManager,
                        processAlertManager, config, stateWheelExecuteThread, curingGlobalParamsService));
        // prepareProcess init dag
        Field dag = WorkflowExecuteRunnable.class.getDeclaredField("dag");
        dag.setAccessible(true);
        dag.set(workflowExecuteThread, new DAG());
        PowerMockito.doNothing().when(workflowExecuteThread, "endProcess");
    }

    @Test
    public void testParseStartNodeName() throws ParseException {
        try {
            Map<String, String> cmdParam = new HashMap<>();
            cmdParam.put(CMD_PARAM_START_NODES, "1,2,3");
            Mockito.when(processInstance.getCommandParam()).thenReturn(JSONUtils.toJsonString(cmdParam));
            Class<WorkflowExecuteRunnable> masterExecThreadClass = WorkflowExecuteRunnable.class;
            Method method = masterExecThreadClass.getDeclaredMethod("parseStartNodeName", String.class);
            method.setAccessible(true);
            List<String> nodeNames =
                    (List<String>) method.invoke(workflowExecuteThread, JSONUtils.toJsonString(cmdParam));
            Assert.assertEquals(3, nodeNames.size());
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testGetStartTaskInstanceList() {
        try {
            TaskInstance taskInstance1 = new TaskInstance();
            taskInstance1.setId(1);
            TaskInstance taskInstance2 = new TaskInstance();
            taskInstance2.setId(2);
            TaskInstance taskInstance3 = new TaskInstance();
            taskInstance3.setId(3);
            TaskInstance taskInstance4 = new TaskInstance();
            taskInstance4.setId(4);
            Map<String, String> cmdParam = new HashMap<>();
            cmdParam.put(CMD_PARAM_RECOVERY_START_NODE_STRING, "1,2,3,4");
            Mockito.when(processService.findTaskInstanceByIdList(
                    Arrays.asList(taskInstance1.getId(), taskInstance2.getId(), taskInstance3.getId(),
                            taskInstance4.getId())))
                    .thenReturn(Arrays.asList(taskInstance1, taskInstance2, taskInstance3, taskInstance4));
            Class<WorkflowExecuteRunnable> masterExecThreadClass = WorkflowExecuteRunnable.class;
            Method method = masterExecThreadClass.getDeclaredMethod("getRecoverTaskInstanceList", String.class);
            method.setAccessible(true);
            List<TaskInstance> taskInstances =
                    workflowExecuteThread.getRecoverTaskInstanceList(JSONUtils.toJsonString(cmdParam));
            Assert.assertEquals(4, taskInstances.size());

            cmdParam.put(CMD_PARAM_RECOVERY_START_NODE_STRING, "1");
            List<TaskInstance> taskInstanceEmpty =
                    (List<TaskInstance>) method.invoke(workflowExecuteThread, JSONUtils.toJsonString(cmdParam));
            Assert.assertTrue(taskInstanceEmpty.isEmpty());

        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testGetPreVarPool() {
        try {
            Set<String> preTaskName = new HashSet<>();
            preTaskName.add(Long.toString(1));
            preTaskName.add(Long.toString(2));

            TaskInstance taskInstance = new TaskInstance();

            TaskInstance taskInstance1 = new TaskInstance();
            taskInstance1.setId(1);
            taskInstance1.setTaskCode(1);
            taskInstance1.setVarPool("[{\"direct\":\"OUT\",\"prop\":\"test1\",\"type\":\"VARCHAR\",\"value\":\"1\"}]");
            taskInstance1.setEndTime(new Date());

            TaskInstance taskInstance2 = new TaskInstance();
            taskInstance2.setId(2);
            taskInstance2.setTaskCode(2);
            taskInstance2.setVarPool("[{\"direct\":\"OUT\",\"prop\":\"test2\",\"type\":\"VARCHAR\",\"value\":\"2\"}]");
            taskInstance2.setEndTime(new Date());

            Map<Integer, TaskInstance> taskInstanceMap = new ConcurrentHashMap<>();
            taskInstanceMap.put(taskInstance1.getId(), taskInstance1);
            taskInstanceMap.put(taskInstance2.getId(), taskInstance2);

            Map<Long, Integer> completeTaskList = new ConcurrentHashMap<>();
            completeTaskList.put(taskInstance1.getTaskCode(), taskInstance1.getId());
            completeTaskList.put(taskInstance2.getTaskCode(), taskInstance2.getId());

            Class<WorkflowExecuteRunnable> masterExecThreadClass = WorkflowExecuteRunnable.class;

            Field completeTaskMapField = masterExecThreadClass.getDeclaredField("completeTaskMap");
            completeTaskMapField.setAccessible(true);
            completeTaskMapField.set(workflowExecuteThread, completeTaskList);

            Field taskInstanceMapField = masterExecThreadClass.getDeclaredField("taskInstanceMap");
            taskInstanceMapField.setAccessible(true);
            taskInstanceMapField.set(workflowExecuteThread, taskInstanceMap);

            taskInstance.setEnvironmentConfig("export PG_DATA_WAREHOUSE_HOST=10.0.100.103\n" +
                    "export PG_DATA_WAREHOUSE_PORT=5434\n" +
                    "export PG_DATA_WAREHOUSE_USER=rep\n" +
                    "export PG_DATA_WAREHOUSE_PASSWORD='Q!c;B?p#Lae}1~Z'\n" +
                    "export PG_DATA_WAREHOUSE_DATABASE=data_warehouse\n" +
                    "export PG_DATA_WAREHOUSE_URL=jdbc:postgresql://10.0.100.103:5434/data_warehouse\n" +
                    "\n" +
                    "export PG_FMS_HOST=10.0.100.102\n" +
                    "export PG_FMS_PORT=5432\n" +
                    "export PG_FMS_USER=rep\n" +
                    "export PG_FMS_PASSWORD='Q!c;B?p#Lae}1~Z'\n" +
                    "export PG_FMS_DATABASE=fc_financial\n" +
                    "export PG_FMS_URL=jdbc:postgresql://10.0.100.102:5432/fc_financial\n" +
                    "\n" +
                    "export DORIS_DATA_WAREHOUSE_HOST=10.0.100.101\n" +
                    "export DORIS_DATA_WAREHOUSE_PORT=9030\n" +
                    "export DORIS_DATA_WAREHOUSE_USER=etl\n" +
                    "export DORIS_DATA_WAREHOUSE_PASSWORD='SDpfRnF07s'\n" +
                    "export DORIS_DATA_WAREHOUSE_DATABASE=ods\n" +
                    "export DORIS_DATA_WAREHOUSE_URL=jdbc:mysql://10.0.100.101:9030/ods\n" +
                    "\n" +
                    "export PG_DS_HOST=10.0.100.103\n" +
                    "export PG_DS_PORT=5434\n" +
                    "export PG_DS_USER=star_river\n" +
                    "export PG_DS_PASSWORD='@#R$%GBJ?*%WR'\n" +
                    "export PG_DS_DATABASE=star_river\n" +
                    "export PG_DS_URL=jdbc:postgresql://10.0.100.103:5434/star_river?currentSchema=ds");
            // taskInstance.setEnvironmentCode(10589110770784l);
            workflowExecuteThread.getPreVarPool(taskInstance, preTaskName);
            Assert.assertNotNull(taskInstance.getVarPool());

            taskInstance2.setVarPool("[{\"direct\":\"OUT\",\"prop\":\"test1\",\"type\":\"VARCHAR\",\"value\":\"2\"}]");
            completeTaskList.put(taskInstance2.getTaskCode(), taskInstance2.getId());

            completeTaskMapField.setAccessible(true);
            completeTaskMapField.set(workflowExecuteThread, completeTaskList);
            taskInstanceMapField.setAccessible(true);
            taskInstanceMapField.set(workflowExecuteThread, taskInstanceMap);

            workflowExecuteThread.getPreVarPool(taskInstance, preTaskName);
            Assert.assertNotNull(taskInstance.getVarPool());
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testCheckSerialProcess() {
        try {
            ProcessDefinition processDefinition1 = new ProcessDefinition();
            processDefinition1.setId(123);
            processDefinition1.setName("test");
            processDefinition1.setVersion(1);
            processDefinition1.setCode(11L);
            processDefinition1.setExecutionType(ProcessExecutionTypeEnum.SERIAL_WAIT);
            Mockito.when(processInstance.getId()).thenReturn(225);
            Mockito.when(processService.findProcessInstanceById(225)).thenReturn(processInstance);
            workflowExecuteThread.checkSerialProcess(processDefinition1);

            Mockito.when(processInstance.getId()).thenReturn(225);
            Mockito.when(processInstance.getNextProcessInstanceId()).thenReturn(222);

            ProcessInstance processInstance9 = new ProcessInstance();
            processInstance9.setId(222);
            processInstance9.setProcessDefinitionCode(11L);
            processInstance9.setProcessDefinitionVersion(1);
            processInstance9.setState(WorkflowExecutionStatus.SERIAL_WAIT);

            Mockito.when(processService.findProcessInstanceById(225)).thenReturn(processInstance);
            Mockito.when(processService.findProcessInstanceById(222)).thenReturn(processInstance9);
            workflowExecuteThread.checkSerialProcess(processDefinition1);
        } catch (Exception e) {
            Assert.fail();
        }
    }

    private List<Schedule> zeroSchedulerList() {
        return Collections.emptyList();
    }

    private List<Schedule> oneSchedulerList() {
        List<Schedule> schedulerList = new LinkedList<>();
        Schedule schedule = new Schedule();
        schedule.setCrontab("0 0 0 1/2 * ?");
        schedulerList.add(schedule);
        return schedulerList;
    }

}
