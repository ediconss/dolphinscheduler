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

package org.apache.dolphinscheduler.plugin.task.datax;

import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.plugin.task.api.TaskCallBack;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.enums.DataType;
import org.apache.dolphinscheduler.plugin.task.api.enums.Direct;
import org.apache.dolphinscheduler.plugin.task.api.enums.ResourceType;
import org.apache.dolphinscheduler.plugin.task.api.model.Property;
import org.apache.dolphinscheduler.plugin.task.api.parameters.SqlParameters;
import org.apache.dolphinscheduler.plugin.task.api.parameters.resource.AbstractResourceParameters;
import org.apache.dolphinscheduler.plugin.task.api.parameters.resource.DataSourceParameters;
import org.apache.dolphinscheduler.plugin.task.api.parameters.resource.ResourceParametersHelper;
import org.apache.dolphinscheduler.plugin.task.sql.SqlTask;
import org.apache.dolphinscheduler.spi.enums.DbType;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import com.google.common.collect.Maps;

@ExtendWith(MockitoExtension.class)
public class SQLTaskTest {

    private SqlTask sqlTask;

    private final TaskCallBack taskCallBack = (taskInstanceId, appIds) -> {
    };
    @BeforeEach
    public void before() throws Exception {
        // TaskExecutionContext taskExecutionContext = mock(TaskExecutionContext.class);
        // ResourceParametersHelper resourceParametersHelper = new ResourceParametersHelper();
        // String parameters = JSONUtils.toJsonString(createSQLParameters());
        // when(taskExecutionContext.getTaskParams()).thenReturn(parameters);
        // taskExecutionContext.setResourceParametersHelper(resourceParametersHelper);
        // this.sqlTask = new SqlTask(taskExecutionContext);
        // this.sqlTask.init();
    }

    @Test
    public void testHandleNullParamsMap() throws Exception {
        String parameters = JSONUtils.toJsonString(createSQLParameters());
        TaskExecutionContext taskExecutionContext = buildTestTaskExecutionContext();
        taskExecutionContext.setTaskParams(parameters);
        taskExecutionContext.setProcessDefineCode(1111l);
        taskExecutionContext.setProcessDefineVersion(2);
        HashMap<String, Property> propertyHashMap = Maps.newHashMap();
        Property property = new Property();
        property.setType(DataType.VARCHAR);
        property.setProp("PG_DATA_WAREHOUSE_HOST");
        property.setDirect(Direct.IN);
        property.setValue("test");
        propertyHashMap.put("PG_DATA_WAREHOUSE_HOST", property);
        taskExecutionContext.setPrepareParamsMap(propertyHashMap);
        SqlTask SqlTask = new SqlTask(taskExecutionContext);

        SqlTask.init();
        SqlTask.handle(taskCallBack);
    }

    private SqlParameters createSQLParameters() {
        SqlParameters sqlParameters = new SqlParameters();
        sqlParameters.setSql("INSERT INTO ods.ods_durian_blacklist_info" +
                " (id, \"type\", driver_name, driver_phone, plate_number, reason, " +
                "file, enabled, tenant_id, org_id, deleted, create_by, create_time," +
                " update_by, update_time) VALUES(772707196202504193, '${PG_DATA_WAREHOUSE_HOST}'," +
                " '姓名25261655', '13525261655', '', '一二三四五六七八九1一二三四五六七八九2一二三四五六七八九3一二三四五六七八九4一二三四五六七八九5一二三四五六七八九6一二三四五六七八九7一二三四五六七八九8一二三四五六七八九9一二三四五六七八10一二三四五六七八11一二三四五六七八12一二三四五六七八13一二三四五六七八14一二三四五六七八15一二三四五六七八16一二三四五六七八17一二三四五六七八18一二三四五六七八19一二三四五六七八20', NULL, 99, 1, 0, 99, 80, '2023-08-25 10:53:54.831', 80, '2023-08-25 10:57:55.946');\n");
        sqlParameters.setSqlType(1);
        sqlParameters.setType("POSTGRESQL");
        sqlParameters.setDatasource(1);
        sqlParameters.setTitle("test");
        // sqlParameters.setVarPool("[{\"prop\":\"PG_DATA_WAREHOUSE_HOST\",\"direct\":\"IN\",\"type\":\"VARCHAR\",\"value\":\"192.168.10.226\"},{\"prop\":\"PG_DATA_WAREHOUSE_PORT\",\"direct\":\"IN\",\"type\":\"VARCHAR\",\"value\":\"5442\"},{\"prop\":\"PG_DATA_WAREHOUSE_USER\",\"direct\":\"IN\",\"type\":\"VARCHAR\",\"value\":\"data_warehouse\"},{\"prop\":\"PG_DATA_WAREHOUSE_PASSWORD\",\"direct\":\"IN\",\"type\":\"VARCHAR\",\"value\":\"data_warehouse\"},{\"prop\":\"PG_DATA_WAREHOUSE_DATABASE\",\"direct\":\"IN\",\"type\":\"VARCHAR\",\"value\":\"data_warehouse\"},{\"prop\":\"PG_DATA_WAREHOUSE_URL\",\"direct\":\"IN\",\"type\":\"VARCHAR\",\"value\":\"jdbc:postgresql://192.168.10.226:5442/data_warehouse\"},{\"prop\":\"PG_FMS_HOST\",\"direct\":\"IN\",\"type\":\"VARCHAR\",\"value\":\"192.168.10.226\"},{\"prop\":\"PG_FMS_PORT\",\"direct\":\"IN\",\"type\":\"VARCHAR\",\"value\":\"5436\"},{\"prop\":\"PG_FMS_USER\",\"direct\":\"IN\",\"type\":\"VARCHAR\",\"value\":\"rep\"},{\"prop\":\"PG_FMS_PASSWORD\",\"direct\":\"IN\",\"type\":\"VARCHAR\",\"value\":\"rep\"},{\"prop\":\"PG_FMS_DATABASE\",\"direct\":\"IN\",\"type\":\"VARCHAR\",\"value\":\"fc_financial\"},{\"prop\":\"PG_FMS_URL\",\"direct\":\"IN\",\"type\":\"VARCHAR\",\"value\":\"jdbc:postgresql://192.168.10.226:5436/fc_financial\"},{\"prop\":\"DORIS_DATA_WAREHOUSE_HOST\",\"direct\":\"IN\",\"type\":\"VARCHAR\",\"value\":\"192.168.10.240\"},{\"prop\":\"DORIS_DATA_WAREHOUSE_PORT\",\"direct\":\"IN\",\"type\":\"VARCHAR\",\"value\":\"9030\"},{\"prop\":\"DORIS_DATA_WAREHOUSE_USER\",\"direct\":\"IN\",\"type\":\"VARCHAR\",\"value\":\"root\"},{\"prop\":\"DORIS_DATA_WAREHOUSE_PASSWORD\",\"direct\":\"IN\",\"type\":\"VARCHAR\",\"value\":\"root\"},{\"prop\":\"DORIS_DATA_WAREHOUSE_DATABASE\",\"direct\":\"IN\",\"type\":\"VARCHAR\",\"value\":\"ods\"},{\"prop\":\"DORIS_DATA_WAREHOUSE_URL\",\"direct\":\"IN\",\"type\":\"VARCHAR\",\"value\":\"jdbc:mysql://192.168.10.240:9030/ods\"},{\"prop\":\"PG_DS_HOST\",\"direct\":\"IN\",\"type\":\"VARCHAR\",\"value\":\"192.168.10.233\"},{\"prop\":\"PG_DS_PORT\",\"direct\":\"IN\",\"type\":\"VARCHAR\",\"value\":\"5434\"},{\"prop\":\"PG_DS_USER\",\"direct\":\"IN\",\"type\":\"VARCHAR\",\"value\":\"data_warehouse\"},{\"prop\":\"PG_DS_PASSWORD\",\"direct\":\"IN\",\"type\":\"VARCHAR\",\"value\":\"datawarehouse\"},{\"prop\":\"PG_DS_DATABASE\",\"direct\":\"IN\",\"type\":\"VARCHAR\",\"value\":\"data_warehouse\"},{\"prop\":\"PG_DS_URL\",\"direct\":\"IN\",\"type\":\"VARCHAR\",\"value\":\"jdbc:postgresql://192.168.10.233:5434/star_river?currentSchema=ds\"}]");
        return sqlParameters;
    }

    private TaskExecutionContext buildTestTaskExecutionContext() {
        TaskExecutionContext taskExecutionContext = new TaskExecutionContext();
        taskExecutionContext.setTaskAppId("app-id");
        taskExecutionContext.setExecutePath("/tmp/execution");
        ResourceParametersHelper resourceParametersHelper = new ResourceParametersHelper();
        Map<ResourceType, Map<Integer, AbstractResourceParameters>> resourceMap = Maps.newHashMap();
        Map<Integer, AbstractResourceParameters> dsMap = Maps.newHashMap();
        DataSourceParameters dataSourceParameters = new DataSourceParameters();
        dataSourceParameters.setType(DbType.POSTGRESQL);
        dataSourceParameters.setConnectionParams(
                "{\"user\":\"data_warehouse\",\"password\":\"datawarehouse\",\"address\":\"jdbc:postgresql://192.168.10.233:5434\",\"database\":\"data_warehouse\",\"jdbcUrl\":\"jdbc:postgresql://192.168.10.233:5434/data_warehouse\",\"driverClassName\":\"org.postgresql.Driver\",\"validationQuery\":\"select version()\"}");
        dsMap.put(1, dataSourceParameters);
        resourceMap.put(ResourceType.DATASOURCE, dsMap);
        resourceParametersHelper.setResourceMap(resourceMap);
        taskExecutionContext.setResourceParametersHelper(resourceParametersHelper);
        return taskExecutionContext;
    }

}
