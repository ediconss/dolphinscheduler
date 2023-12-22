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

package org.apache.dolphinscheduler.service.storage.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import org.apache.dolphinscheduler.common.model.StorageEntity;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.obs.services.ObsClient;

@RunWith(MockitoJUnitRunner.class)
public class ObsOperatorTest {

    private static final String ACCESS_KEY_ID_MOCK = "IBKKI8AGRUYFXXFVARWH";
    private static final String ACCESS_KEY_SECRET_MOCK = "DMCkqCsNyziCWNFHYdIrIqRFsJyuPnBUHPFdx9hK";
    private static final String REGION_MOCK = "southwest-2";
    private static final String END_POINT_MOCK = "obs.cn-southwest-2.myhuaweicloud.com";
    private static final String BUCKET_NAME_MOCK = "fruits-test";
    private static final String TENANT_CODE_MOCK = "dolphinscheduler-dev";
    private static final String DIR_MOCK = "DIR_MOCK";
    private static final String FILE_NAME_MOCK = "FILE_NAME_MOCK";
    private static final String FILE_PATH_MOCK = "FILE_PATH_MOCK";

    @Mock
    private ObsClient ossClientMock;

    private ObsOperator ossOperator;

    @Before
    public void setUp() throws Exception {
        ossOperator = spy(new ObsOperator());
        doReturn(ACCESS_KEY_ID_MOCK).when(ossOperator).readObsAccessKeyID();
        doReturn(ACCESS_KEY_SECRET_MOCK).when(ossOperator).readObsAccessKeySecret();
        // doReturn(REGION_MOCK).when(ossOperator).ge
        doReturn(BUCKET_NAME_MOCK).when(ossOperator).readObsBucketName();
        doReturn(END_POINT_MOCK).when(ossOperator).readObsEndPoint();
        doReturn(ossClientMock).when(ossOperator).getObsClient();
        doNothing().when(ossOperator).ensureBucketSuccessfullyCreated(any());

        ossOperator.init();

    }

    @Test
    public void initOssOperator() {
        // verify(ossOperator, times(1)).getObsClient();
        // Assert.assertEquals(ACCESS_KEY_ID_MOCK, ossOperator.getAccessKeyId());
        // Assert.assertEquals(ACCESS_KEY_SECRET_MOCK, ossOperator.getAccessKeySecret());
        //// Assert.assertEquals(REGION_MOCK, ossOperator.getRegion());
        // Assert.assertEquals(BUCKET_NAME_MOCK, ossOperator.getBucketName());
    }

    @Test
    public void getListDir() {
        final String expectedResourceDir = String.format("dolphinscheduler/%s/resources/", TENANT_CODE_MOCK);
        List<StorageEntity> storageEntities =
                ossOperator.listDir(TENANT_CODE_MOCK, expectedResourceDir + "xuanwu-sql/面试");
        System.out.println(storageEntities);
    }

}
