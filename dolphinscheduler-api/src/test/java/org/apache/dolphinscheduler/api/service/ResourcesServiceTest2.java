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

package org.apache.dolphinscheduler.api.service;

import org.apache.dolphinscheduler.api.ApiApplicationServer;
import org.apache.dolphinscheduler.api.dto.resources.Directory;
import org.apache.dolphinscheduler.api.dto.resources.FileLeaf;
import org.apache.dolphinscheduler.api.dto.resources.ResourceComponent;
import org.apache.dolphinscheduler.api.dto.resources.visitor.ResourceTreeVisitor;
import org.apache.dolphinscheduler.api.dto.resources.visitor.Visitor;
import org.apache.dolphinscheduler.api.permission.ResourcePermissionCheckService;
import org.apache.dolphinscheduler.api.service.impl.ResourcesServiceImpl;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.common.enums.AuthorizationType;
import org.apache.dolphinscheduler.common.enums.UserType;
import org.apache.dolphinscheduler.common.model.StorageEntity;
import org.apache.dolphinscheduler.dao.entity.Resource;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.mapper.ProcessDefinitionMapper;
import org.apache.dolphinscheduler.dao.mapper.ResourceMapper;
import org.apache.dolphinscheduler.dao.mapper.ResourceUserMapper;
import org.apache.dolphinscheduler.dao.mapper.TenantMapper;
import org.apache.dolphinscheduler.dao.mapper.UdfFuncMapper;
import org.apache.dolphinscheduler.dao.mapper.UserMapper;
import org.apache.dolphinscheduler.service.storage.StorageOperate;
import org.apache.dolphinscheduler.spi.enums.ResourceType;

import org.apache.commons.compress.utils.Lists;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.context.WebApplicationContext;

/**
 * resources service test
 */
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*", "javax.management.*", "org.w3c.*"})
@RunWith(SpringRunner.class)

@SpringBootTest(classes = ApiApplicationServer.class)
@WebAppConfiguration
public class ResourcesServiceTest2 {

    private static final Logger logger = LoggerFactory.getLogger(ResourcesServiceTest2.class);

    @InjectMocks
    private ResourcesServiceImpl resourcesService;

    @Autowired
    private ResourceMapper resourcesMapper;

    @Autowired
    private UdfFuncMapper udfFunctionMapper;

    @Autowired
    private TenantMapper tenantMapper;

    @Autowired
    private UserMapper userMapper;

    @Autowired
    private ResourceUserMapper resourceUserMapper;

    @Autowired
    private ProcessDefinitionMapper processDefinitionMapper;

    @Autowired(required = false)
    private StorageOperate storageOperate;

    @Value("${xuanwu.sql.path:xuanwu-sql}")
    private String codePath;

    @Autowired
    protected ResourcePermissionCheckService resourcePermissionCheckService;

    private AuthorizationType checkResourceType(ResourceType type) {
        return type.equals(ResourceType.FILE) ? AuthorizationType.RESOURCE_FILE_ID : AuthorizationType.UDF_FILE;
    }

    protected MockMvc mockMvc;
    @Autowired
    private WebApplicationContext webApplicationContext;
    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.webAppContextSetup(webApplicationContext).build();
    }

    @Test
    public void testReflushResource() {
        User user = new User();
        user.setId(2);
        user.setTenantCode("dolphinscheduler-dev");
        user.setUserType(UserType.GENERAL_USER);
        reflushResource(user);
    }

    private List<Resource> queryAuthoredResourceList(User loginUser, ResourceType type) {
        Set<Integer> resourceIds = resourcePermissionCheckService
                .userOwnedResourceIdsAcquisition(checkResourceType(type), loginUser.getId(), logger);
        if (resourceIds.isEmpty()) {
            return Collections.emptyList();
        }
        List<Resource> resources = resourcesMapper.selectBatchIds(resourceIds);
        resources = resources.stream().filter(rs -> rs.getType() == type).collect(Collectors.toList());
        return resources;
    }

    @Transactional
    public Result<Object> reflushResource(User loginUser) {
        List<Resource> allResourceList = queryAuthoredResourceList(loginUser, ResourceType.FILE);
        Visitor resourceTreeVisitor = new ResourceTreeVisitor(allResourceList);
        List<ResourceComponent> currentFileTree = resourceTreeVisitor.visit().getChildren();
        ResourceComponent currentFile = null;

        for (ResourceComponent resourceComponent : currentFileTree) {
            if (resourceComponent.getName().equals(codePath)) {
                currentFile = resourceComponent;
            }
        }

        String resDir = storageOperate.getResDir(loginUser.getTenantCode());
        List<StorageEntity> storageFileTree = storageOperate.listDir(loginUser.getTenantCode(), resDir + codePath);

        compareFileTree(loginUser, currentFile, currentFile.getChildren(), storageFileTree);

        return null;
    }

    // 递归两个文件夹，同步数据库和文件系统
    public void compareFileTree(User loginUser, ResourceComponent parentFile, List<ResourceComponent> dataBaseFileTree,
                                List<StorageEntity> storageFileTree) {
        // 遍历数据库文件夹
        // 如果在文件系统中有，数据库中没有则创建
        // 如果在文件系统中没有，数据库中有则删除
        // 如果都有，则递归子文件夹
        for (ResourceComponent dataBaseFile : dataBaseFileTree) {
            StorageEntity findStorageFile = null;
            for (StorageEntity storageFile : storageFileTree) {
                if (dataBaseFile.getName().equals(storageFile.getFileName())) {
                    findStorageFile = storageFile;
                    break;
                }
            }
            if (findStorageFile == null) {
                // 没有找到，删除
                resourcesMapper.deleteById(dataBaseFile.getId());
            } else {
                // 找到了，递归子文件夹
                if (dataBaseFile.isDirctory()) {
                    compareFileTree(loginUser, dataBaseFile, dataBaseFile.getChildren(), findStorageFile.getChild());
                }
            }
        }

        for (StorageEntity storageFile : storageFileTree) {
            ResourceComponent findResourceComponent = null;
            for (ResourceComponent dataBaseFile : dataBaseFileTree) {
                if (dataBaseFile.getName().equals(storageFile.getFileName())) {
                    findResourceComponent = dataBaseFile;
                    break;
                }
            }
            if (findResourceComponent == null) {
                // 创建
                Resource resource = new Resource();
                resource.setUserId(loginUser.getId());
                resource.setUserName(loginUser.getUserName());
                resource.setAlias(storageFile.getFileName());
                resource.setFileName(storageFile.getFileName());
                resource.setPid(parentFile.getId());
                resource.setFullName("/" + parentFile.getFullName() + "/" + storageFile.getFileName());
                resource.setCreateTime(new Date());
                resource.setUpdateTime(new Date());
                resource.setSize(storageFile.getSize());
                resource.setDescription("");
                resource.setType(ResourceType.FILE);
                // 递归子文件夹
                if (storageFile.isDirectory()) {
                    resource.setDirectory(true);
                    resourcesMapper.insert(resource);
                    compareFileTree(loginUser, getResourceComponent(resource), Lists.newArrayList(),
                            storageFile.getChild());
                } else {
                    resourcesMapper.insert(resource);
                }
            }
        }

    }

    private static ResourceComponent getResourceComponent(Resource resource) {
        ResourceComponent tempResourceComponent;
        if (resource.isDirectory()) {
            tempResourceComponent = new Directory();
        } else {
            tempResourceComponent = new FileLeaf();
        }

        tempResourceComponent.setName(resource.getAlias());
        tempResourceComponent.setFullName(resource.getFullName().replaceFirst("/", ""));
        tempResourceComponent.setId(resource.getId());
        tempResourceComponent.setPid(resource.getPid());
        tempResourceComponent.setIdValue(resource.getId(), resource.isDirectory());
        tempResourceComponent.setDescription(resource.getDescription());
        tempResourceComponent.setType(resource.getType());
        return tempResourceComponent;
    }
}
