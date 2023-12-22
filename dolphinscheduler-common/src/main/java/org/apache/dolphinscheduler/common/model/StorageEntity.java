package org.apache.dolphinscheduler.common.model;

import java.util.Date;
import java.util.List;

import lombok.Data;

// StorageEneity is an entity representing a resource in the third-part storage service.
// It is only stored in t_ds_relation_resources_task if the resource is used by a task.
// It is not put in the model module because it has more attributes than corresponding objects stored
//  in table t_ds_relation_resources_task.

@Data
public class StorageEntity {

    private int id;
    private String fullName;
    private String fileName;
    private String alias;
    private String pfullName;
    private boolean isDirectory;
    private List<StorageEntity> child;
    private int userId;
    private String userName;
    private long size;
    private Date createTime;
    private Date updateTime;
}
