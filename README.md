# DataXS

DataXS 是基于 DataX 的私人定制版，感谢 [DataX](https://github.com/alibaba/DataX) 团队

```bash
mvn -U clean package assembly:assembly -Dmaven.test.skip=true
```

### 区别

- 对 DataX 对部分功能进行删除，删除了一些依赖依赖库
- 只更改了  Mysql Job 配置，如下所示：

```json
{
    "job": {
        "content": [
            {
                "reader": {
                    "name": "mysqlreader",
                    "parameter": {
                        "connection": [
                            {
                                "jdbcUrl": "",
                                "username": "",
                                "password": "",
                                "table": [
                                    {
                                      "name": "",
                                      "column": []
                                    }
                                ]
                            }
                        ],
                        "where": ""
                    }
                },
                "writer": {
                    "name": "mysqlwriter",
                    "parameter": {
                        "connection": [
                            {
                                "jdbcUrl": "",
                                "username": "",
                                "password": "",
                                "table": [
                                    {
                                       "name": "",
                                       "column": [],
                                    }
                                ]
                            }
                        ],
                        "preSql": [],
                        "session": [],
                        "writeMode": "replace"
                    }
                }
            }
        ],
        "setting": {
            "speed": {
                "channel": ""
            }
        }
    }
}
```