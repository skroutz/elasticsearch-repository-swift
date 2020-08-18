Swift repository plugin for Elasticsearch
=========================================

***This is an experimental version the elasticsearch-repository-swift to support ES v7.7.0. Use it at your own risk.***

In order to install the plugin, simply run: `bin/plugin install org.wikimedia.elasticsearch.swift/swift-repository-plugin/<version>`.

For Elasticsearch versions prior to 7.7.0, please refer to https://github.com/wikimedia/search-repository-swift.

Starting with version 7.7.0.X of this plugin the plugin is developed on `skroutz`, and compiled against the each needed version locally by hand.

The resulting versioning schema looks like this: `ES_MAJOR.ES_MINOR.ES_PATCH.REVISION`

## Building the plugin

The plugin needs a JDK 14 installation.

Either fetch it from you package manager or download it from the [official website](https://jdk.java.net/14/)

To build the plugin run `JAVA_HOME=<path_to_jdk_14> ./gradlew build`

To run the tests run `JAVA_HOME=<path_to_jdk_14> ./gradlew test`

A test suite run requires proper Swift account and container settings on an existing Swift installation.

## Create Repository
```
    $ curl -XPUT 'http://localhost:9200/_snapshot/my_backup' -d '{
        "type": "swift",
        "settings": {
            "swift_url": "http://localhost:8080/auth/v1.0/",
            "swift_container": "my-container",
            "swift_username": "myuser",
            "swift_password": "mypass!"
        }
    }'
```

See [Snapshot And Restore](https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html) for more information


## Settings
|  Setting                            |   Description
|-------------------------------------|------------------------------------------------------------
| swift_container                     | Swift container name. **Mandatory**
| swift_url                           | Swift auth url. **Mandatory**
| swift_authmethod                    | Swift auth method, one of "KEYSTONE" "TEMPAUTH" or "" for basic auth
| swift_password                      | Swift password
| swift_tenantname                    | Swift tenant name, only used with keystone auth
| swift_username                      | Swift username
| swift_preferred_region              | Region to use.  If you do not specify a region, Swift will pick the endpoint of the first region.  If you have multiple regions, the order is not guarenteed.
| chunk_size                          | Maximum size for individual objects in the snapshot. Defaults to `5gb` as that's the Swift default
| compress                            | Turns on compression of the snapshot files. Defaults to `false` as it tends to break with Swift
| max_restore_bytes_per_sec           | Throttles per node restore rate. Defaults to `20mb` per second.
| max_snapshot_bytes_per_sec          | Throttles per node snapshot rate. Defaults to `20mb` per second.

## Configuration Settings
Plugin settings to be placed in elasticsearch YAML configuration. Keep defaults, unless problems are detected.

|  Setting                            |   Description
|-------------------------------------|------------------------------------------------------------
| repository_swift.minimize_blob_exists_checks | true (default) or false. Reduces volume of SWIFT requests to check a blob's existence.
| repository_swift.allow_caching     | true or false (default). Allow JOSS caching
