# Install/Update template

Install the `task` template in elasticsearch:

``` shell
INSTANCE=http://something:9200
TEMPLATE_NAME=template_swh_tasks
curl -i -H'Content-Type: application/json' -d@./elastic-template.json -XPUT ${INSTANCE}/_template/${TEMPLATE_NAME}
```

# Update index settings

The index setup is fixed and defined on the template settings basis.

When that setup needs to change, we need to update both the template
and the existing indices.

To update index settings:

``` shell
INDEX_NAME=swh-tasks-2017-11
curl -i -H'Content-Type: application/json' -d@./update-index-settings.json -XPUT ${INSTANCE}/${INDEX_NAME}/_settings
```
