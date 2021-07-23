Please note that to control the primary keys and Storage table load mode, following processors section needs to be added.

- Replace the `"incremental"` and `"primary_key"` values in the `keboola.processor-create-manifest` definition to control the load mode.
- Replace the `"folder"` value in the `keboola.processor-move-files` definition to control the result table name.
- Use place only the [`Create File Manifest processor`](https://components.keboola.com/components/kds-team.processor-create-file-manifest)
inside the `after` section to load results into the File Blob Storage instead of the Synapse.

```json
    "after": [
      {
        "definition": {
          "component": "keboola.processor-move-files"
        },
        "parameters": {
          "direction": "tables",
          "addCsvSuffix": true,
          "folder": "test"
        }
      },
      {
        "definition": {
          "component": "keboola.processor-create-manifest"
        },
        "parameters": {
          "delimiter": ",",
          "enclosure": "\"",
          "incremental": false,
          "primary_key": [
            "aaa"
          ],
          "columns_from": "header"
        }
      },
      {
        "definition": {
          "component": "keboola.processor-skip-lines"
        },
        "parameters": {
          "lines": 1
        }
      }      
    ]
```

**NOTE:** New UI will soon remove the need to define the processors manually.