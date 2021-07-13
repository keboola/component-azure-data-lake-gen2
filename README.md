Azure Data Lake Gen2 Extractor
=============

This component can be used to extract files from Azure Data Lake Gen2 and apply after processors to save them into storage.

**Table of contents:**

[TOC]

Prerequisites
=============

Prepare an account name, account key, and file system.


Configuration
=============
 - Account Name (account_name) - [REQ] Azure Data Lake Gen2 Account Name
 - Access Key (#account_key) - [REQ] Azure Data Lake Gen2 Access Key
 - File System (file_system) - [REQ] Azure Data Lake Gen2 file system (file-system-name means the name of the container,
   it is the data lake gen2 name for it)
 - File - [REQ] file dictionary:
    - file_name [REQ] name of file, or pattern (dir/text.csv will find the text csv file in dir , 
      dir/text_*.csv will find all csv files starting with text_ in the dir)
    - new_files_only [REQ] - if true, will only fetch files that have been updated since the lst run of the component
    - add_timestamp [REQ] - if true, will append the timestamp infront of the name of the file
   
Sample Configuration
=============

### Configuration Parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  {
    "account_name" : "YOUR_ACCOUNT_NAME",
    "#account_key" : "YOUR_KEY",
    "file_system" : "container",
    "file": {
      "file_name": "some_dir_in_container/file.csv",
      "new_files_only": false,
      "add_timestamp": true
    }
  }
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
### Processors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
{
  "before": [],
  "after": [
    {
      "definition": {
        "component": "keboola.processor-move-files"
      },
      "parameters": {
        "direction": "tables"
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
        "primary_key": [],
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
    },
    {
      "definition": {
        "component": "keboola.processor-add-row-number-column"
      },
      "parameters": {
        "column_name": "azure_row_number"
      }
    },
    {
      "definition": {
        "component": "keboola.processor-add-filename-column"
      },
      "parameters": {
        "column_name": "azure_filename"
      }
    }
  ]
}
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
