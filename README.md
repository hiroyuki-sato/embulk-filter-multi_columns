# Multi Columns filter plugin for Embulk

The filter plugin to split a single column into multiple columns.

## Overview

* **Plugin type**: filter

## Configuration

- **rules**:  (array of separate rules, required)
- **default_timezone**: (timezone setting, default: `UTC`)

### rules

- **src**: The name of input column, The input columns must be string column. (string, required)
- **remain**: To use input column as output column.(boolean, default: `false`)
- **separator**: Regex separator character. (string, default: `\s+`)
- **columns**: Output column definitions. (array of columns, required)

## Example


```csv
id,full_name,registered_at
1,Darrion Wolf,2011-01-05 11:51:25
2,Ruby Rutherford,2011-07-05 09:37:54
3,Stuart Kautzer,2010-11-08 16:11:43
4,Jared Toy,2012-12-07 07:18:39
5,Kelsie Stoltenberg,2011-09-09 13:47:08
```


```yaml
in:
  type: file
  path_prefix: example/csv/sample_04.csv
  parser:
  # parser config.
    columns:
    - {name: id, type: long}
    - {name: full_name, type: string}
    - {name: registered_at, type: string }
filters:
  - type: multi_columns
    default_timezone: "Asia/Tokyo"
    rules:
      - src: full_name
        separater: '\s+'
        columns:
          - { name: first_name, type: string }
          - { name: last_name,  type: string }
      - src: registered_at
        columns:
          - { name: register_date, type: timestamp, format: "%Y-%m-%d" }
          - { name: register_time, type: string }
        remain: true
```


```text
+---------+----------------------+-------------------+------------------+-------------------------+----------------------+
| id:long | registered_at:string | first_name:string | last_name:string | register_date:timestamp | register_time:string |
+---------+----------------------+-------------------+------------------+-------------------------+----------------------+
|       1 |  2011-01-05 11:51:25 |           Darrion |             Wolf | 2011-01-04 15:00:00 UTC |             11:51:25 |
|       2 |  2011-07-05 09:37:54 |              Ruby |       Rutherford | 2011-07-04 15:00:00 UTC |             09:37:54 |
|       3 |  2010-11-08 16:11:43 |            Stuart |          Kautzer | 2010-11-07 15:00:00 UTC |             16:11:43 |
|       4 |  2012-12-07 07:18:39 |             Jared |              Toy | 2012-12-06 15:00:00 UTC |             07:18:39 |
|       5 |  2011-09-09 13:47:08 |            Kelsie |      Stoltenberg | 2011-09-08 15:00:00 UTC |             13:47:08 |
+---------+----------------------+-------------------+------------------+-------------------------+----------------------+
```

## Limitation

* Can't separate the column as JSON type.

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
