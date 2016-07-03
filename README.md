# Concat filter plugin for Embulk

The `embulk-filter-concat` plugin provides a similar function as CONCAT in SQL.

## Overview

* **Plugin type**: filter

## Configuration

- **name**: name of new column (string, required)
- **columns**: columns to add (array of hash, required)
    - **name**: name of column (required)
- **delimiter**: delimiter is inserted into between columns. (string, default: ` ` (space))

## Example

Say input.csv is as follows:

```
date,time,id
2015-07-13,00:00:00,0
2015-07-13,00:00:00,1
2015-07-13,00:00:00,2
```

```yaml
filters:
  - type: concat
    name: datetime
    columns:
    - {name: date}
    - {name: time}
```

combines columns with delimiter to new column as String type as :

```
date,time,id,datetime
2015-07-13,00:00:00,0,2015-07-13 00:00:00
2015-07-13,00:00:00,1,2015-07-13 00:00:00
2015-07-13,00:00:00,2,2015-07-13 00:00:00
```

## Limitation

- `Json` and `Timestamp` type are not supported.

## ToDo

- Add Test.

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
