## Features
* HTTP client with `Select`, `Insert`, `Exec` methods
* `RowBinary`, `RowBinaryWithNames` and `RowBinaryWithNamesAndTypes` formats are supported
* `WithDiscovery` option for easy integration with Service Discovery
* Zero-reflection generic-based types
* You can implement your own Go type for a ClickHouse type. Example [type](./example/struct_tuple.go) and [tests](./example/struct_tuple_test.go)
* [External data](https://clickhouse.com/docs/engines/table-engines/special/external-data) is supported

## TODO
* Support `JSON` type

## Usage

### Select
```go
err := client.Select(ctx,
    "SELECT * FROM table LIMIT 10",
    rowbinary.WithFormatReader(func(r *rowbinary.FormatReader) error {
    for r.Next() {
        var value string
        if err := rowbinary.Scan(r, rowbinary.String, &value); err != nil {
            return err
        }
        // process value
    }
    return r.Err()
}))
```

### Insert
```go
err := client.Insert(ctx, "table",
    rowbinary.C("column", rowbinary.String),
    rowbinary.WithFormatWriter(func(w *rowbinary.FormatWriter) error {
    for _, value := range values {
        if err := rowbinary.Write(w, rowbinary.String, value); err != nil {
            return err
        }
    }
    return nil
}))
```

### Exec
```go
err := client.Exec(ctx, "CREATE TABLE table (id UInt64) ENGINE = MergeTree() ORDER BY id")
```
