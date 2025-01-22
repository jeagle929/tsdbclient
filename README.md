# tsdbclient

tsdb client helper that support TDengine

# Quick Start

```shell
go get github.com/jeagle929/tsdbclient
```

```go

package main

import (
	"fmt"
	"github.com/jeagle929/tsdbclient"
)

func main() {
	fmt.Println("This is a example")
	
	res, err := tsdbclient.QueryData("select d from `dma_report_on`")
	if err != nil {
		// handle error
    }
	
	fmt.Printf("query data result: %+v\n", res)
	
	fmt.Println("OK")
	
}
```

# Features

1. query and insert data to tsdb, include tdengine and influxdb
2. support subscribe of tdengine

# Environments

```shell
SVC_IOT_TDENGINE_HOST=127.0.0.1
SVC_IOT_TDENGINE_PORT=6041
SVC_IOT_TDENGINE_DB=iot
SVC_IOT_TDENGINE_USER=root
SVC_IOT_TDENGINE_PASS=taosdata
SVC_IOT_TDENGINE_PREC=ms
```
