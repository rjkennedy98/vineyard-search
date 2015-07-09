#!/bin/bash

bash /Users/rkennedy/Desktop/mlcp-1.3-3/bin/mlcp.sh import -host localhost -port 8033 \
    -username vineyarduser -password vineyarduser -mode local \
    -input_file_path /Users/rkennedy/ml-workplace/vineyard-search/scripts/wineinfo.csv \
    -input_file_type delimited_text \
    -transform_module /mlcp/wineTransform.xqy \
    -transform_namespace "https://github.com/rjkennedy98/vineyard-search/mlcp" \
    -transform_param "my-value" \
    -thread_count 12 -output_permissions vineyard-role,read,vineyard-role,insert,vineyard-role,execute,vineyard-role,update

