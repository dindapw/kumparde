copy $schema.$table $list_column from 's3://$bucket/$key'
access_key_id '$access_key'
secret_access_key '$secret_key'
csv gzip
NULL ''
timeformat 'auto'