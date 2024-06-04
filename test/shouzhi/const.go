package main

const LST_MAPPING = `
{
  "dynamic": "strict",
  "properties": {
    "bal_amt": {
      "type": "double"
    }
  
  }
}
`

const TASK_TEMPLATE = `
	{
		"ticketId": 1,
		"targetInstance": "{}",
		"projectId": "{}",
		"user": "80318002@itc",
		"targetCluster": "{}",
		"isOffline": true,
		"instanceMode": 3,
		"targetRegion": "",
		"sourceRegion": "",
		"capacity": "",
		"content": [
			{
				"reader": {
					"name": "elasticsearchreader",
					"parameter": {
						"endpoint": "{}",
						"accessId": "{}",
						"accessKey": "{}",
						"index": "{}",
						"type": "{}",
						"columns": [],
						"query": "{\"query\":{}}"
					}
				},
				"writer": {
					"name": "elasticsearchwriter",
					"parameter": {
						"endpoint": "{}",
						"index": "{}",
						"type": "{}"
					}
				}
			}
		]
	}
`

const TASK_HEADER = `
 {
    "Content-Type": "application/json",
    "Accept-Type": "application/json",
    "Connection": "keep-alive",
    "Authorization":{}
}
`
