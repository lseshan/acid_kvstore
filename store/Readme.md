./store --id 1 --cluster http://127.0.0.1:13379 --port 13380
curl -L http://127.0.0.1:13380/api/key/project -X GET
curl  -d ‘{“key”:“project”, “val”:“45"}’ -H ‘Content-Type: application/json’  -X POST http://127.0.0.1:13380/api/key
