# dfscache
1. How to deploy Ceph: https://www.howtoforge.com/tutorial/how-to-install-a-ceph-cluster-on-ubuntu-16-04/
2. Install LlibRados devel: sudo apt-get install librados-dev librbd-dev
3. Install ZMQ library dev: sudo apt-get install libzmq3-dev

### Build & install server
`GOBIN=$GOPATH/bin go install server.go`

### Build % install ZMQ client
`GOBIN=$GOPATH/bin go install client.go`

##Interact with server
###Upload file to storage
`curl -v -X POST -F "content=@<filename_to_upload>" http://localhost:9999/upload`

Example:
>curl --dump-header - -X POST -F "content=@/Users/dk/tmp/pad.tar.gz" http://localhost:9999/upload
 HTTP/1.1 100 Continue 
 HTTP/1.1 200 OK
 Content-Type: application/json
 Date: Sat, 08 Apr 2017 14:37:12 GMT
 Content-Length: 172
 
 {"pool":"dsfcache-ba","oid":"ba601f66-6f58-497a-a0c9-7e8ff21acf9b","size":108161,"exparation":1491665825,"uri":"/download/dsfcache-ba/ba601f66-6f58-497a-a0c9-7e8ff21acf9b"}

###Retrieve file from storage
`curl -v -O http://localhost:9999/download/<pool_name>/<object_id>`

Example:
`curl -v -O http://localhost:9999/download/dsfcache-ba/ba601f66-6f58-497a-a0c9-7e8ff21acf9b`
>curl -O http://localhost:9999/download/dsfcache-ba/ba601f66-6f58-497a-a0c9-7e8ff21acf9b

   % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                  Dload  Upload   Total   Spent    Left  Speed
 100  105k  100  105k    0     0  1398k      0 --:--:-- --:--:-- --:--:-- 1408k
 
###Delete file from storage
`curl -X DELETE http://localhost:9999/delete/<pool_name>/<object_id>`

>curl -X DELETE http://localhost:9999/delete/dsfcache-ba/ba601f66-6f58-497a-a0c9-7e8ff21acf9b

###Get file by ZMQ protocol
`GOBIN=$GOPATH/bin/client -oid <object_id>`
