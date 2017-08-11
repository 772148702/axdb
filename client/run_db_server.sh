mkdir temp && cd temp

fan wisp::Main -httpPort 8081 axdbCluster::ActionMod &
fan wisp::Main -httpPort 8082 axdbCluster::ActionMod &
fan wisp::Main -httpPort 8083 axdbCluster::ActionMod &

curl 'http://localhost:8081/m1/init?isLeader=true'
curl 'http://localhost:8081/m1/addNewLog?type=1&log=http%3A%2F%2Flocalhost%3A8081%2Fm1%2Chttp%3A%2F%2Flocalhost%3A8082%2Fm2%2Chttp%3A%2F%2Flocalhost%3A8083%2Fm3'
