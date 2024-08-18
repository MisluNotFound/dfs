package heartbeat

import (
	"log"
	"math/rand"
	"moss/pkg/rabbitmq"
	"moss/pkg/setting"
	"strconv"
	"sync"
	"time"
)

var dataServers = make(map[string]time.Time)
var mutex sync.Mutex

// ListenHeartbeat consume the heartbeat message from dataServers
// and reset heartbeat time
func ListenHeartbeat() {
	q := rabbitmq.New(setting.RabbitMQAddr)
	defer q.Close()
	q.Bind("apiServers")
	c := q.Consume()
	go removeExpireDataServer()
	for msg := range c {
		dataServer, err := strconv.Unquote(string(msg.Body))
		if err != nil {
			log.Printf("ListenHeartbeat unquote msg error: %s, msg: %s\n", err, msg.Body)
			continue
		}
		//log.Printf("ListenHeartbeat msg\n")
		mutex.Lock()
		dataServers[dataServer] = time.Now()
		mutex.Unlock()
	}
}

func removeExpireDataServer() {
	for {
		time.Sleep(time.Second * 5)
		for s, t := range dataServers {
			if t.Add(10 * time.Second).Before(time.Now()) {
				mutex.Lock()
				delete(dataServers, s)
				mutex.Unlock()
			}
		}
	}
}

func GetDataServers() []string {
	mutex.Lock()
	defer mutex.Unlock()
	ds := make([]string, 0)
	for s := range dataServers {
		ds = append(ds, s)
	}

	return ds
}

// ChooseRandomDataServer excludes server which include complete shard
// that is used to fix lost data
func ChooseRandomDataServer(n int, exclude map[int]string) (ds []string) {
	candidates := make([]string, 0)
	revExcludeMap := make(map[string]int)
	for id, s := range exclude {
		revExcludeMap[s] = id
	}

	servers := GetDataServers()
	for i := range servers {
		s := servers[i]
		if _, excluded := revExcludeMap[s]; !excluded {
			candidates = append(candidates, s)
		}
	}
	length := len(candidates)
	if length < n {
		return
	}

	p := rand.Perm(length)
	for i := 0; i < n; i++ {
		ds = append(ds, candidates[p[i]])
	}
	return
}
