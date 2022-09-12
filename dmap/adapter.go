package dmap

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v9"
	"strconv"
	"time"
)

type sDataT struct {
	T string `json:"t"`
}
type sData struct {
	sDataT
	Del bool           `json:"del"`
	Dk  string         `json:"dk"`
	K   string         `json:"k"`
	V   ValueInterface `json:"v"`
}

func syncEncode(s *sData) []byte {
	s.T = s.V.T()
	en, _ := json.Marshal(s)
	return en
}

func syncDecode(data []byte) *sData {
	var t sDataT
	var syncData = &sData{}
	_ = json.Unmarshal(data, &t)
	if f, ok := M[t.T]; ok {
		syncData.V = f()
		_ = json.Unmarshal(data, &syncData)
		return syncData
	}
	return nil
}

type Adapter interface {
	listen(func(*sData))
	broadcast(bool, string, string, ValueInterface) error
	stop()
}

type RedisAdapter struct {
	client                 *redis.Client
	conf                   *Conf
	streamKey, electionKey string
	ctx                    context.Context
	cancel                 context.CancelFunc
}

func newAdapter(c *Conf, s *Svc) (adapter Adapter) {
	if c.Redis != nil {
		adapter = newRedisAdapter(c, s)
	}
	return
}
func newRedisAdapter(c *Conf, s *Svc) Adapter {
	validateRedisConf(c)
	redisClient := redis.NewClient(&redis.Options{
		Addr:     c.Redis.Addr,
		Password: c.Redis.Pass,
		DB:       0,
		PoolSize: 20,
	})

	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		panic(`Redis数据库连接失败` + err.Error())
		return nil
	}
	ctx, cancel := context.WithCancel(s.ctx)
	r := &RedisAdapter{
		client:      redisClient,
		conf:        c,
		electionKey: fmt.Sprintf("%s-dmap-%s-lock", c.Redis.Prefix, c.s),
		ctx:         ctx,
		cancel:      cancel,
	}
	r.election()
	return r
}
func validateRedisConf(conf *Conf) {
	if conf.Redis.StreamMaxLen == 0 {
		conf.Redis.StreamMaxLen = 10000
	}
	if conf.Redis.SyncExpireTime == 0 {
		conf.Redis.SyncExpireTime = 300
	}
	if conf.Redis.StreamExpireTime == 0 {
		conf.Redis.StreamExpireTime = 300
	}
}
func (r *RedisAdapter) election() (channel string) {
	channel = strconv.FormatInt(time.Now().Unix(), 10)
	ctx := context.Background()

	setNX := r.client.SetNX(ctx, r.electionKey, channel, time.Duration(r.conf.Redis.SyncExpireTime)*time.Second)
	if setNX.Val() {
		r.streamKey = fmt.Sprintf("%s-dmap-%s-stream-%s", r.conf.Redis.Prefix, r.conf.s, channel)
		r.expireStreamKey()
		return
	}
	channel = r.client.Get(ctx, r.electionKey).Val()
	return
}
func (r *RedisAdapter) expireStreamKey() {
	ctx := context.Background()
	exp := func() {
		// 设置释放时间
		r.client.Expire(ctx, r.streamKey, time.Duration(r.conf.Redis.StreamExpireTime)*time.Second)
	}
	t := time.NewTicker(1 * time.Minute).C
	go func() {
		for {
			select {
			case <-r.ctx.Done():
				fmt.Println("RedisAdapter stop.", "stream", r.streamKey)
				return
			case <-t:
				exp()
				fmt.Println("expire redis stream key")
			}
		}
	}()
}

func (r *RedisAdapter) broadcast(del bool, dk, k string, v ValueInterface) (err error) {
	ctx := context.Background()
	args := &redis.XAddArgs{}
	args.ID = "*"
	args.Stream = r.streamKey
	args.MaxLen = r.conf.Redis.StreamMaxLen
	args.Approx = true
	args.Values = map[string]interface{}{
		"data": syncEncode(&sData{Del: del, Dk: dk, K: k, V: v}),
	}
	var sid string
	sid, err = r.client.XAdd(ctx, args).Result()
	if err != nil {
		return
	}
	fmt.Println("Syncing to pool ", sid, dk, k, v)
	return
}

func (r *RedisAdapter) listen(handler func(*sData)) {
	// 消费信息
	ctx := context.Background()
	// 退避系数
	var retreat time.Duration = 1
	var iteration = "0"
	for {
		select {
		case <-r.ctx.Done():
			fmt.Println("RedisStream Read End. Connection is Closed", "stream", r.streamKey)
			return
		default:
			// 0-0表示从头开始读取; $表示从最后一个开始读; block=0 表示无限等待，此处使用100毫秒超时
			args := &redis.XReadArgs{
				Streams: []string{r.streamKey, iteration},
				Count:   0,
				Block:   10 * time.Second,
			}
			res, err := r.client.XRead(ctx, args).Result()
			//退避策略
			if err != nil {
				if err != redis.Nil {
					fmt.Println(" RedisStream Read Error", "err", err)
					retreatTime := (2*(retreat-1) + 1) * time.Second
					time.Sleep(retreatTime)
					retreat++
				}
				continue
			}
			// 正常从RedisStream读取信息了，重置retreat
			retreat = 1
			for i := range res {
				for _, message := range res[i].Messages {
					//fmt.Println("get message ", message)
					iteration = message.ID
					data, ok := message.Values["data"].(string)
					if !ok {
						continue
					}
					handler(syncDecode([]byte(data)))
				}
			}
		}
	}
}
func (r *RedisAdapter) stop() {
	r.cancel()
	fmt.Println("redis adapter stop")
}
