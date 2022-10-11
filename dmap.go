package dmap

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

var (
	svcOnce sync.Once
	svc     *Svc
)

type ConfRedis struct {
	Addr             string // 地址
	Pass             string // 密码
	Prefix           string // 前缀
	SyncExpireTime   uint64 // 集群启动时需要选举一个stream管道，此值定义了频道的过期时间，若超时后再有新节点加入即会开启新频道。
	StreamExpireTime uint64 // stream key的过期时间。Tips：如果集群存活时间太短可能会导致stream无法自动删除
	StreamMaxLen     int64  // stream 最大长度，默认Approx为true
}
type Conf struct {
	Redis *ConfRedis
	s     string
	f     string
}

func Config(s, f string, redis *ConfRedis) *Conf {
	var conf = &Conf{
		s: s,
		f: f,
	}
	if redis != nil {
		conf.Redis = redis
	}
	return conf
}

type ValueCreator func() ValueInterface

var creatorFactory = make(map[string]ValueCreator)

func RegStruct(vs []ValueInterface) {
	for _, v := range vs {
		flags := v.DmapFlags()
		if len(flags) > 0 && flags[0] != "" {
			creatorFactory[flags[0]] = v.DmapCreator()
		}
	}
}

type InterceptorValueInterface interface {
	ValueInterface
	Before()
	After()
}
type InvokeInterface interface {
	Invoke(ValueInterface)
	ValueInterface
}

type ValueInterface interface {
	DmapCreator() ValueCreator
	DmapFlags() []string
}

type Svc struct {
	name    string
	flag    string
	maps    sync.Map
	conf    *Conf
	channel string
	Adapter
	ctx    context.Context
	cancel context.CancelFunc
}

func (c *Conf) NewSvc() *Svc {
	svcOnce.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		svc = &Svc{name: c.s, flag: c.f, maps: sync.Map{}, conf: c, ctx: ctx, cancel: cancel}
		svc.Adapter = newAdapter(c, svc)
		svc.validate()
		svc.running()
	})
	return svc
}

func (s *Svc) validate() {
	if s.Adapter == nil || s.name == "" {
		panic("configuration error")
	}
}
func (s *Svc) running() {
	go s.listen(s.syncHandler)
	go s.listenSignal()
	fmt.Println("svc is running ", s.name)
}
func (s *Svc) listenSignal() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, os.Interrupt)
	select {
	case sig := <-sigs:
		fmt.Println("收到信号notify sigs", sig)
		s.Stop()
		os.Exit(int(sig.(syscall.Signal))) // second signal. Exit directly.
	}
}
func (s *Svc) getFlag() string {
	return s.flag
}

func (s *Svc) Stop() {
	s.cancel()
	fmt.Println("svc is stop ", s.name)
}

func (s *Svc) sync(act, dk string, k string, v ValueInterface, f func()) {
	if err := s.broadcast(act, dk, k, v); err == nil {
		f()
	} else {
		fmt.Println("sync error", dk, k, v, err.Error())
	}
}
func (s *Svc) syncHandler(d *sData) {
	s.syncDmap(d)
	return
}
func (s *Svc) syncDmap(data *sData) {
	if data == nil || data.Dk == "" || data.V == nil {
		return
	}

	od, ok := svc.GetDmap(data.Dk)
	if ok {
		if s.getFlag() == data.P {
			return
		}
		if m, y := od.Load(data.K); y {
			switch m.(type) {
			case InterceptorValueInterface:

			}
		}
	}
	iviFunc := func(data *sData, f func()) {
		if m, ok := od.Load(data.K); ok {
			if ivi, yes := m.(InterceptorValueInterface); yes {
				ivi.Before()
				f()
				ivi.After()
				return
			}
			f()
		}
	}

	switch data.Act {
	case syncActDel:
		iviFunc(data, func() {
			od.OnlyDelete(data.K)
		})

	case syncActStore:
		if !ok {
			od = New(data.Dk)
		}
		iviFunc(data, func() {
			od.OnlyStore(data.K, data.V)
		})
	case syncActInvoke:
		if m, ok := od.Load(data.K); ok {
			in := m.(InvokeInterface)
			args := data.V.(ValueInterface)
			in.Invoke(args)
		}

	}
	return
}

func (s *Svc) GetDmap(k string) (d *Dmap, exist bool) {
	if v, ok := s.maps.Load(k); ok {
		d = v.(*Dmap)
		exist = true
	}
	return
}

func (s *Svc) RangeMap(f func(k, v interface{}) bool) {
	s.maps.Range(func(key, value interface{}) bool {
		return f(key, value)
	})
	return
}

func New(k string) *Dmap {
	d, _ := svc.maps.LoadOrStore(k, &Dmap{m: sync.Map{}, k: k})
	return d.(*Dmap)
}

type Dmap struct {
	m sync.Map
	k string
}

func (d *Dmap) OnlyDelete(k string) {
	d.m.Delete(k)
	return
}
func (d *Dmap) OnlyStore(k string, v ValueInterface) {
	d.m.Store(k, v)
}
func (d *Dmap) Load(k string) (v ValueInterface, ok bool) {
	if _v, isLoad := d.m.Load(k); isLoad {
		if __v, isValue := _v.(ValueInterface); isValue {
			v = __v
			ok = true
		}
	}
	return
}
func (d *Dmap) Store(k string, v ValueInterface) {
	svc.sync(syncActStore, d.k, k, v, func() {
		d.OnlyStore(k, v)
	})
}
func (d *Dmap) LoadOrStore(k string, v ValueInterface) (interface{}, bool) {
	if d, ok := d.Load(k); ok {
		return d, true
	}
	d.Store(k, v)
	return v, false
}

func (d *Dmap) Delete(k string) {
	if v, ok := d.Load(k); ok {
		svc.sync(syncActDel, d.k, k, v, func() {
			d.OnlyDelete(k)
		})
	}

	return
}
func (d *Dmap) Invoke(k string, args ValueInterface) {
	if v, ok := d.Load(k); ok {
		if in, isInvoke := v.(InvokeInterface); isInvoke {
			svc.sync(syncActInvoke, d.k, k, args, func() {
				in.Invoke(args)
			})
		}
	}
	return
}
func (d *Dmap) Range(f func(key, value interface{}) (shouldContinue bool)) {
	d.m.Range(f)
	return
}
