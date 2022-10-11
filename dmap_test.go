package dmap

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

type Conn struct {
	ID string `json:"id"`
}
type InvokeArgs struct {
	ArgsId  int    `json:"ArgsId"`
	Name    string `json:"name"`
	podFlag string
}

func (in *InvokeArgs) DmapCreator() ValueCreator {
	return func() ValueInterface {
		return new(InvokeArgs)
	}
}
func (in *InvokeArgs) DmapFlags() []string {
	return []string{"InvokeArgs", in.podFlag}
}

type InvokeStruct1 struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
	Conn *Conn  `json:"-"`
}

func (in *InvokeStruct1) Invoke(args ValueInterface) {
	if _, ok := args.(*InvokeArgs); ok {
		if in.Conn != nil {
			fmt.Println("received invoke", args)
		} else {
			fmt.Println("received invoke 不是目标id", args)
		}
	}
	return
}

func (in *InvokeStruct1) DmapCreator() ValueCreator {
	return func() ValueInterface {
		return &InvokeStruct1{}
	}
}
func (in *InvokeStruct1) DmapFlags() []string {
	return []string{"InvokeStruct1"}
}

type MyStruct1 struct {
	ID      int       `json:"id"`
	Name    string    `json:"name"`
	Son     MyStruct2 `json:"son"`
	Sync    bool
	podFlag string
}
type MyStruct2 struct {
	ID int `json:"id"`
}

func (s1 MyStruct1) DmapCreator() ValueCreator {
	return func() ValueInterface {
		return new(MyStruct1)
	}
}
func (s1 MyStruct1) DmapFlags() []string {
	return []string{"MyStruct1", s1.podFlag}
}

type MyStruct3 struct {
	ID      int    `json:"id"`
	Name    string `json:"name"`
	podFlag string
}

func (s3 MyStruct3) DmapCreator() ValueCreator {
	return func() ValueInterface {
		return new(MyStruct1)
	}
}
func (s3 MyStruct3) DmapFlags() []string {
	return []string{"MyStruct1", s3.podFlag}
}
func (s3 MyStruct3) Before() {
	fmt.Println("received Before ", s3)
}
func (s3 MyStruct3) After() {
	fmt.Println("received After ", s3)
}

type DInt int

func (i DInt) DmapCreator() ValueCreator {
	return func() ValueInterface {
		return new(DInt)
	}
}
func (i DInt) DmapFlags() []string {
	return []string{"DInt"}
}

func TestDamp(t *testing.T) {
	wg := &sync.WaitGroup{}
	//pods := []string{"pod0", "pod1"}
	wg.Add(10)
	go run(t, "pod0", wg)
	go run(t, "pod1", wg)
	wg.Wait()
	time.Sleep(5 * time.Second)
	t.Logf("test end")
}

func BenchmarkDmap(b *testing.B) {
	//  b.SetParallelism(4)//设置测试使用的CPU数
	m1 := New("m1")
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m1.Store("k3", DInt(1))
			m1.Store("k1", &MyStruct1{
				ID:   1,
				Name: "name_1",
				Sync: true,
				Son: MyStruct2{
					ID: 100,
				},
				podFlag: "p1",
			})
			m1.Store("k3", &MyStruct3{
				ID:      1,
				Name:    "name_3",
				podFlag: "p2",
			})
		}
	})
}
func init() {
	_ = os.Setenv("podName", "pod1")
	RegStruct([]ValueInterface{new(DInt), new(MyStruct1), new(InvokeStruct1), new(InvokeArgs)})
	_ = Config("test", os.Getenv("podName"), &ConfRedis{
		Addr:   fmt.Sprintf("%s:%s", "127.0.0.1", "6379"),
		Prefix: "my_map",
	}).NewSvc()
}
func run(t *testing.T, p string, wg *sync.WaitGroup) {
	m1 := New("m1")
	k0 := "k0"
	time.AfterFunc(time.Duration(rand.Intn(3))*time.Second, func() {
		m1.Store(k0, &InvokeStruct1{
			ID:   0,
			Name: "name_0",
			Conn: &Conn{
				ID: "cid0",
			},
		})
		wg.Done()
		time.AfterFunc(time.Duration(rand.Intn(5))*time.Second, func() {
			m1.Invoke(k0, &InvokeArgs{
				ArgsId:  11111,
				Name:    "11111",
				podFlag: p,
			})
			wg.Done()
		})

	})
	k1 := "k1"
	time.AfterFunc(time.Duration(rand.Intn(3))*time.Second, func() {
		m1.Store(k1, &MyStruct1{
			ID:   1,
			Name: "name_1",
			Sync: true,
			Son: MyStruct2{
				ID: 100,
			},
			podFlag: p,
		})
		wg.Done()
		time.AfterFunc(time.Duration(rand.Intn(3))*time.Second, func() {
			if od, ok := m1.Load(k1); ok {
				v := od.(*MyStruct1)
				v.Name = v.Name + "0"
				m1.Store(k1, v)
			}
			wg.Done()
		})
	})

	k2 := "k2"
	time.AfterFunc(time.Duration(rand.Intn(3))*time.Second, func() {
		m1.Store(k2, DInt(10000))
		wg.Done()
	})

	k3 := "k3"
	time.AfterFunc(time.Duration(rand.Intn(3))*time.Second, func() {
		m1.Store(k3, &MyStruct3{
			ID:      1,
			Name:    "name_3",
			podFlag: p,
		})
		wg.Done()
		time.AfterFunc(time.Duration(rand.Intn(3))*time.Second, func() {
			if od, ok := m1.Load(k3); ok {
				v := od.(*MyStruct3)
				v.Name = v.Name + "0"
				m1.Store(k3, v)
			}
			wg.Done()
		})
	})
	t.Logf("%s is running", p)
}
