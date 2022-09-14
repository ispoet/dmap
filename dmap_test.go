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
	ArgsId int    `json:"ArgsId"`
	Name   string `json:"name"`
}

func (in *InvokeArgs) Creator() ValueCreator {
	return func() ValueInterface {
		return new(InvokeArgs)
	}
}
func (in *InvokeArgs) TypeName() string {
	return "InvokeArgs"
}

type InvokeStruct1 struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
	Conn *Conn  `json:"-"`
}

func (in *InvokeStruct1) Invoke(args ValueInterface) {
	if in.Conn != nil {
		fmt.Println(os.Getenv("pod"), "invoke", args)
	} else {
		fmt.Println(os.Getenv("pod"), "不是目标id", args)
	}

	return
}

func (in *InvokeStruct1) Creator() ValueCreator {
	return func() ValueInterface {
		return &InvokeStruct1{
			ID:   0,
			Name: "",
			Conn: nil,
		}
	}
}
func (in *InvokeStruct1) TypeName() string {
	return "InvokeStruct1"
}

type MyStruct1 struct {
	ID   int       `json:"id"`
	Name string    `json:"name"`
	Son  MyStruct2 `json:"son"`
	Sync bool
}
type MyStruct2 struct {
	ID int `json:"id"`
}

func (s1 MyStruct1) Creator() ValueCreator {
	return func() ValueInterface {
		return new(MyStruct1)
	}
}
func (s1 MyStruct1) TypeName() string {
	return "MyStruct1"
}

type DInt int

func (i DInt) Creator() ValueCreator {
	return func() ValueInterface {
		return new(DInt)
	}
}
func (i DInt) TypeName() string {
	return "DInt"
}

func TestDamp(t *testing.T) {
	wg := &sync.WaitGroup{}
	p := "pod0"
	_ = os.Setenv("pod", p)
	run(t, p, wg)
	wg.Wait()
	t.Logf("%s is end", p)
}

func BenchmarkDmap(b *testing.B) {
	//  b.SetParallelism(4)//设置测试使用的CPU数
	m1 := New("m1")
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m1.Store("k3", DInt(1))
		}
	})
}
func init() {
	RegStruct([]ValueInterface{new(DInt), new(MyStruct1), new(InvokeStruct1), new(InvokeArgs)})
	_ = Config("test", &ConfRedis{
		Addr:   fmt.Sprintf("%s:%s", "127.0.0.1", "6379"),
		Prefix: "my_map",
	}).NewSvc()
}
func run(t *testing.T, p string, wg *sync.WaitGroup) {
	wg.Add(5)

	m1 := New("m1")
	k0 := fmt.Sprintf("%s_%s", p, "k0")
	time.AfterFunc(time.Duration(rand.Intn(5))*time.Second, func() {
		_ = os.Setenv("pod", p)
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
				ArgsId: 11111,
				Name:   "11111",
			})
			wg.Done()
		})

	})
	k1 := fmt.Sprintf("%s_%s", p, "k1")
	time.AfterFunc(time.Duration(rand.Intn(5))*time.Second, func() {
		m1.Store(k1, &MyStruct1{
			ID:   1,
			Name: "name_1",
			Sync: true,
			Son: MyStruct2{
				ID: 100,
			},
		})
		wg.Done()
		time.AfterFunc(time.Duration(rand.Intn(5))*time.Second, func() {
			if od, ok := m1.Load(k1); ok {
				v := od.(*MyStruct1)
				v.Name = v.Name + "0"
				m1.Store(k1, v)
			}
			wg.Done()
		})
	})

	k2 := fmt.Sprintf("%s_%s", p, "k2")
	time.AfterFunc(time.Duration(rand.Intn(5))*time.Second, func() {
		m1.Store(k2, DInt(10000))
		wg.Done()
	})
	t.Logf("%s is running", p)
}
