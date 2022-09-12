package main

import (
	"dmap/dmap"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"
)

var Svc *dmap.Svc

type MyStruct1 struct {
	ID   int       `json:"id"`
	Name string    `json:"name"`
	Son  MyStruct2 `json:"son"`
}
type MyStruct2 struct {
	ID int `json:"id"`
}

func (s1 MyStruct1) String() string {
	return fmt.Sprintf("%d,%s,%d", s1.ID, s1.Name, s1.Son.ID)
}
func (s1 MyStruct1) C() dmap.ValueCreator {
	return func() dmap.ValueInterface {
		return new(MyStruct1)
	}
}
func (s1 MyStruct1) T() string {
	return "MyStruct1"
}

type DInt int

func (i DInt) C() dmap.ValueCreator {
	return func() dmap.ValueInterface {
		return new(DInt)
	}
}
func (i DInt) T() string {
	return "DInt"
}

func main() {
	dmap.Test()
	dmap.RegStruct([]dmap.ValueInterface{new(DInt), new(MyStruct1)})
	//p := os.Args[0]
	var p string
	if len(os.Args) == 2 {
		p = os.Args[1]
	}
	Svc = dmap.Config("test", &dmap.ConfRedis{
		Addr:   fmt.Sprintf("%s:%s", "127.0.0.1", "6379"),
		Prefix: "my_map",
	}).NewSvc()
	if p == "b" {
		b()
	} else {
		a()
	}
}

func a() {
	wg := sync.WaitGroup{}
	wg.Add(1)

	//m1 := dmap.New("m1")
	//m1.Store("k0", DInt(111))
	m1 := dmap.New("m2")
	m1.Store("k0", MyStruct1{
		ID:   1,
		Name: "name_1",
		Son: MyStruct2{
			ID: 100,
		},
	})

	var ki = 1
	t := time.NewTicker(1000 * time.Millisecond)
	t1 := time.NewTicker(3 * time.Second)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-t1.C:
				Svc.RangeMap(func(k, v interface{}) bool {
					var m = make(map[string]interface{})
					v.(*dmap.Dmap).Range(func(key, value interface{}) bool {
						m[key.(string)] = value
						return true
					})
					fmt.Println("节点a的", k, "的所有元素", len(m))
					return true
				})

			case <-t.C:
				i := rand.Intn(100)
				if i%1 == 0 {
					k := fmt.Sprintf("k%d", ki)
					m1.Store(k, MyStruct1{
						ID:   ki,
						Name: fmt.Sprintf("name_%d", ki),
						Son: MyStruct2{
							ID: 100,
						},
					})
					fmt.Println("新增key ", k)
					ki++
				} else {
					rk := 0
					if ki >= 1 {
						rk = rand.Intn(ki)
					}
					k := fmt.Sprintf("k%d", rk)
					if od, ok := m1.Load(k); ok {
						v := od.(MyStruct1)
						v.Name = v.Name + "0"
						m1.Store(k, v)
						//fmt.Println("更新key", k, v)
					} else {
						fmt.Println("更新未找到key", k)
					}

				}

				//t.Reset(time.Duration(i) * time.Second)
			case <-time.After(60 * time.Second):
				return
			}
		}
	}()
	wg.Wait()
	fmt.Println("a is end")
}

func b() {
	wg := sync.WaitGroup{}
	wg.Add(1)

	m1 := dmap.New("m3")
	m1.Store("k0", DInt(1))
	//ki := 1
	t := time.NewTicker(1 * time.Second)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-time.NewTicker(3 * time.Second).C:
				Svc.RangeMap(func(k, v interface{}) bool {
					var m = make(map[string]interface{})
					v.(*dmap.Dmap).Range(func(key, value interface{}) bool {
						m[key.(string)] = value
						return true
					})
					fmt.Println("节点b的", k, "的所有元素", len(m))
					return true
				})
			case <-t.C:
				i := rand.Intn(10) + 5
				//if i%2 == 0 {
				//	k := fmt.Sprintf("k%d", ki)
				//	m1.Store(k, strconv.Itoa(ki))
				//	fmt.Println("新增key ", k)
				//	ki++
				//} else {
				//	rk := rand.Intn(ki - 1)
				//	k := fmt.Sprintf("k%d", rk)
				//	if od, ok := m1.Load(k); ok {
				//		v := od.(string) + strconv.Itoa(rk)
				//		m1.Store(k, v)
				//		fmt.Println("更新key", k, v)
				//	} else {
				//		fmt.Println("更新未找到key", k)
				//	}
				//
				//}

				t.Reset(time.Duration(i) * time.Second)
			case <-time.After(60 * time.Second):
				return
			}
		}
	}()
	wg.Wait()
	fmt.Println("b is end")
}
