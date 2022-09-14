package dmap

import (
	"fmt"
	"github.com/gorilla/websocket"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

var dmapSvc *Svc

type InvokeArgs struct {
	ClientID int    `json:"cid"`
	Name     string `json:"name"`
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
	ID   int             `json:"id"`
	Name string          `json:"name"`
	Conn *websocket.Conn `json:"-"`
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

func (s1 MyStruct1) String() string {
	return fmt.Sprintf("%d,%s,%d", s1.ID, s1.Name, s1.Son.ID)
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

func TestDmap(t *testing.T) {
	Test()
	RegStruct([]ValueInterface{new(DInt), new(MyStruct1), new(InvokeStruct1), new(InvokeArgs)})
	//p := os.Args[0]
	var p string
	if len(os.Args) == 2 {
		p = os.Args[1]
	}
	dmapSvc = Config("test", &ConfRedis{
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
	//m1 := dmap.New("m2")
	//m1.Store("k0", MyStruct1{
	//	ID:   1,
	//	Name: "name_1",
	//	Sync: true,
	//	Son: MyStruct2{
	//		ID: 100,
	//	},
	//})

	conn, _, err := websocket.DefaultDialer.Dial(fmt.Sprintf("wss://127.0.0.1"), nil)
	if err != nil {
		fmt.Println(err)
		return
	}

	m1 := New("a")
	v1 := &InvokeStruct1{
		ID:   0,
		Name: "name_0",
		Conn: conn,
	}
	m1.Store("k0", v1)
	//m1.Invoke("k0", InvokeArgs{
	//	ID:   11111,
	//	Name: "11111",
	//})

	var ki = 1
	t := time.NewTicker(3000 * time.Millisecond)
	t1 := time.NewTicker(3 * time.Second)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-t1.C:
				//Svc.RangeMap(func(k, v interface{}) bool {
				//	var m = make(map[string]interface{})
				//	v.(*dmap.Dmap).Range(func(key, value interface{}) bool {
				//		m[key.(string)] = value
				//		return true
				//	})
				//	fmt.Println("节点a的", k, "的所有元素", len(m))
				//	return true
				//})

			case <-t.C:
				i := rand.Intn(100)
				if i%20 == 0 {
					k := fmt.Sprintf("k%d", ki)
					//m1.Store(k, MyStruct1{
					//	ID:   ki,
					//	Name: fmt.Sprintf("name_%d", ki),
					//	Sync: true,
					//	Son: MyStruct2{
					//		ID: 100,
					//	},
					//})
					m1.Store(k, &InvokeStruct1{
						ID:   ki,
						Name: fmt.Sprintf("name_%d", ki),
						Conn: conn,
					})
					fmt.Println("新增key ", k)
					ki++
				} else {
					rk := 0
					if ki >= 1 {
						rk = rand.Intn(ki)
					}
					k := fmt.Sprintf("k%d", rk)
					m1.Invoke(k, &InvokeArgs{
						ClientID: rk,
						Name:     "aaaaa",
					})
					//if od, ok := m1.Load(k); ok {
					//	//v := od.(InvokeStruct1)
					//
					//	//v.Name = v.Name + "0"
					//	//m1.Store(k, v)
					//	//fmt.Println("更新key", k, v)
					//} else {
					//	fmt.Println("更新未找到key", k)
					//}

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

	//m1 := dmap.New("b")
	//m1.Store("k0", DInt(1))
	//ki := 1
	t := time.NewTicker(1 * time.Second)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-time.NewTicker(3 * time.Second).C:
				dmapSvc.RangeMap(func(k, v interface{}) bool {
					var m = make(map[string]interface{})
					v.(*Dmap).Range(func(key, value interface{}) bool {
						m[key.(string)] = value
						return true
					})
					fmt.Println("节点b的", k, "的所有元素", len(m))
					return true
				})
			case <-t.C:
				if m, ok := dmapSvc.GetDmap("a"); ok {
					m.Invoke("k0", &InvokeArgs{
						ClientID: 0,
						Name:     "bbbbb",
					})
				}
				//i := rand.Intn(10) + 5
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

				//t.Reset(time.Duration(i) * time.Second)
			case <-time.After(10 * time.Second):
				return
			}
		}
	}()
	wg.Wait()
	fmt.Println("b is end")
}
