package main
import "fmt"

func main(){
	arr1 := [3]int{1, 2, 3}
	fmt.Println(arr1)
	var a, b int = 1, 2
	fmt.Println(a, b)
	fmt.Println("hello world")

	const s string = "constant"
	fmt.Println(s)

	if 7%2 == 0{
		fmt.Println("7 is even")
	} else {
		fmt.Println("7 is odd")
	}

	if num:=9; num<0 {
		fmt.Println(num, "is negative")
	} else{
		fmt.Println(num, "is positive")
	}
	// array
	aa := [5]int{1, 2, 3, 4, 5}
	fmt.Println(aa)
	var bb [5]int
	bb[0] = 1
	fmt.Println(bb, len(bb))
	//slice
	c := make([]string, 3)
	c[0] = "hello"
	c = append(c, "dd")
	fmt.Println(c)
	fmt.Println(c[:2])
	fmt.Println(c[3:])
	fmt.Println(len(c))
	twoD := make([][]int, 3)
	for i:=0; i<3; i++{
		innerLen := i + 1
		twoD[i] = make([]int, innerLen)
		for j:=0; j<innerLen; j++{
			twoD[i][j] = i + j
		}
	}
	// map
	m := make(map[string]int)
	m["k1"] = 7
	m["k2"] = 13
	fmt.Println("map:", m)
	delete(m, "k2")
	_, prs := m["k2"] // 第一个返回值（如果不存在为0或""），第二个返回是否存在
	fmt.Println("prs:", prs)

	n := map[string]int{"foo":1, "bar":2, "a":3}
	delete(n, "a")
	fmt.Println(n)

	// range
	nums := []int{2, 3, 4}
	sum := 0
	for _, num := range nums{
		sum += num
	}
	fmt.Println("sum of map:", sum)
	mp := map[int]int{1:1, 2:4}
	for key, val := range mp{
		fmt.Println(key, val)
	}

}