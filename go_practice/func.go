package main

import "fmt"

func plus(a int, b int) int{
	return a + b
}

func sum(nums ...int){
	fmt.Print(nums, " ")
	total := 0
	for _, num := range nums{
		total += num
	}
	fmt.Println(total)
}

func main(){
	a, b := 1, 2
	res := plus(a, b)
	fmt.Println("plus result:", res)
}