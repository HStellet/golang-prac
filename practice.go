package main

import "github.com/gin-gonic/gin"
func main() {
	r:=gin.Default()
	r.GET("/",handler4)
	r.GET("/ping",handler4)
	r.GET("/heya",handler4)
	r.GET("/ping2",handler4)
	r.GET("/hello",handler4)

	setEnvironment("x")
	r.Run(":9000")
}
