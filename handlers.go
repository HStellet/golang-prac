package main
import(
  "github.com/gin-gonic/gin"
  "net/http"
  "fmt"
  "time"
  // "io/ioutil"
  "sync/atomic"
  // "golang.org/x/sync/semaphore"
)

var isLocked int64

var ENVIRONEMNT string
func handler2(c *gin.Context) {
  time.Sleep(10*time.Second)
  c.JSON(http.StatusOK, gin.H{
    "status":http.StatusOK,
    "message": "pong5",
  })
  return
}

func handler3(c *gin.Context) {
  time.Sleep(5*time.Second)
  c.JSON(http.StatusOK, gin.H{
    "status":http.StatusOK,
    "message": "pong10",
  })
  return
}

func handler4(c *gin.Context) {

  c.JSON(http.StatusOK, gin.H{
    "status":http.StatusOK,
    "message": recurs(10),
  })
  return
}
func CheckBusy(c *gin.Context){
    // fmt.Println(isLocked)
    if atomic.LoadInt64(&isLocked)==1{
        fmt.Println("123")
        c.AbortWithStatusJSON(http.StatusOK,gin.H{
          "status":404,
          "message":"Not Found",
        })
    } else{
      atomic.StoreInt64(&isLocked, int64(1))
      c.Next()
      atomic.StoreInt64(&isLocked, int64(0))

    }
}
func recurs(x int) int{
  if(x==1 || x==0){
    return 1
  }else{
    return recurs(x-1)+recurs(x-2)
  }
}
//
//
func handler1(c *gin.Context) {
  c.String(http.StatusOK, "homePage1")
  // execute()
}
// func handler3(c *gin.Context) {
//   name:=c.Query("name")
//   age:=c.Query("age")
//   c.JSON(http.StatusOK, gin.H{
//     "name":name,
//     "age":age,
//   })
// }
// func handler4(c *gin.Context) {
//   c.String(http.StatusOK, "homePage2")
// }
// func handler5(c *gin.Context) {
//   name:=c.Param("name")
//   age:=c.Param("age")
//   c.JSON(http.StatusOK, gin.H{
//     "name":name,
//     "age":age,
//   })func main(){



// }
// func handler6(c *gin.Context) {
//   body:=c.Request.Body
//   value,err:=ioutil.ReadAll(body)
//   if err!=nil{
//     fmt.Println(err.Error())
//   }
//   c.JSON(http.StatusOK, gin.H{
//     "message":string(value),
//   })
// }
func setEnvironment(str string) {

	arr:=[]string{ "dev", "prod", "pp", "prdp", "prod_background"}
	flag:=0
	for _,x:=range arr{
		if x==str{
			flag=1
			break
		}
	}
	if flag==0{
		str="dev"
	}
	ENVIRONEMNT=str
  fmt.Println(ENVIRONEMNT)
}
