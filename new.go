package main

import (
	"flag"
	"fmt"
	"bytes"
	"time"
	"strconv"
	// "config"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awsutil"
	"sort"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3"
	"net/http"
	"log"
	"os"
	"os/exec"
	"path/filepath"
)

var sess *session.Session
var svc *s3.S3

func main() {
	sess = session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	regionName :=
	cfg := aws.NewConfig().WithRegion(regionName)

	svc = s3.New(sess, cfg)

	fntocall := flag.String("flagname", "x", "it will let you choose the function you want to run")

	env := flag.String("env", "", "it will let you choose the function you want to run")

	flag.Parse()
	fmt.Println(*env)
	mapping := map[string]func(){"rec": execute, "push": pushToS3,"down":DownloadFromS3Bucket,"create":createDir}
	fnname, exists := mapping[*fntocall]

	if exists {
		fnname()
	} else {
		log.Println("wrong flag type")
	}

}
func execute() {
	t:=time.Now()
	y:=t.Year()
	m:=int(t.Month())
	d:=t.Day()
	h:=t.Hour()

	str:="./logs/"
	str+=strconv.Itoa(y)

	if m>=1 && m<=9{
		str+="0"
	}
	str+=strconv.Itoa(m)
	if d>=1 && d<=9{
		str+="0"
	}
	str+=strconv.Itoa(d)
	if h>=0 && h<=9{
		str+="0"
	}
	str+=strconv.Itoa(h)
	str+=".log"
	fmt.Println(str)
	cmdArgs := []string{"../../goreplay/gor","--input-raw", ":9000", "--output-file="+str, "--output-file-queue-limit", "0", "--output-file-max-size-limit", "4k","--output-file-append","--http-allow-url","/ping","--http-allow-url","/hello"}
	x, _ := filepath.Abs(".")
	fmt.Println("starting:", x)
	err := exec.Command("sudo", cmdArgs...).Run()
	if err != nil {
		fmt.Println("Err:", err.Error())
	}

	cmdArgs2 := []string{"chmod","777",str}
	err2 := exec.Command("sudo", cmdArgs2...).Run()
	if err2 != nil {
		fmt.Println("Err:", err2.Error())
	}
	fmt.Println("Command Successfully Executed")
}
func pushToS3(){
	var fileN string
	var root string="./logs/"
	files, err := FilePathWalkDir(root)
 	if err != nil {
  	fmt.Println(err.Error())
 	}
 	for _, file := range files{
		fileN=file
		var str string="./"
		str+=fileN
		onlyFile := filepath.Base(str)

		var str1 string=""
		var y string
		var m string
		var d string
		var h string
		for i,s:=range onlyFile{
			if i==10{
				h=str1
				str1=""
				break
			}
			if i==4{
				y=str1
				str1=""
			}
			if i==6{
				m=str1
				str1=""
			}
			if i==8{
				d=str1
				str1=""
			}
			str1+=string(s)
		}
		_=h
		var bucket string=
		var filename string=str
		file, err := os.Open(filename)
		if err != nil {
			fmt.Println("Failed to open file", filename, err)
			os.Exit(1)
		}
		defer file.Close()
	  fileInfo, _ := file.Stat()

	  size := fileInfo.Size()
		buffer := make([]byte, size) // read file content to buffer

		file.Read(buffer)

		fileBytes := bytes.NewReader(buffer)
		fileType := http.DetectContentType(buffer)

	  path := "/logs/"+y+"/"+y+m+"/"+y+m+d+"/"+onlyFile

	  params := &s3.PutObjectInput{
	    Bucket: aws.String(bucket),
	    Key: aws.String(path),
	    Body: fileBytes,
	    ContentLength: aws.Int64(size),
	    ContentType: aws.String(fileType),
	  }
	  resp, err := svc.PutObject(params)
	  if err != nil {
	    // handle error
	  }
	  fmt.Printf("response %s", awsutil.StringValue(resp))
		cmdArgs := []string{"rm",str}
		x, _ := filepath.Abs(".")
		fmt.Println("starting:", x)
		err2 := exec.Command("sudo", cmdArgs...).Run()
		if err2 != nil {
			fmt.Println("Failed to execute", err2)
			os.Exit(1)
		}
	}
}
func DownloadFromS3Bucket(){
	var bucket string=
	t:=time.Now()
	y:=t.Year()
	m:=int(t.Month())
	d:=t.Day()
	year:=strconv.Itoa(y)
	var day string=""
	var month string=""
	if m>=1 && m<=9{
		month+="0"
	}
	month+=strconv.Itoa(m)
	if d>=1 && d<=9{
		day+="0"
	}
	day+=strconv.Itoa(d)
	item := "logs/"+year+"/"+year+month+"/"+year+month+day
	fmt.Println(item)
	params := &s3.ListObjectsInput {
    Bucket: aws.String(bucket),
    Prefix: aws.String(item),
	}
	var fileArr []string
	resp, _ := svc.ListObjects(params)
	for _, key := range resp.Contents {
    fileArr=append(fileArr,*key.Key)
	}
	sort.Strings(fileArr)
	item=fileArr[len(fileArr)-1]
	fileName := "./logs/"+filepath.Base(item)

	file, err := os.Create(fileName)
	if err != nil {
			fmt.Println(err)
	}

	defer file.Close()
	fmt.Println(item,"xyz")
	downloader := s3manager.NewDownloader(sess)
	numBytes, err := downloader.Download(file,&s3.GetObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(item),
			})
	if err != nil {
			fmt.Println("Err:", err.Error())
	}
	fmt.Println("Downloaded", file.Name(), numBytes, "bytes")

}


func FilePathWalkDir(root string) ([]string, error) {
 var files []string
 err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
  if !info.IsDir() {
   files = append(files, path)
  }
  return nil
 })
 return files, err
}
func createDir(){
	os.MkdirAll("/home/hardik/goProject/views/logs", os.ModePerm)
}
