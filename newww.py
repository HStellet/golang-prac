package s3

import (
	"bytes"
	"config"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"constants"
	"encoding/csv"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/awsutil"
	"log"
	"logger"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

var sess *session.Session
var svc *s3.S3

func DoInit() {
	config.DoInit("prdp")
	// config.DoInit("/usr/local/goibibo/santa/settings/prdp/")
	sess = session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	regionName := config.Config.GetString("s3_region")
	cfg := aws.NewConfig().WithRegion(regionName)
	//sess, err := session.NewSession(&aws.Config{
	//	Region:      aws.String(config.Config.GetString("s3_region")),
	//	Credentials: credentials.NewSharedCredentials("./aws-credentials", "default"),
	//})
	//if err != nil {
	//	fmt.Println(err.Error())
	//}

	svc = s3.New(sess, cfg)

}

func GetObject(path string) string {

	bucketName := config.Config.GetString("s3_bucket")

	out, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(path),
	})
	var output string
	if err != nil {
		logger.SantaLoggerStats.Error(constants.LogBucket.S3, "GetObject", "error in getting object from S3 ", err.Error(), "", "")
	} else {
		buf := new(bytes.Buffer)
		buf.ReadFrom(out.Body)
		output = buf.String()
		out.Body.Close()
	}
	return output
}

func GetAllFiles() map[string][]byte {
	var fileObj map[string][]byte
	var files []string

	fileObj = make(map[string][]byte)

	bucketName := config.Config.GetString("s3_bucket")
	path := config.Config.GetString("PLB_DATA")

	// The data always gets pushed to one day prior
	tDate := time.Now().Add(-24 * time.Hour).Format("20060102")

	path += "/" + tDate + "/"

	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(path),
	}

	if result, err := svc.ListObjectsV2(input); err == nil {
		for _, content := range result.Contents {
			files = append(files, *content.Key)
		}
	} else {
		logger.SantaLoggerStats.Error(constants.LogBucket.S3, "GetObjectKeys", "error in getting object keys from S3 ", err.Error(), "", "")
	}

	for _, f := range files {
		out, err := svc.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(f),
		})
		if err != nil {
			logger.SantaLoggerStats.Error(constants.LogBucket.S3, "GetObject", "error in getting object from S3 ", err.Error(), "", "")
		} else {
			buf := new(bytes.Buffer)
			buf.ReadFrom(out.Body)
			fileObj[f] = buf.Bytes()
			out.Body.Close()
		}
	}

	return fileObj

}

func GetObjectKeys(bucketName string, prefix string, suffix string) []string {
	var output []string

	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(prefix),
	}

	if result, err := svc.ListObjectsV2(input); err == nil {
		for _, content := range result.Contents {
			if strings.HasSuffix(*content.Key, suffix) {
				output = append(output, *content.Key)
			}
		}
	} else {
		logger.SantaLoggerStats.Error(constants.LogBucket.S3, "GetObjectKeys", "error in getting object keys from S3 ", err.Error(), "", "")
	}

	return output
}

func GetLatestDirectory(bucketName string, prefix string) string {
	dirs := make(map[string]int64)
	manifestSt := make(map[string]int)
	ts := int64(0)
	latestDir := ""

	params := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(prefix),
	}

	if result, err := svc.ListObjectsV2(params); err == nil {
		for _, content := range result.Contents {
			contentSplit := strings.Split(*content.Key, "/")
			dirs[contentSplit[2]] = content.LastModified.Unix()
			// Check if manifest file exists, which ensures completion of unload into S3
			if (contentSplit[len(contentSplit)-1]) == "manifest" {
				manifestSt[contentSplit[2]] = 1
			}
		}
	} else {
		logger.SantaLoggerStats.Error(constants.LogBucket.S3, "GetLatestDirectory", "error in getting object keys from S3 ", err.Error(), "", "")
	}
	if len(dirs) == 0 {
		message := fmt.Sprintf("Zero directories in the S3 bucket %s - %s", bucketName, prefix)
		logger.SantaLoggerStats.Info(constants.LogBucket.S3, "GetLatestDirectory", message, "", "", "")
		return ""
	}

	for dir := range dirs {
		if _, ok := manifestSt[dir]; ok {
			continue
		}
		delete(dirs, dir)
	}

	for k, v := range dirs {
		if ts < v {
			ts = v
			latestDir = k
		}
	}

	return latestDir
}

func GetS3Data(bucketName string, prefix string) []map[string]string {
	dirToRead := GetLatestDirectory(bucketName, prefix)
	prefix = prefix + "/" + dirToRead
	unloadedData := GetS3PathData(bucketName, prefix)
	return unloadedData
}

func GetS3PathData(bucketName, prefix string) []map[string]string {
	unloadedData := make([]map[string]string, 0)
	objectKeys := GetObjectKeys(bucketName, prefix, "")
	for _, fileName := range objectKeys {
		if strings.Contains(fileName, "manifest") {
			continue
		}
		data := GetObject(fileName)
		csvReader := csv.NewReader(strings.NewReader(data))

		records, err := csvReader.ReadAll()
		if err != nil {
			log.Fatal(err)
		}

		if len(records) == 0 {
			return unloadedData
		}

		headers := records[0]
		for index, val := range headers {
			headers[index] = strings.ToLower(val)
		}

		for index, row := range records {
			if index != 0 {
				mapData := make(map[string]string)
				for colIndex, colVal := range row {
					mapData[headers[colIndex]] = colVal
				}
				unloadedData = append(unloadedData, mapData)
			}
		}
	}
	return unloadedData
}

func GetS3PathColumnData(bucketName, prefix string, columnName string) []string {
	unloadedData := make([]string, 0)
	objectKeys := GetObjectKeys(bucketName, prefix, "")
	for _, fileName := range objectKeys {
		if strings.Contains(fileName, "manifest") {
			continue
		}
		data := GetObject(fileName)
		csvReader := csv.NewReader(strings.NewReader(data))

		records, err := csvReader.ReadAll()
		if err != nil {
			log.Fatal(err)
		}

		if len(records) == 0 {
			return unloadedData
		}

		var columnIndex = -1

		for index, val := range records[0] {
			if strings.ToLower(val) == columnName {
				columnIndex = index
				break
			}
		}

		if columnIndex < 0 {
			return unloadedData
		}

		for index, row := range records {
			if index == 0 || len(row) <= columnIndex {
				continue
			}

			unloadedData = append(unloadedData, row[columnIndex])
		}
	}
	return unloadedData
}
func PushingToS3() {
	var fileN string
	var root string = "/usr/local/goibibo/santa/src/goreplay/logs/"
	files, err := FilePathWalkDir(root)
	if err != nil {
		fmt.Println(err.Error())
	}
	for _, file := range files {
		fileN = file
		var str string = "./"
		str += fileN
		onlyFile := filepath.Base(str)

		var str1 string = ""
		var y string
		var m string
		var d string
		var h string
		for i, s := range onlyFile {
			if i == 10 {
				h = str1
				str1 = ""
				break
			}
			if i == 4 {
				y = str1
				str1 = ""
			}
			if i == 6 {
				m = str1
				str1 = ""
			}
			if i == 8 {
				d = str1
				str1 = ""
			}
			str1 += string(s)
		}
		_ = h
		bucket := config.Config.GetString("s3_bucket")

		var filename string = root + onlyFile
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

		path := "/preprod/logs/" + y + "/" + y + m + "/" + y + m + d + "/" + onlyFile

		params := &s3.PutObjectInput{
			Bucket:        aws.String(bucket),
			Key:           aws.String(path),
			Body:          fileBytes,
			ContentLength: aws.Int64(size),
			ContentType:   aws.String(fileType),
		}
		resp, err := svc.PutObject(params)
		if err != nil {
			// handle error
		}
		fmt.Printf("response %s", awsutil.StringValue(resp))
		cmdArgs := []string{root + onlyFile}
		err2 := exec.Command("rm", cmdArgs...).Run()
		if err2 != nil {
			fmt.Println("Failed to execute", err2)
			os.Exit(1)
		}
	}
}
func DownloadFromS3() {
	bucket := config.Config.GetString("s3_bucket")

	t := time.Now()
	y := t.Year()
	m := int(t.Month())
	d := t.Day()
	year := strconv.Itoa(y)
	var day string = ""
	var month string = ""
	if m >= 1 && m <= 9 {
		month += "0"
	}
	month += strconv.Itoa(m)
	if d >= 1 && d <= 9 {
		day += "0"
	}
	day += strconv.Itoa(d)
	item := "preprod/logs/" + year + "/" + year + month + "/" + year + month + day
	fmt.Println(item)
	params := &s3.ListObjectsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(item),
	}
	var fileArr []string
	DoInit()
	resp, _ := svc.ListObjects(params)
	for _, key := range resp.Contents {
		fmt.Println(*key.Key)

		fileArr = append(fileArr, *key.Key)
	}
	sort.Strings(fileArr)
	item = fileArr[len(fileArr)-1]

	fileName := "/usr/local/goibibo/santa/src/goreplay/downloads/"
	fileName += filepath.Base(item)

	file, err := os.Create(fileName)
	if err != nil {
		fmt.Println(err)
	}

	defer file.Close()
	DoInit()
	fmt.Println(sess)
	downloader := s3manager.NewDownloader(sess)
	numBytes, err := downloader.Download(file, &s3.GetObjectInput{
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
