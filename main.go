package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"io"
	"io/ioutil"
	"net/http"
	"sync"
	"time"
	"errors"
	"os"
)

const (
	measurementsURL = "https://rald-dev.greenbeep.com/api/v1/measurements"
	topic           = "measurements"
	defaultGmapsKey = "xyz"

	redisAddr   = "localhost:6379"
	redisPasswd = ""
	redisDB     = 0
)

var (
	measurementCache = make(map[string]Measurement)
	rdb              *redis.Client
	gmapsKey         string
	rwMU             = sync.RWMutex{}
)

type Measurement struct {
	Sensor    string    `json:"sensor"`
	Source    string    `json:"source"`
	PM1Dot0   float64   `json:"pm1dot0"`
	PM2Dot5   float64   `json:"pm2dot5"`
	PM10      float64   `json:"pm10"`
	Latitude  float64   `json:"latitude"`
	Longitude float64   `json:"longitude"`
	Recorded  time.Time `json:"recorded"`
}

func init() {
	rdb = newRedisClient()
	if k := os.Getenv("GMAPS_KEY"); k != "" {
		gmapsKey = k
		return
	}
	gmapsKey = defaultGmapsKey
}

func newRedisClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: redisPasswd,
		DB:       redisDB,
	})
}

func fetchMeasurements() error {
	res, err := http.Get(measurementsURL)
	if err != nil {
		return err
	}
	if res.Body == nil {
		return errors.New("nil body")
	}
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return err
	}
	var measurements []Measurement
	err = json.Unmarshal(body, &measurements)
	if err != nil {
		return err
	}
	rwMU.Lock()
	defer rwMU.Unlock()
	for _, v := range measurements {
		measurement, exists := measurementCache[v.Source]
		if !exists {
			fmt.Printf("No data for source '%s', creating key.\n", v.Source)
			measurementCache[v.Source] = v
			continue
		}
		if v.Recorded.Equal(measurement.Recorded) || v.Recorded.Before(measurement.Recorded) {
			continue
		}
		if v.Recorded.After(measurement.Recorded) {
			fmt.Printf("New data for source '%s'\n", v.Source)
			measurementCache[v.Source] = v
			measurementJSON, _ := json.Marshal(&v)
			rdb.Publish(context.Background(), topic, string(measurementJSON))
		}
	}
	return nil
}

func main() {
	go func() {
		for {
			err := fetchMeasurements()
			if err != nil {
				fmt.Printf("Couldn't fetch measurements: %s\n", err.Error())
				continue
			}
			time.Sleep(time.Millisecond * 500)
		}
	}()
	router := gin.Default()
	router.LoadHTMLGlob("templates/*")
	//router.LoadHTMLFiles("templates/template1.html", "templates/template2.html")
	router.GET("/measurements", func(c *gin.Context) {
		rwMU.RLock()
		defer rwMU.RUnlock()
		measurementsJSON, err := json.Marshal(&measurementCache)
		if err != nil {
			fmt.Printf("Couldn't marshal data: %s\n", err.Error())
			c.AbortWithError(500, err)
			return
		}
		c.Data(200, "application/json", measurementsJSON)
	})
	router.GET("/stream", func(c *gin.Context) {
		reqRedis := newRedisClient()
		defer reqRedis.Close()
		pubsub := rdb.Subscribe(c, topic)
		c.Stream(func(w io.Writer) bool {
			for {
				input, err := pubsub.Receive(c)
				if err != nil {
					fmt.Println("Error receiving: ", err)
					continue
				}
				switch input.(type) {
				case *redis.Message:
					msg := input.(*redis.Message)
					s := []byte(msg.Payload)
					c.SSEvent("message", s)
					return true
				}
			}
			return false
		})
	})
	router.GET("/", func(c *gin.Context) {
		c.HTML(http.StatusOK, "index.tmpl", gin.H{
			"gmapsKey": gmapsKey,
		})
	})
	router.Run(":8080")
}
