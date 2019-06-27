// Package config reads a config file in json or yaml and provies
// this to the rest of the packages.
package config

import (
	"fmt"

	"github.com/spf13/viper"
	//"gopkg.in/fsnotify.v1"

	fs "github.com/fsnotify/fsnotify"
)

// Use this directly across packages
// NOT thread-safe
// Access mutli-level config using . notation
// e.g voyager.hystrix.timeout_sec
var Config *viper.Viper

// DoInit to initialise application level config data. Config data is kept in a
// settings folder according to the given environmet. settings folder in turn
// contains the folder according to the environment(dev, pp, prodpp, prod etc).
// In the environment folder json files are having the config info.
// Package uses the viper functionality to parse the key value pair
// in json or yaml.
// to access a key present in deep json level use key notation like :
// 			"voyager.hystrix.TIMEOUT_SEC"
//			"newrelic.NEWRELIC_LICENSE_KEY"dsad
// input params: environment - decided environmdsadasdent of the applcaiton machine.
// 				 (dev, pp, prodpp, prod etc)

func DoInit(environent string) {
	Config = viper.New()
	Config.SetConfigName("config")
	Config.AddConfigPath(fmt.Sprintf("settings/%s", environent))
	if err := Config.ReadInConfig(); err != nil {
		// Handle errors reading the config file
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}
	//TO Watch config; If not this statement, changes will not reflect.
	Config.WatchConfig()
	Config.OnConfigChange(func(e fs.Event) {
		fmt.Println("Config file changed:", e.Name)
	})
}
