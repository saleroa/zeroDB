package config

import (
	"log"
	"os"

	"gopkg.in/yaml.v2"
)

// 开启服务的配置项
type Config struct {
	Addr         string ` yaml:"addr"` // 服务监听地址
	DirPath      string ` yaml:"dir_path"`
	BlockSize    int64  // 存放dbfile的位置
	MaxKeySize   uint32 ` yaml:"max_key_size"`
	MaxValueSize uint32 ` yaml:"max_value_size"`

	//是否将写入从操作系统缓冲区缓存同步到实际磁盘。如果为 false，系统崩溃，会丢失最近的一些写入
	Sync             bool `yaml:"sync"`
	ReclaimThreshold int  `yaml:"reclaim_threshold"` // threshold to reclaim disk
}

func InitConfig(path string) (cfg Config) {
	// read yaml
	yamlFile, err := os.ReadFile(path)

	if err != nil {
		log.Fatalf("Error reading YAML file: %v", err)
		return
	}

	// unmarshall YAML to config
	err = yaml.Unmarshal(yamlFile, &cfg)
	if err != nil {
		log.Fatalf("Error unmarshalling YAML: %v", err)
		return
	}
	return
}
