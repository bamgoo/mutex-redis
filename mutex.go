package mutex_redis

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/bamgoo/bamgoo"
	"github.com/bamgoo/mutex"
	"github.com/redis/go-redis/v9"
)

type (
	redisDriver struct{}

	redisConnect struct {
		instance *mutex.Instance
		setting  redisSetting
		client   *redis.Client
	}

	redisSetting struct {
		Addr     string
		Username string
		Password string
		Database int
	}
)

func init() {
	bamgoo.Register("redis", &redisDriver{})
}

func (d *redisDriver) Connect(inst *mutex.Instance) (mutex.Connection, error) {
	setting := redisSetting{
		Addr: "127.0.0.1:6379",
	}

	if v, ok := inst.Config.Setting["addr"].(string); ok && v != "" {
		setting.Addr = v
	}
	if v, ok := inst.Config.Setting["server"].(string); ok && v != "" {
		setting.Addr = v
	}
	if v, ok := inst.Config.Setting["username"].(string); ok && v != "" {
		setting.Username = v
	}
	if v, ok := inst.Config.Setting["user"].(string); ok && v != "" {
		setting.Username = v
	}
	if v, ok := inst.Config.Setting["password"].(string); ok {
		setting.Password = v
	}
	if v, ok := inst.Config.Setting["pass"].(string); ok {
		setting.Password = v
	}

	if v, ok := inst.Config.Setting["database"]; ok {
		switch vv := v.(type) {
		case int:
			setting.Database = vv
		case int64:
			setting.Database = int(vv)
		case float64:
			setting.Database = int(vv)
		case string:
			if num, err := strconv.Atoi(vv); err == nil {
				setting.Database = num
			}
		}
	}

	return &redisConnect{instance: inst, setting: setting}, nil
}

func (c *redisConnect) Open() error {
	c.client = redis.NewClient(&redis.Options{
		Addr:     c.setting.Addr,
		Username: c.setting.Username,
		Password: c.setting.Password,
		DB:       c.setting.Database,
	})
	return c.client.Ping(context.Background()).Err()
}

func (c *redisConnect) Close() error {
	if c.client != nil {
		return c.client.Close()
	}
	return nil
}

func (c *redisConnect) Lock(key string, expire time.Duration) error {
	if c.client == nil {
		return errors.New("connection not ready")
	}
	if expire <= 0 {
		expire = c.instance.Config.Expire
	}
	if expire <= 0 {
		expire = time.Second
	}

	ok, err := c.client.SetNX(context.Background(), key, time.Now().UnixNano(), expire).Result()
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("existed")
	}
	return nil
}

func (c *redisConnect) Unlock(key string) error {
	if c.client == nil {
		return errors.New("connection not ready")
	}
	return c.client.Del(context.Background(), key).Err()
}
