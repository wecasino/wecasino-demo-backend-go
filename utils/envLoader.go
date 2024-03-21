package utils

import (
	"os"
	"strconv"
	"strings"

	"github.com/go-jose/go-jose/v3"
	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"
)

type EnvLoader struct {
	Logger *logrus.Logger
	Prefix string
}

func StanderLoader() *EnvLoader {
	return NewPrefixEnvLoader("", logrus.StandardLogger())
}

func NewEnvLoader(logger *logrus.Logger) *EnvLoader {
	return NewPrefixEnvLoader("", logger)
}

func NewPrefixEnvLoader(prefix string, logger *logrus.Logger) *EnvLoader {
	err := godotenv.Load()
	if err != nil {
		println("[ENV] godotenv find no .env file and use system only.")
	}
	return &EnvLoader{
		Logger: logger,
		Prefix: strings.TrimSpace(prefix),
	}
}

func (ldr *EnvLoader) LoadEnv(name string) string {
	env := strings.TrimSpace(os.Getenv(ldr.Prefix + name))
	ldr.Logger.Infof("[ENV] load env: %v => [%v]", name, env)
	return env
}

func (ldr *EnvLoader) LoadTrue(name string) bool {
	env := strings.TrimSpace(ldr.LoadEnv(name))
	return env == "true" || env == "True" || env == "TRUE" || env == "T" || env == "1"
}

func (ldr *EnvLoader) LoadEnvMustNotEmpty(name string) string {
	env := ldr.LoadEnv(name)
	if env == "" {
		ldr.Logger.Panicf("[ENV] fail to load env: %v", name)
	}
	return env
}

func (ldr *EnvLoader) LoadEnvOrDefault(name, def string) string {
	env := strings.TrimSpace(os.Getenv(ldr.Prefix + name))
	if env == "" {
		ldr.Logger.Infof("[ENV] load env: %v => [%v] Use deault: %v", name, env, def)
		return def
	} else {
		ldr.Logger.Infof("[ENV] load env: %v => [%v]", name, env)
		return env
	}
}

func (ldr *EnvLoader) LoadEnvOrDefaultInt(name string, def int64) int64 {
	env := ldr.LoadEnvOrDefault(name, strconv.FormatInt(def, 10))
	n, err := strconv.ParseInt(env, 10, 64)
	if err != nil {
		ldr.Logger.Infof("[ENV] convert to int with error: %v Use deault: %v", err.Error(), def)
		return def
	}
	return n
}

func (ldr *EnvLoader) LoadJWKMustNotEmpty(name string) *jose.JSONWebKey {
	env := ldr.LoadEnvMustNotEmpty(name)
	_jwk := &jose.JSONWebKey{}
	err := _jwk.UnmarshalJSON([]byte(env))
	if err != nil {
		panic(err)
	}
	return _jwk
}
