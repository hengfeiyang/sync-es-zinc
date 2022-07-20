/* Copyright 2022 Zinc Labs Inc. and Contributors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
 */

package main

import (
	"os"
	"reflect"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
	"github.com/rs/zerolog/log"
)

type config struct {
	ZincHost       string `env:"ZINC_HOST,default=localhost:4080"`
	ZincUser       string `env:"ZINC_USER,default=admin"`
	ZincPassword   string `env:"ZINC_PASSWORD,default=Complexpass#123"`
	ZincIndexName  string `env:"ZINC_INDEX_NAME,default=myindex"`
	ESHost         string `env:"ES_HOST,default=localhost:9200"`
	ESUser         string `env:"ES_USER,default=elastic"`
	ESPassword     string `env:"ES_PASSWORD,default="`
	ESIndexName    string `env:"ES_INDEX_NAME,default=myindex"`
	SyncMaxRecords int    `env:"SYNC_MAX_RECORDS,default=10000"`
	SyncRetries    int    `env:"SYNC_RETRIES,default=3"`
}

var Config = new(config)

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Print(err.Error())
	}
	rv := reflect.ValueOf(Config).Elem()
	loadConfig(rv)
}

func loadConfig(rv reflect.Value) {
	rt := rv.Type()
	for i := 0; i < rt.NumField(); i++ {
		fv := rv.Field(i)
		ft := rt.Field(i)
		if ft.Type.Kind() == reflect.Struct {
			loadConfig(fv)
			continue
		}
		if ft.Tag.Get("env") != "" {
			tag := ft.Tag.Get("env")
			setField(fv, tag)
		}
	}
}

func setField(field reflect.Value, tag string) {
	if tag == "" {
		return
	}
	tagColumn := strings.Split(tag, ",")
	v := os.Getenv(tagColumn[0])
	if v == "" {
		if len(tagColumn) > 1 {
			tv := strings.Join(tagColumn[1:], ",")
			if strings.HasPrefix(tv, "default=") {
				v = tv[8:]
			}
		}
	}
	if v == "" {
		return
	}
	switch field.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		vi, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			log.Fatal().Err(err).Msgf("env %s is not int", tag)
		}
		field.SetInt(int64(vi))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		vi, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			log.Fatal().Err(err).Msgf("env %s is not uint", tag)
		}
		field.SetUint(uint64(vi))
	case reflect.Bool:
		vi, err := strconv.ParseBool(v)
		if err != nil {
			log.Fatal().Err(err).Msgf("env %s is not bool", tag)
		}
		field.SetBool(vi)
	case reflect.String:
		field.SetString(v)
	case reflect.Slice:
		vs := strings.Split(v, ",")
		field.Set(reflect.ValueOf(vs))
		field.SetLen(len(vs))
	}
}
