/*
Copyright 2016 Medcl (m AT medcl.net)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	log "github.com/cihub/seelog"
)

type ESAPIV7 struct {
	ESAPIV5
}

func (s *ESAPIV7) NewScroll(indexNames string, scrollTime string, docBufferCount int, query string, match string, slicedId, maxSlicedCount int, fields string) (scroll interface{}, err error) {
	url := fmt.Sprintf("%s/%s/_search?scroll=%s&size=%d", s.Host, indexNames, scrollTime, docBufferCount)

	jsonBody := ""
	if len(query) > 0 || maxSlicedCount > 0 || len(fields) > 0 {
		queryBody := map[string]interface{}{}

		if len(fields) > 0 {
			if !strings.Contains(fields, ",") {
				log.Error("The fields shoud be seraprated by ,")
				return nil, errors.New("")
			} else {
				queryBody["_source"] = strings.Split(fields, ",")
			}
		}

		if len(query) > 0 {
			queryBody["query"] = map[string]interface{}{}
			queryBody["query"].(map[string]interface{})["query_string"] = map[string]interface{}{}
			queryBody["query"].(map[string]interface{})["query_string"].(map[string]interface{})["query"] = query
		}

		if maxSlicedCount > 1 {
			log.Tracef("sliced scroll, %d of %d", slicedId, maxSlicedCount)
			queryBody["slice"] = map[string]interface{}{}
			queryBody["slice"].(map[string]interface{})["id"] = slicedId
			queryBody["slice"].(map[string]interface{})["max"] = maxSlicedCount
		}

		jsonArray, err := json.Marshal(queryBody)
		if err != nil {
			log.Error(err)

		} else {
			jsonBody = string(jsonArray)
		}
	}

	//by danny
	if len(match) > 0 {
		log.Warn("-Q is set, ignore -q and --fields")
		if maxSlicedCount > 1 {
			var tmpMap map[string]interface{}
			err := json.Unmarshal([]byte(match), &tmpMap)
			if err != nil {
				log.Errorf("error to unmarshal query body set by -M: %s", err)
				return nil, err
			}
			log.Tracef("add to -M query body, sliced scroll, %d of %d", slicedId, maxSlicedCount)
			tmpMap["slice"] = map[string]interface{}{}
			tmpMap["slice"].(map[string]interface{})["id"] = slicedId
			tmpMap["slice"].(map[string]interface{})["max"] = maxSlicedCount
			tmpJarr, err := json.Marshal(tmpMap)
			if err != nil {
				log.Errorf("error to marshal query body by -M and slice scroll: %s", err)
				return nil, err
			}
			jsonBody = string(tmpJarr)

		} else {
			jsonBody = match
		}

	}
	log.Warnf("url: %s, query body: %s", url, jsonBody)

	resp, body, errs := Post(url, s.Auth, jsonBody, s.HttpProxy)

	if resp != nil && resp.Body != nil {
		io.Copy(ioutil.Discard, resp.Body)
		defer resp.Body.Close()
	}

	if errs != nil {
		log.Error(errs)
		return nil, errs[0]
	}

	if resp.StatusCode != 200 {
		return nil, errors.New(body)
	}

	log.Trace("new scroll,", body)

	if err != nil {
		log.Error(err)
		return nil, err
	}

	scroll = &ScrollV7{}
	err = json.Unmarshal([]byte(body), scroll)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	return scroll, err
}

func (s *ESAPIV7) NextScroll(scrollTime string, scrollId string) (interface{}, error) {
	id := bytes.NewBufferString(scrollId)

	url := fmt.Sprintf("%s/_search/scroll?scroll=%s&scroll_id=%s", s.Host, scrollTime, id)
	resp, body, errs := Get(url, s.Auth, s.HttpProxy)

	if resp != nil && resp.Body != nil {
		io.Copy(ioutil.Discard, resp.Body)
		defer resp.Body.Close()
	}

	if errs != nil {
		log.Error(errs)
		return nil, errs[0]
	}

	if resp.StatusCode != 200 {
		return nil, errors.New(body)
	}

	// decode elasticsearch scroll response
	scroll := &ScrollV7{}
	err := json.Unmarshal([]byte(body), &scroll)
	if err != nil {
		log.Error(body)
		log.Error(err)
		return nil, err
	}

	return scroll, nil
}
