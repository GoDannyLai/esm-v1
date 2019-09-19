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
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	log "github.com/cihub/seelog"
	"gopkg.in/cheggaaa/pb.v1"
)

func checkFileIsExist(filename string) bool {
	var exist = true
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		exist = false
	}
	return exist
}

func (m *Migrator) NewFileReadWorker(pb *pb.ProgressBar, wg *sync.WaitGroup) {
	log.Debug("start reading file")
	f, err := os.Open(m.Config.DumpInputFile)
	if err != nil {
		log.Error(err)
		return
	}

	defer f.Close()
	r := bufio.NewReader(f)
	lineCount := 0
	for {
		line, err := r.ReadString('\n')
		if io.EOF == err || nil != err {
			break
		}
		lineCount += 1
		js := map[string]interface{}{}

		//log.Trace("reading file,",lineCount,",", line)
		err = json.Unmarshal([]byte(line), &js)
		if err != nil {
			log.Error(err)
			continue
		}
		m.DocChan <- js
		pb.Increment()
	}

	defer f.Close()
	log.Debug("end reading file")
	close(m.DocChan)
	wg.Done()
}

func (c *Migrator) NewFileDumpWorker(pb *pb.ProgressBar, wg *sync.WaitGroup) {
	var f *os.File
	var err1 error

	if checkFileIsExist(c.Config.DumpOutFile) {
		f, err1 = os.OpenFile(c.Config.DumpOutFile, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
		if err1 != nil {
			log.Error(err1)
			return
		}

	} else {
		f, err1 = os.Create(c.Config.DumpOutFile)
		if err1 != nil {
			log.Error(err1)
			return
		}
	}

	w := bufio.NewWriter(f)

READ_DOCS:
	for {
		docI, open := <-c.DocChan

		// this check is in case the document is an error with scroll stuff
		if status, ok := docI["status"]; ok {
			if status.(int) == 404 {
				log.Error("error: ", docI["response"])
				continue
			}
		}

		// sanity check
		for _, key := range []string{"_index", "_type", "_source", "_id"} {
			if _, ok := docI[key]; !ok {
				//json,_:=json.Marshal(docI)
				//log.Errorf("failed parsing document: %v", string(json))
				break READ_DOCS
			}
		}

		jsr, err := json.Marshal(docI)
		log.Trace(string(jsr))
		if err != nil {
			log.Error(err)
		}
		n, err := w.WriteString(string(jsr))
		if err != nil {
			log.Error(n, err)
		}
		w.WriteString("\n")
		pb.Increment()

		// if channel is closed flush and gtfo
		if !open {
			goto WORKER_DONE
		}
	}

WORKER_DONE:
	w.Flush()
	f.Close()

	if !c.Config.UnGzip {
		outMsg, errMsg, errM := GzipFile(c.Config.DumpOutFile, c.Config.GzipTimeout)
		if errM != nil {
			log.Errorf("error to gzip %s: %s %s\n\t%s", c.Config.DumpOutFile, errM, errMsg, outMsg)
		} else {
			log.Warnf("successfully gzip %s: %s\n\t%s", c.Config.DumpOutFile, errMsg, outMsg)
		}
	}

	wg.Done()
	log.Debug("file dump finished")
}

func (c *Migrator) NewFileDumpWorkerSplit(pb *pb.ProgressBar, wg *sync.WaitGroup) {

	var (
		f           *os.File
		err1        error
		idx         int    = 0
		fndir       string = filepath.Dir(c.Config.DumpOutFile)
		fnbase      string
		fnsuffix    string
		fn          string
		sizeCurrent int = 0
		sizeMax     int = c.Config.SplitSize * 1024 * 1024
		w           *bufio.Writer
		errMsg      string
		outMsg      string
		errM        error
	)
	tmp := filepath.Base(c.Config.DumpOutFile)
	tmpArr := strings.Split(tmp, ".")
	if len(tmpArr) > 1 {
		fnsuffix = tmpArr[len(tmpArr)-1]
	} else {
		fnsuffix = ""
	}
	if len(tmpArr) > 2 {
		fnbase = strings.Join(tmpArr[:len(tmpArr)-1], ".")
	} else {
		fnbase = tmpArr[0]
	}

READ_DOCS:
	for {
		if sizeCurrent == 0 {
			// new file
			idx++
			if fnsuffix == "" {
				fn = filepath.Join(fndir, fmt.Sprintf("%s.split.%d", fnbase, idx))
			} else {
				fn = filepath.Join(fndir, fmt.Sprintf("%s.split.%d.%s", fnbase, idx, fnsuffix))
			}
			if checkFileIsExist(fn) {
				f, err1 = os.OpenFile(fn, os.O_APPEND|os.O_WRONLY|os.O_TRUNC, os.ModeAppend)
				if err1 != nil {
					log.Error(err1)
					return
				}

			} else {
				f, err1 = os.Create(fn)
				if err1 != nil {
					log.Error(err1)
					return
				}
			}
			w = bufio.NewWriter(f)
			log.Infof("writting to file %s", fn)

		}
		docI, open := <-c.DocChan

		// this check is in case the document is an error with scroll stuff
		if status, ok := docI["status"]; ok {
			if status.(int) == 404 {
				log.Error("error: ", docI["response"])
				continue
			}
		}

		// sanity check
		for _, key := range []string{"_index", "_type", "_source", "_id"} {
			if _, ok := docI[key]; !ok {
				//json,_:=json.Marshal(docI)
				//log.Errorf("failed parsing document: %v", string(json))
				break READ_DOCS
			}
		}

		jsr, err := json.Marshal(docI)
		log.Trace(string(jsr))
		if err != nil {
			log.Error(err)
		}
		n, err := w.WriteString(string(jsr))
		if err != nil {
			log.Error(n, err)
		}
		w.WriteString("\n")
		pb.Increment()
		sizeCurrent += n

		if sizeCurrent >= sizeMax {
			w.Flush()
			f.Close()
			w = nil
			f = nil
			sizeCurrent = 0
			if !c.Config.UnGzip {
				outMsg, errMsg, errM = GzipFile(fn, c.Config.GzipTimeout)
				if errM != nil {
					log.Errorf("error to gzip %s: %s %s\n\t%s", fn, errM, errMsg, outMsg)
				} else {
					log.Warnf("successfully gzip %s: %s\n\t%s", fn, errMsg, outMsg)
				}
			}
		}

		// if channel is closed flush and gtfo
		if !open {
			break
		}
	}

	if w != nil {
		w.Flush()
	}
	if f != nil {
		f.Close()
		if !c.Config.UnGzip {
			outMsg, errMsg, errM = GzipFile(fn, c.Config.GzipTimeout)
			if errM != nil {
				log.Errorf("error to gzip %s: %s %s\n\t%s", fn, errM, errMsg, outMsg)
			} else {
				log.Warnf("successfully gzip %s: %s\n\t%s", fn, errMsg, outMsg)
			}
		}
	}

	wg.Done()
	log.Debug("file dump finished")
}

func GzipFile(fileFull string, timeout int) (string, string, error) {
	var (
		out    bytes.Buffer
		errout bytes.Buffer
		err    error
	)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "gzip", fileFull)
	cmd.Stdout = &out
	cmd.Stderr = &errout

	err = cmd.Run()

	if err != nil {
		return strings.TrimSpace(out.String()), strings.TrimSpace(errout.String()), err
	} else {
		return strings.TrimSpace(out.String()), strings.TrimSpace(errout.String()), nil
	}
}
