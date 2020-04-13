package main

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"github.com/golang/glog"
	"golang.org/x/net/html"
	"golang.org/x/net/html/atom"
	"golang.org/x/sync/semaphore"
	"golang.org/x/text/encoding/charmap"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode"
)

const vkUserAPI = "userapi.com"
const vkURL = "vk.com"

var limit int64 = 8 * 10
var sem = semaphore.NewWeighted(limit)

func (e *extractor) d(url, name string) (err error) {
	r, err := http.Get(url)
	if err != nil {
		glog.Error(err)
		return
	}
	defer r.Body.Close()
	err = e.writeToZIP(name, r.Body)
	if err != nil {
		glog.Error(err)
		return
	}
	return
}

func (e *extractor) writeToZIP(name string, reader io.Reader) (err error) {
	e.writerMutex.Lock()
	defer e.writerMutex.Unlock()
	file, err := e.writerZIP.Create(name)
	if err != nil {
		glog.Error(err)
		return
	}
	_, err = io.Copy(file, reader)
	if err != nil {
		glog.Error(err)
	}
	return
}

type job struct {
	url, name, src string
}

func (e *extractor) parseHTML(buff []byte, jobs chan job) (overwrite []byte, err error) {

	name, err := getName(buff)
	if err != nil {
		glog.Error(err)
		return
	}

	doc, err := html.Parse(bytes.NewReader(buff))
	if err != nil {
		glog.Error(err)
		return
	}

	var f func(*html.Node)
	f = func(n *html.Node) {
		if n.Type == html.ElementNode {
			for _, a := range n.Attr {
				if a.Key == "class" {
					if a.Val == "attachment__link" {
						j := getJob(n, name)
						jobs <- j
						replaceNode := html.Node{
							Parent:      n.Parent,
							FirstChild:  nil,
							LastChild:   nil,
							PrevSibling: n.NextSibling,
							NextSibling: n.NextSibling,
							Data:        "img",
							DataAtom:    atom.Img,
							Type:        n.Type,
							Namespace:   n.Namespace,
							Attr: []html.Attribute{
								{Key: "src", Val: j.src},
								{Key: "alt", Val: j.url},
							},
						}
						*n = replaceNode
						return
					}
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			f(c)
		}
	}
	f(doc)

	w := bytes.NewBuffer([]byte{})
	err = html.Render(w, doc)
	if err != nil {
		glog.Error(err)
		return
	}
	return w.Bytes(), err
}

func getJob(n *html.Node, name string) (j job) {
	for _, a := range n.Attr {
		if a.Key == "href" {
			dir := "originals"
			if !strings.Contains(a.Key, ".jpg") {
				dir = "files"
			}
			j.url = a.Val
			vals := strings.Split(a.Val, "/")
			j.name = dir + "/" + name + "/" + vals[len(vals)-1]
			obj := vals[len(vals)-1]
			j.src = "../../" + dir + "/" + url.PathEscape(name) + "/" + url.PathEscape(obj)
			return j
		}
	}
	return
}

func getName(buff []byte) (name string, err error) {
	doc, err := html.Parse(bytes.NewReader(buff))
	if err != nil {
		glog.Error(err)
		return
	}

	var f func(*html.Node)
	f = func(n *html.Node) {
		if n.Type == html.ElementNode {
			for _, a := range n.Attr {
				if a.Key == "class" {
					if a.Val == "ui_crumb" && n.Data == "div" {
						if isASCII(n.FirstChild.Data) {
							name = n.FirstChild.Data
						} else {
							name = string(DecodeWindows1251([]byte(n.FirstChild.Data)))
						}
						return
					}
				}
			}
		}
		// заканчиваем перебор если имя найдено
		for c := n.FirstChild; c != nil && name == ""; c = c.NextSibling {
			f(c)
		}
	}
	f(doc)
	return
}

func DecodeWindows1251(ba []uint8) []uint8 {
	dec := charmap.Windows1251.NewDecoder()
	out, err := dec.Bytes(ba)
	if err != nil {
		glog.Error(err)
		return nil
	}

	return out
}

func isASCII(s string) bool {
	for _, c := range s {
		if c > unicode.MaxASCII {
			return false
		}
	}
	return true
}

func (e *extractor) openZIPREADER() (err error) {
	e.readerFile, err = os.Open(e.archive)
	if err != nil {
		glog.Error(err)
		return
	}
	inf, err := e.readerFile.Stat()
	if err != nil {
		glog.Error(err)
		return
	}
	e.readerZIP, err = zip.NewReader(e.readerFile, inf.Size())
	if err != nil {
		glog.Error(err)
		return
	}
	return
}

func (e *extractor) openZIPWriter() (err error) {
	e.writerFile, err = os.OpenFile(e.archive+".full.zip",
		os.O_TRUNC|os.O_WRONLY|os.O_CREATE,
		os.ModePerm)
	if err != nil {
		glog.Error(err)
		return
	}
	e.writerZIP = zip.NewWriter(e.writerFile)
	if err != nil {
		glog.Error(err)
		return
	}
	return
}

type extractor struct {
	archive string

	readerZIP  *zip.Reader
	readerFile *os.File

	writerZIP   *zip.Writer
	writerFile  *os.File
	writerMutex sync.Mutex
}

func (e *extractor) run(ctx context.Context, done chan error) {
	var err error
	defer func() {
		if r := recover(); r != nil {
			glog.Error(err)
			err = fmt.Errorf("случилась паника из-за %s; первичная ошибка, если была: %s", r, err)
		}
		done <- err
	}()

	err = e.openZIPREADER()
	if err != nil {
		glog.Error(err)
		return
	}
	defer e.readerFile.Close()

	err = e.openZIPWriter()
	if err != nil {
		glog.Error(err)
		return
	}
	defer e.writerFile.Close()
	defer func() {
		glog.Info("CLOSE IT")
		e.writerZIP.Close()
	}()

	jobs := make(chan job, 1000000)
	doneList := make(chan struct{})
	go func() {
		for index := range e.readerZIP.File {
			name := e.readerZIP.File[index].Name
			r, err := e.readerZIP.File[index].Open()
			if err != nil {
				glog.Error(err)
				return
			}
			buff, err := ioutil.ReadAll(r)
			if err != nil {
				glog.Error(err)
				return
			}
			r.Close()

			if strings.Contains(name, "messages") && path.Ext(name) == ".html" {
				buff, err = e.parseHTML(buff, jobs)
				if err != nil {
					glog.Error(err)
					return
				}
			}
			err = e.writeToZIP(name, bytes.NewReader(buff))
			if err != nil {
				glog.Error(err)
				return
			}
		}
		close(doneList)
		close(jobs)
	}()
	wg := sync.WaitGroup{}
	doneGetList := make(chan struct{})
	go func() {
		for jb := range jobs {
			if !(strings.Contains(jb.url, vkUserAPI) || strings.Contains(jb.url, vkURL)) {
				continue
			}
			err := sem.Acquire(ctx, 1)
			if err != nil {
				glog.Error(err)
				return
			}
			wg.Add(1)
			go func(j job) {
				defer wg.Done()
				defer sem.Release(1)
				handlerErr := e.d(j.url, j.name)
				if handlerErr != nil {
					glog.Error(handlerErr, j.url)
				}
				//if current := atomic.AddInt64(&i, 1); current%10000 == 0 {
				//	mStart.Lock()
				//	total := time.Now().Sub(start).String()
				//	period := time.Now().Sub(lastBatchTime).String()
				//	lastBatchTime = time.Now()
				//	println(current, atomic.LoadInt64(&count), element, total, period)
				//	mStart.Unlock()
				//}
			}(jb)
		}
		close(doneGetList)
	}()

	<-doneList
	<-doneGetList
	wg.Wait()
	return

	glog.Info("WAIT FOR END")
}
func (e *extractor) _clean(ctx context.Context) {

}
func (e *extractor) clean() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*6)
	go func() {
		for i := 5; i != 0; i-- {
			glog.Info("Завершение программы. Очистка закончится через %d", i)
			time.Sleep(time.Second)
		}
		cancel()
	}()
	e._clean(ctx)
	glog.Error("очистка не выполнена. функция не закончена")
}

func main() {

	world, armageddon := context.WithCancel(context.Background())

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		glog.Infof("Получен сигнал %s; Окончание всех операций", sig.String())
		armageddon()
	}()

	done := make(chan error)

	var e = extractor{
		archive: os.Args[1],
	}

	go e.run(world, done)

	select {
	case <-world.Done():
		if world.Err() != nil {
			e.clean()
		}
	case err := <-done:
		if err != nil {
			e.clean()
		}
	}

}
