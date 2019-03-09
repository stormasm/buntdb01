package buntdb

import (
	"bytes"
//"errors"
	"fmt"
//"io/ioutil"
	"math/rand"
	"os"
//"strconv"
	"strings"
//"sync"
	"testing"
	"time"
)

func testOpen(t testing.TB) *DB {
	if err := os.RemoveAll("data.db"); err != nil {
		t.Fatal(err)
	}
	return testReOpen(t, nil)
}
func testReOpen(t testing.TB, db *DB) *DB {
	return testReOpenDelay(t, db, 0)
}

func testReOpenDelay(t testing.TB, db *DB, dur time.Duration) *DB {
	if db != nil {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(dur)
	db, err := Open("data.db")
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func testClose(db *DB) {
	_ = db.Close()
	_ = os.RemoveAll("data.db")
}

func TestBasic(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	db := testOpen(t)
	defer testClose(db)

	// create a simple index
	if err := db.CreateIndex("users", "fun:user:*", IndexString); err != nil {
		t.Fatal(err)
	}

	// create a spatial index
	if err := db.CreateSpatialIndex("rects", "rect:*", IndexRect); err != nil {
		t.Fatal(err)
	}
	if true {
		err := db.Update(func(tx *Tx) error {
			if _, _, err := tx.Set("fun:user:0", "tom", nil); err != nil {
				return err
			}
			if _, _, err := tx.Set("fun:user:1", "Randi", nil); err != nil {
				return err
			}
			if _, _, err := tx.Set("fun:user:2", "jane", nil); err != nil {
				return err
			}
			if _, _, err := tx.Set("fun:user:4", "Janet", nil); err != nil {
				return err
			}
			if _, _, err := tx.Set("fun:user:5", "Paula", nil); err != nil {
				return err
			}
			if _, _, err := tx.Set("fun:user:6", "peter", nil); err != nil {
				return err
			}
			if _, _, err := tx.Set("fun:user:7", "Terri", nil); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		// add some random items
		start := time.Now()
		if err := db.Update(func(tx *Tx) error {
			for _, i := range rand.Perm(100) {
				if _, _, err := tx.Set(fmt.Sprintf("tag:%d", i+100), fmt.Sprintf("val:%d", rand.Int()%100+100), nil); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if false {
			println(time.Now().Sub(start).String(), db.keys.Len())
		}
		// add some random rects
		if err := db.Update(func(tx *Tx) error {
			if _, _, err := tx.Set("rect:1", Rect([]float64{10, 10}, []float64{20, 20}), nil); err != nil {
				return err
			}
			if _, _, err := tx.Set("rect:2", Rect([]float64{15, 15}, []float64{24, 24}), nil); err != nil {
				return err
			}
			if _, _, err := tx.Set("rect:3", Rect([]float64{17, 17}, []float64{27, 27}), nil); err != nil {
				return err
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	// verify the data has been created
	buf := &bytes.Buffer{}
	err := db.View(func(tx *Tx) error {
		err := tx.Ascend("users", func(key, val string) bool {
			fmt.Fprintf(buf, "%s %s\n", key, val)
			return true
		})
		if err != nil {
			t.Fatal(err)
		}
		err = tx.AscendRange("", "tag:170", "tag:172", func(key, val string) bool {
			fmt.Fprintf(buf, "%s\n", key)
			return true
		})
		if err != nil {
			t.Fatal(err)
		}
		err = tx.AscendGreaterOrEqual("", "tag:195", func(key, val string) bool {
			fmt.Fprintf(buf, "%s\n", key)
			return true
		})
		if err != nil {
			t.Fatal(err)
		}
		err = tx.AscendGreaterOrEqual("", "rect:", func(key, val string) bool {
			if !strings.HasPrefix(key, "rect:") {
				return false
			}
			min, max := IndexRect(val)
			fmt.Fprintf(buf, "%s: %v,%v\n", key, min, max)
			return true
		})
		expect := make([]string, 2)
		n := 0
		err = tx.Intersects("rects", "[0 0],[15 15]", func(key, val string) bool {
			if n == 2 {
				t.Fatalf("too many rects where received, expecting only two")
			}
			min, max := IndexRect(val)
			s := fmt.Sprintf("%s: %v,%v\n", key, min, max)
			if key == "rect:1" {
				expect[0] = s
			} else if key == "rect:2" {
				expect[1] = s
			}
			n++
			return true
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range expect {
			if _, err := buf.WriteString(s); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	res := `
fun:user:2 jane
fun:user:4 Janet
fun:user:5 Paula
fun:user:6 peter
fun:user:1 Randi
fun:user:7 Terri
fun:user:0 tom
tag:170
tag:171
tag:195
tag:196
tag:197
tag:198
tag:199
rect:1: [10 10],[20 20]
rect:2: [15 15],[24 24]
rect:3: [17 17],[27 27]
rect:1: [10 10],[20 20]
rect:2: [15 15],[24 24]
`
	res = strings.Replace(res, "\r", "", -1)
	if strings.TrimSpace(buf.String()) != strings.TrimSpace(res) {
		t.Fatalf("expected [%v], got [%v]", strings.TrimSpace(res), strings.TrimSpace(buf.String()))
	}
}
