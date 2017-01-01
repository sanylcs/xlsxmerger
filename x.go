package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/tealeg/xlsx"
)

const (
	maxFileReadPerTime = 10
	maxSheetDigest     = 10
	colMarker          = "Customer ID"
	rowMarker          = "Interval Time"
)

var (
	dir, fname, rMarker, cMarker, out string
)

type pos struct {
	x, y int // x=column, y=row
}

type processedSheet struct {
	*xlsx.Sheet
	nextX, nextY *int
	XMarker      *pos
	YMarker      *pos
	YHeadCol     int // col positioon to get the Y head
}

type xHeadManyY struct {
	head         string
	XType, YType xlsx.CellType
	XFmt, YFmt   string
	data         map[string]string
	dataType     xlsx.CellType
	dataFmt      string
}

func main() {
	if !flag.Parsed() {
		flag.Parse()
	}
	fd, err := os.Open(dir)
	if err != nil {
		log.Fatalln(err)
	}
	fi, err := fd.Stat()
	if err != nil {
		log.Fatalln(err)
	}
	if !fi.IsDir() {
		log.Fatalln("Must provide directory")
	}
	if out == "" {
		out = "output.xlsx"
	}
	if cMarker == "" {
		cMarker = colMarker
	}
	if rMarker == "" {
		rMarker = rowMarker
	}
	ctx, cancel := context.WithCancel(context.Background())
	sheetch := make(chan *xlsx.Sheet)
	dch := make(chan *xHeadManyY)
	// last goroutine stage process: take smallest group of processed data and
	// check and merge into single sheet.
	go func() {
		// parse the merge data
		fs, err := checkAndOpenXLSX(fname)
		if err != nil {
			panic(err)
		} else if len(fs.Sheets) == 0 {
			panic(errors.New("must has sheet data"))
		}
		m := &processedSheet{Sheet: fs.Sheets[0]}
		if err = parseMarker(m); err != nil {
			panic(err)
		}
		// fmt.Println("debug processed merged:", m.YMarker, m.XMarker,
		// 	m.YHeadCol)
		for d := range dch {
			// fmt.Println("debug: data:", d)
			n, found := findXHead(d.head, m)
			if !found {
				// Can not find the provided X header value. So it is a new X
				// (column) data so execute column insertion. First insert the
				// new X header to X header row. AddCell will globally add
				// column to all rows.
				c := m.Rows[m.XMarker.y].AddCell()
				SetCellValue(c, d.head, d.XFmt, d.XType)
				// Then for each of the Y data rows insert cells value data with
				// the new data.
			}
			for k, v := range d.data {
				var currYRow int
				for j := m.YMarker.y + 1; j < len(m.Rows); j++ {
					s := m.Cell(j, m.YHeadCol).Value
					if k == s {
						// found matched Y head, k is NOT new
						// replace the cell with new k value
						currYRow = j
					} else {
						// check next matched Y head
						next := j + 1
						if next < len(m.Rows) {
							s = m.Cell(next, m.YHeadCol).Value
							if s != "" && k >= s {
								continue
							}
						}
						// k is new Y row value so execute row insertion
						// into row right after j
						rows := m.Rows[next:]
						m.Rows = m.Rows[:next]
						m.AddRow()
						m.Rows = append(m.Rows, rows...)
						if len(m.Rows) > m.MaxRow {
							m.MaxRow = len(m.Rows)
						}
						// set new y header value
						SetCellValue(m.Cell(next, m.YHeadCol), k, d.YFmt,
							d.YType)
						// set x value for the respective new y row
						currYRow = next
					}
					SetCellValue(m.Cell(currYRow, n), v, d.dataFmt, d.dataType)
					break
				}
			}
		}
		err = fs.Save(out)
		if err != nil {
			fmt.Println("Error while saving processed file:", err)
		}
		cancel()
	}()
	var wg sync.WaitGroup
	wg.Add(maxSheetDigest)
	for i := 0; i < maxSheetDigest; i++ {
		// Sheet goroutine: input sheet and break into smallest data to be
		// processed in last stage.
		go func() {
			defer wg.Done()
			for sh := range sheetch {
				// fmt.Println("debug: Sheet", sh.Name, "c:", sh.MaxCol,
				// 	len(sh.Cols), "r:", sh.MaxRow, len(sh.Rows))
				p := &processedSheet{Sheet: sh}
				if err := parseMarker(p); err != nil {
					panic(err)
				}
				for {
					y, x, err := nextXCell(p)
					if err != nil {
						break
					}
					c := p.Sheet.Cell(y, x)
					v := c.Value
					if v == "" {
						continue
					}
					d := &xHeadManyY{
						head:  v,
						XType: c.Type(),
						XFmt:  c.NumFmt,
						data:  map[string]string{},
					}
					fillDataFormat(d, p)
					for {
						h, y, x, err := nextYCell(p)
						if err != nil || h == "" {
							break
						}
						d.data[h] = p.Sheet.Cell(y, x).Value
					}
					dch <- d
				}
			}
		}()
	}
	var (
		fis     []os.FileInfo
		gerr    error
		cs      [maxFileReadPerTime]chan string
		sheetwg sync.WaitGroup
	)
	sheetwg.Add(maxFileReadPerTime)
	for i := 0; i < maxFileReadPerTime; i++ {
		cs[i] = make(chan string)
		go func(i int) {
			defer sheetwg.Done()
			for fn := range cs[i] {
				fs, err := checkAndOpenXLSX(fn)
				if err == nil && len(fs.Sheets) > 0 {
					for _, s := range fs.Sheets {
						sheetch <- s
					}
				} else {
					fmt.Println("Error in loading XLSX file:", err)
				}
			}
		}(i)
	}
	for gerr == nil {
		fis, gerr = fd.Readdir(maxFileReadPerTime)
		for i, fi := range fis {
			cs[i] <- filepath.Join(fd.Name(), fi.Name())
		}
	}
	for i := 0; i < maxFileReadPerTime; i++ {
		close(cs[i])
	}
	sheetwg.Wait()
	close(sheetch)
	wg.Wait()
	close(dch)
	<-ctx.Done()
}

func SetCellValue(c *xlsx.Cell, v, f string, t xlsx.CellType) {
	c.NumFmt = f
	switch t {
	case xlsx.CellTypeString:
		c.SetString(v)
	case xlsx.CellTypeNumeric:
		fl, err := strconv.ParseFloat(v, 64)
		if err != nil {
			panic(err)
		}
		c.SetFloatWithFormat(fl, f)
	default:
		c.SetValue(v)
	}
}

func fillDataFormat(d *xHeadManyY, p *processedSheet) {
	c := p.Sheet.Cell(*p.nextY+1, p.YHeadCol)
	d.YType = c.Type()
	d.YFmt = c.NumFmt
	c = p.Cell(*p.nextY+1, *p.nextX)
	d.dataType = c.Type()
	d.dataFmt = c.NumFmt
}

func checkAndOpenXLSX(f string) (*xlsx.File, error) {
	e := filepath.Ext(f)
	if strings.HasSuffix(e, ".xls") || strings.HasSuffix(e, ".xlsx") {
		fs, err := xlsx.OpenFile(f)
		if err != nil {
			return nil, err
		}
		return fs, nil
	}
	s := fmt.Sprint("Unsupported file extension:", f)
	fmt.Println(s)
	return nil, errors.New(s)
}

func nextXCell(p *processedSheet) (int, int, error) {
	if p.nextX != nil {
		*p.nextX++
		if *p.nextX >= len(p.Cols) {
			return 0, 0, io.EOF
		}
	} else {
		p.nextX = new(int)
		*p.nextX = p.XMarker.x
		if p.nextY == nil {
			p.nextY = new(int)
		}
	}
	*p.nextY = p.YMarker.y
	return p.XMarker.y, *p.nextX, nil
}

// nextYCell must only be called after nextXCell is called.
func nextYCell(p *processedSheet) (string, int, int, error) {
	*p.nextY++
	if *p.nextY >= len(p.Rows) {
		return "", 0, 0, io.EOF
	}
	row := *p.nextY
	return p.Sheet.Cell(row, p.YHeadCol).Value, row, *p.nextX, nil
}

var errNoMarker = errors.New("Can not find marker")

func parseMarker(p *processedSheet) error {
	var (
		i int
		r *xlsx.Row
	)
	rs := p.Sheet.Rows
	for i, r = range rs {
		if _, pos, ok := checkMarker(cMarker, i, r.Cells); ok {
			p.XMarker = pos
			i++
			break
		}
	}
	for j, rr := range rs[i:] {
		if n, pos, ok := checkMarker(rMarker, j+i, rr.Cells); ok {
			pos.x = p.XMarker.x // col must same as XMarker col
			p.YMarker = pos
			p.YHeadCol = n
			return nil
		}
	}
	return errNoMarker
}

func checkMarker(m string, i int, cs []*xlsx.Cell) (int, *pos, bool) {
	var (
		n int
		p *pos
		t bool
	)
	for j, cell := range cs {
		if p == nil && strings.HasPrefix(strings.Trim(cell.Value, " "), m) {
			n = j
			p = &pos{y: i, x: j + 1}
			t = true
		} else if p != nil && cell.Value != "" {
			p.x = j
			break
		}
	}
	return n, p, t
}

func findXHead(h string, p *processedSheet) (int, bool) {
	if p.XMarker.x >= p.MaxCol {
		return p.XMarker.x, false
	}
	for i, c := range p.Rows[p.XMarker.y].Cells[p.XMarker.x:] {
		if h == c.Value {
			return i + p.XMarker.x, true
		}
	}
	return len(p.Rows[p.XMarker.y].Cells), false
}

func init() {
	flag.StringVar(&dir, "d", "", "Provide the directory to input files")
	flag.StringVar(&fname, "f", "", "Provide the output file path")
	flag.StringVar(&cMarker, "c", "", "Provide column marker string")
	flag.StringVar(&rMarker, "r", "", "Provide row marker string")
	flag.StringVar(&out, "o", "", "Provide output file path")
}
