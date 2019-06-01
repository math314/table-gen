package main

import (
	"bytes"
	"fmt"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/querypb"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"text/template"
)

func ToUpper(c byte) byte {
	if 'a' <= c && c <= 'z' {
		return c - 'a' + 'A'
	} else {
		return c
	}
}

func ToPascalFromSnake(snake string) string {
	ret := bytes.Buffer{}
	upper := true
	for _, c := range snake {
		if upper {
			ret.WriteByte(ToUpper(byte(c)))
			upper = false
		} else if c == '_' {
			upper = true
		} else {
			ret.WriteByte(byte(c))
		}
	}
	return ret.String()
}

func ToCamelFromSnake(snake string) string {
	ret := bytes.Buffer{}
	upper := false
	for _, c := range snake {
		if upper {
			ret.WriteByte(ToUpper(byte(c)))
			upper = false
		} else if c == '_' {
			upper = true
		} else {
			ret.WriteByte(byte(c))
		}
	}
	return ret.String()
}

type CreateTable struct {
	ddl *sqlparser.DDL
}

func getGolangType(cd *sqlparser.ColumnDefinition) string {
	sqlTyp := cd.Type.SQLType()
	golangTyp := strings.ToLower(sqlTyp.String())
	switch sqlTyp {
	case querypb.Type_DECIMAL:
		golangTyp = "float64"
	case querypb.Type_DATETIME:
		golangTyp = "time.Time"
	case querypb.Type_VARCHAR:
		fallthrough
	case querypb.Type_TEXT:
		golangTyp = "string"
	}
	return golangTyp
}

func (ct *CreateTable) GenerateStruct() string {
	b := bytes.Buffer{}
	fmt.Fprintf(&b, "type Mem%s struct {\n", ToPascalFromSnake(ct.ddl.NewName.Name.String()))
	for _, col := range columnTypes(ct.ddl.TableSpec) {
		fmt.Fprintf(&b, "    %s %s `json:\"%s\"`\n", col.Name, col.Typ, col.RawName)
	}
	fmt.Fprintf(&b, "}\n")

	return b.String()
}

type nameType struct {
	RawName, Name, Typ string
}

func columnTypes(ts *sqlparser.TableSpec) []nameType {
	var nameTypeList []nameType
	for _, col := range ts.Columns {
		name := col.Name.String()
		golangTyp := getGolangType(col)
		if name == "id" {
			golangTyp = "int"
		}
		nameTypeList = append(nameTypeList, nameType{
			RawName: name, Name: ToPascalFromSnake(name), Typ: golangTyp,
		})
	}
	return nameTypeList
}

func uniqueIndexes(ts *sqlparser.TableSpec) []nameType {
	columnNameToType := map[string]string{}
	for _, col := range columnTypes(ts) {
		columnNameToType[col.Name] = col.Typ
	}

	nameTypeList := []nameType{}
	for _, index := range ts.Indexes {
		if index.Info.Primary {
			continue // should be `id` primary auto increment value
		}
		if !index.Info.Unique {
			continue
		}
		var names []string
		for _, c := range index.Columns {
			if c.Column.String() == "id" {
				continue
			}
			names = append(names, c.Column.String())
		}

		if len(names) == 0 {
			continue
		}
		if len(names) > 1 {
			log.Fatal("multi unique index is not supported yet.")
		}

		typ := columnNameToType[ToPascalFromSnake(names[0])]
		nameTypeList = append(nameTypeList, nameType{
			RawName: names[0], Name: ToPascalFromSnake(names[0]), Typ: typ,
		})
	}

	return nameTypeList
}

func (ct *CreateTable) GenerateGoStructs() string {
	tmpl, err := template.New("gen.tmpl").Funcs(template.FuncMap{
		"pascal": ToPascalFromSnake,
		"camel":  ToCamelFromSnake,
	}).ParseFiles("src/gen.tmpl")

	if err != nil {
		log.Fatal(err)
	}

	hasTime := false
	for _, c := range columnTypes(ct.ddl.TableSpec) {
		if c.Typ == "time.Time" {
			hasTime = true
		}
	}

	data := map[string]interface{}{}
	data["Table"] = ToPascalFromSnake(ct.ddl.NewName.Name.String())
	data["RawTable"] = ct.ddl.NewName.Name.String()
	data["InitialSize"] = int(1e5)
	data["UniqueIndexes"] = uniqueIndexes(ct.ddl.TableSpec)
	data["Columns"] = columnTypes(ct.ddl.TableSpec)
	data["HasTime"] = hasTime

	b := bytes.Buffer{}
	err = tmpl.Execute(&b, data)
	if err != nil {
		log.Fatal(err)
	}

	return b.String()
}

func main() {
	//var src, dst string
	//
	//flag.StringVar(&src, "src", "", "source file path")
	//flag.StringVar(&dst, "dst", "", "destination file path")
	//flag.Parse()

	file, err := os.Open("/home/math/dumps/Dump20190518/isuketch_strokes.sql")
	defer file.Close()
	if err != nil {
		log.Fatal(err)
	}

	tokens := sqlparser.NewTokenizer(file)
	for {
		stmt, err := sqlparser.ParseNext(tokens)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		ddl := stmt.(*sqlparser.DDL)
		if ddl == nil {
			continue
		}
		if ddl.Action != "create" {
			continue
		}

		ct := CreateTable{ddl}
		val := ct.GenerateGoStructs()
		ioutil.WriteFile("src/mdb/strokes.go", []byte(val), 0644)
		print(val)
	}
}
