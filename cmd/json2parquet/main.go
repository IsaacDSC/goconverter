// json2parquet converte JSON (array de objetos ou NDJSON / JSON Lines)
// num ficheiro Parquet com colunas de primeiro nível.
// Objetos e arrays são serializados em colunas do tipo STRING (JSON compacto).
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
)

func main() {
	inputPath := flag.String("i", "", "ficheiro JSON de entrada (vazio ou \"-\" = stdin)")
	outputPath := flag.String("o", "", "ficheiro Parquet de saída (vazio ou \"-\" = stdout)")
	comp := flag.String("compression", "snappy", "compressão: none, snappy, gzip, zstd, lz4, brotli")
	flag.Parse()

	if err := run(*inputPath, *outputPath, *comp); err != nil {
		fmt.Fprintf(os.Stderr, "json2parquet: %v\n", err)
		os.Exit(1)
	}
}

func run(inputPath, outputPath, compression string) error {
	in, err := openInput(inputPath)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := openOutput(outputPath)
	if err != nil {
		return err
	}
	defer out.Close()

	rows, err := readJSONObjects(in)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return errors.New("nenhum registo JSON encontrado")
	}

	keys, colTypes := inferColumns(rows)
	if len(keys) == 0 {
		return errors.New("não foi possível inferir colunas (objetos vazios?)")
	}

	rowType := buildRowStruct(keys, colTypes)
	schema := parquet.SchemaOf(reflect.New(rowType).Elem().Interface())

	opts := []parquet.WriterOption{schema}
	if c, ok := codecFromName(compression); ok {
		opts = append(opts, parquet.Compression(c))
	} else {
		return fmt.Errorf("compressão desconhecida: %q", compression)
	}

	w := parquet.NewGenericWriter[any](out, opts...)

	batch := make([]any, 0, 256)
	for _, m := range rows {
		val, err := materializeRow(reflect.New(rowType).Elem(), keys, colTypes, m)
		if err != nil {
			_ = w.Close()
			return err
		}
		batch = append(batch, val.Interface())
		if len(batch) >= 256 {
			if _, err := w.Write(batch); err != nil {
				_ = w.Close()
				return err
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		if _, err := w.Write(batch); err != nil {
			_ = w.Close()
			return err
		}
	}
	return w.Close()
}

func openInput(path string) (io.ReadCloser, error) {
	if path == "" || path == "-" {
		return io.NopCloser(os.Stdin), nil
	}
	return os.Open(path)
}

func openOutput(path string) (io.WriteCloser, error) {
	if path == "" || path == "-" {
		return nopWriteCloser{os.Stdout}, nil
	}
	return os.Create(path)
}

type nopWriteCloser struct{ io.Writer }

func (n nopWriteCloser) Close() error { return nil }

func codecFromName(name string) (compress.Codec, bool) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "none", "uncompressed":
		return &parquet.Uncompressed, true
	case "snappy":
		return &parquet.Snappy, true
	case "gzip":
		return &parquet.Gzip, true
	case "zstd":
		return &parquet.Zstd, true
	case "lz4", "lz4raw":
		return &parquet.Lz4Raw, true
	case "brotli":
		return &parquet.Brotli, true
	default:
		return nil, false
	}
}

func readJSONObjects(r io.Reader) ([]map[string]any, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return nil, nil
	}

	if data[0] == '[' {
		dec := json.NewDecoder(bytes.NewReader(data))
		dec.UseNumber()
		var arr []map[string]any
		if err := dec.Decode(&arr); err != nil {
			return nil, fmt.Errorf("JSON array: %w", err)
		}
		return arr, nil
	}

	// NDJSON: uma linha = um objeto JSON
	var rows []map[string]any
	lines := bytes.Split(data, []byte("\n"))
	for _, line := range lines {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		dec := json.NewDecoder(bytes.NewReader(line))
		dec.UseNumber()
		var obj map[string]any
		if err := dec.Decode(&obj); err != nil {
			return nil, fmt.Errorf("NDJSON linha %q: %w", string(line), err)
		}
		rows = append(rows, obj)
	}
	return rows, nil
}

type colInf struct {
	hasNil     bool
	hasBool    bool
	hasInt     bool
	hasFloat   bool
	hasString  bool
	hasComplex bool
}

func (c *colInf) observe(v any) {
	if v == nil {
		c.hasNil = true
		return
	}
	switch x := v.(type) {
	case bool:
		c.hasBool = true
	case json.Number:
		if _, err := x.Int64(); err == nil {
			c.hasInt = true
		} else {
			c.hasFloat = true
		}
	case float64:
		c.hasFloat = true
	case string:
		c.hasString = true
	case map[string]any, []any:
		c.hasComplex = true
	default:
		// json.Unmarshal edge types
		c.hasComplex = true
	}
}

func (c *colInf) finalize() reflect.Kind {
	if c.hasComplex {
		return reflect.String
	}
	if c.hasString {
		return reflect.String
	}
	if c.hasBool && (c.hasInt || c.hasFloat) {
		return reflect.String
	}
	if c.hasFloat {
		return reflect.Float64
	}
	if c.hasInt {
		return reflect.Int64
	}
	if c.hasBool {
		return reflect.Bool
	}
	return reflect.String
}

func inferColumns(rows []map[string]any) ([]string, []reflect.Kind) {
	inf := make(map[string]*colInf)
	for _, row := range rows {
		for k, v := range row {
			if inf[k] == nil {
				inf[k] = &colInf{}
			}
			inf[k].observe(v)
		}
	}
	keys := make([]string, 0, len(inf))
	for k := range inf {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	kinds := make([]reflect.Kind, len(keys))
	for i, k := range keys {
		kinds[i] = inf[k].finalize()
	}
	return keys, kinds
}

func parquetTagName(k string) string {
	// evita quebra do separador da struct tag
	if strings.ContainsAny(k, ",`") {
		return strings.Map(func(r rune) rune {
			if r == ',' || r == '`' {
				return '_'
			}
			return r
		}, k)
	}
	return k
}

func buildRowStruct(keys []string, kinds []reflect.Kind) reflect.Type {
	fields := make([]reflect.StructField, len(keys))
	for i, k := range keys {
		name := parquetTagName(k)
		var base reflect.Type
		switch kinds[i] {
		case reflect.Bool:
			base = reflect.TypeOf(false)
		case reflect.Int64:
			base = reflect.TypeOf(int64(0))
		case reflect.Float64:
			base = reflect.TypeOf(float64(0))
		default:
			base = reflect.TypeOf("")
		}
		ptr := reflect.PtrTo(base)
		tag := fmt.Sprintf(`parquet:"%s,optional"`, name)
		fields[i] = reflect.StructField{
			Name: fmt.Sprintf("F%d", i),
			Type: ptr,
			Tag:  reflect.StructTag(tag),
		}
	}
	return reflect.StructOf(fields)
}

func materializeRow(row reflect.Value, keys []string, kinds []reflect.Kind, m map[string]any) (reflect.Value, error) {
	for i, k := range keys {
		f := row.Field(i)
		v, ok := m[k]
		if !ok || v == nil {
			continue
		}
		ptr := reflect.New(f.Type().Elem())
		switch kinds[i] {
		case reflect.Bool:
			b, err := toBool(v)
			if err != nil {
				return row, fmt.Errorf("coluna %q: %w", k, err)
			}
			ptr.Elem().SetBool(b)
		case reflect.Int64:
			n, err := toInt64(v)
			if err != nil {
				return row, fmt.Errorf("coluna %q: %w", k, err)
			}
			ptr.Elem().SetInt(n)
		case reflect.Float64:
			fv, err := toFloat64(v)
			if err != nil {
				return row, fmt.Errorf("coluna %q: %w", k, err)
			}
			ptr.Elem().SetFloat(fv)
		case reflect.String:
			s, err := toJSONStringCell(v)
			if err != nil {
				return row, fmt.Errorf("coluna %q: %w", k, err)
			}
			ptr.Elem().SetString(s)
		}
		f.Set(ptr)
	}
	return row, nil
}

func toBool(v any) (bool, error) {
	switch x := v.(type) {
	case bool:
		return x, nil
	default:
		return false, fmt.Errorf("esperava bool, obteve %T", v)
	}
}

func toInt64(v any) (int64, error) {
	switch x := v.(type) {
	case json.Number:
		return x.Int64()
	case float64:
		return int64(x), nil
	case int64:
		return x, nil
	case int:
		return int64(x), nil
	default:
		return 0, fmt.Errorf("esperava número inteiro, obteve %T", v)
	}
}

func toFloat64(v any) (float64, error) {
	switch x := v.(type) {
	case json.Number:
		return x.Float64()
	case float64:
		return x, nil
	case int64:
		return float64(x), nil
	case int:
		return float64(x), nil
	default:
		return 0, fmt.Errorf("esperava número, obteve %T", v)
	}
}

func toJSONStringCell(v any) (string, error) {
	switch x := v.(type) {
	case string:
		return x, nil
	case bool:
		return strconv.FormatBool(x), nil
	case json.Number:
		return x.String(), nil
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64), nil
	case int64:
		return strconv.FormatInt(x, 10), nil
	case map[string]any, []any:
		b, err := json.Marshal(x)
		if err != nil {
			return "", err
		}
		return string(b), nil
	default:
		b, err := json.Marshal(x)
		if err != nil {
			return "", err
		}
		return string(b), nil
	}
}
