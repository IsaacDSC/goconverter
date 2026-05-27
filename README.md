# json2parquet

Conversor de **JSON** para **Apache Parquet** em Go.

## Uso

Compilar:

```bash
go build -o json2parquet ./cmd/json2parquet
```

Entrada em um destes formatos:

1. **Array JSON** — `[\n  {"col": 1},\n  {...}\n]`
2. **NDJSON (JSON Lines)** — um objeto por linha:

   ```json
   {"id": 1}
   {"id": 2}
   ```

Por defeito usa **stdin** / **stdout** se `-i`/`-o` forem omitidos ou forem `-`.

```bash
./json2parquet -i dados.json -o saida.parquet
cat dados.json | ./json2parquet -o saida.parquet
./json2parquet -compression zstd -i entrada.json -o saida.parquet
```

Compressão: `none`, `snappy`, `gzip`, `zstd`, `lz4`, `brotli`.

## Exemplo rápido

Na pasta `example/` há `sample.json` e o script `./example/convert.sh`, que gera `sample.parquet` na mesma pasta (`go run ./cmd/json2parquet` a partir da raiz do projeto).

```bash
./example/convert.sh
```

## Schema

- Apenas campos de **primeiro nível** de cada objeto viram colunas.
- Tipos inferidos entre linhas:
  - `bool` → BOOLEAN (opcional)
  - número inteiro (com `encoding/json.UseNumber`) → INT64 (opcional)
  - número com parte decimal ou mistura int/flutuante → DOUBLE (opcional)
  - `string` ou mistura com tipos incompatíveis → STRING (opcional)
  - objetos ou arrays aninhados → STRING com o JSON serializado (compacto)

## Dependência

- [parquet-go/parquet-go](https://github.com/parquet-go/parquet-go)
