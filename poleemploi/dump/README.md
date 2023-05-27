### `poleemploi/dump`

---

Usage:

Create an account & an application on [pole-emploi.io/inscription](https://pole-emploi.io/inscription).
Save the ID & secret on your computer. Then, to download all job offers since 2019:

```
$ time go run github.com/xpetit/jobs/poleemploi/dump@latest \
  -id     "REDACTED" \
  -secret "REDACTED" \
  -min '2019-01-01 00:00:00' \
  > poleemploi.zstd
saving 828127 job offers
Too Many Requests
Too Many Requests
Too Many Requests
Too Many Requests
saved 812021 job offers at a rate of 4.3 req/sec (maximum allowed: 4)

real    24m58.196s
user    2m34.112s
sys     0m27.604s
```

The result is a JSON Lines with a size of approximately 420MB compressed (2.5GB uncompressed).

Due to the limits of 150 offers/req & 4 reqs/sec, it doesn't seem possible to go faster using this API fairly.
