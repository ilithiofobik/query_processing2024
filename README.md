# Tasks
[Practical Course: High-Performance Query Processing (IN0012, IN2106, IN4359)]
Please fork this repo and add a text file containing
- Name: [Your Name]
- ID: [Your Student ID]
we will update the repo with additional tasks every week

## Assignments
Complete each assignment until the following session (Tuesday 23:59).

## Requirements
- linux; if you want to use a different system you may need to change some stuff in the templates first
- make
- the thread building blocks library
  - link: https://github.com/oneapi-src/oneTBB
  - debian: `apt install libtbb-dev` (this is the new oneapi tbb)
- TPC-H data: https://tumde-my.sharepoint.com/personal/till_steinert_tum_de/_layouts/15/onedrive.aspx?id=%2Fpersonal%2Ftill%5Fsteinert%5Ftum%5Fde%2FDocuments%2FTPCHData&ga=1

## Task 1: Aggregation and Manual Join

Manually implement the given queries in `bin/manualjoin.cpp` and `bin/aggregation.cpp`.

Build:
``` sh
make bin/manualjoin.out
```

Run:

``` sh
bin/manualjoin.out
```

Debug:

``` sh
gdb --args bin/manualjoin.out [arguments]
```

Performance build:

``` sh
make target=release bin/manualjoin.out
```

Also try `target=sanitize` to use the address sanitizer.
