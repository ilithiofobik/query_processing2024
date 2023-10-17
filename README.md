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
  - debian: `apt install libtbb2 libtbb2-dev`

## Task 1: Manual Join

Manually implement the given query in `bin/manualjoin.cpp`.

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
gdb bin/manualjoin.out
```

Performance build:

``` sh
make target=release bin/manualjoin.out
```

Also try `target=sanitize` to use the address sanitizer.