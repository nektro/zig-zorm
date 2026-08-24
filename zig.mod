id: c8lag7k2cr3lxodwor24az8j61nblulbw1oxw3tl191wgoje
name: zorm
main: src/lib.zig
license: MPL-2.0
description: The ORM library for Zig.
dependencies:
  - src: http https://sqlite.org/2025/sqlite-amalgamation-3480000.zip sha256-d9a15a42db7c78f88fe3d3c5945acce2f4bfe9e4da9f685cd19f6ea1d40aa884
    id: 5wea8xz8pv9w3gv4ve959e6wnxp372g6t4dpzf8j
    license: blessing
    description: SQLite is a C-language library that implements a small, fast, self-contained, high-reliability, full-featured, SQL database engine.
    c_include_dirs:
      - sqlite-amalgamation-3480000
    c_source_files:
      - sqlite-amalgamation-3480000/sqlite3.c

  - src: git https://github.com/nektro/zig-tracer
  - src: git https://github.com/nektro/zig-whatwg-url
  - src: git https://github.com/nektro/zig-extras
  - src: git https://github.com/nektro/zig-net
  - src: git https://github.com/nektro/zig-nio
  - src: git https://github.com/nektro/zig-sys-linux
  - src: git https://github.com/nektro/zig-sys-freebsd
  - src: git https://github.com/nektro/zig-sys-netbsd
  - src: git https://github.com/nektro/zig-sys-openbsd
