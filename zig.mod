id: c8lag7k2cr3lxodwor24az8j61nblulbw1oxw3tl191wgoje
name: zorm
main: src/lib.zig
license: AGPL-3.0
description: The ORM library for Zig.
dependencies:
  - src: git https://github.com/vrischmann/zig-sqlite commit-91e5fedd15c5ea3cb42ccceefb3d0f4bb9bad68f
    c_source_files:
      - c/sqlite3.c
      - c/workaround.c
  - src: git https://github.com/nektro/zig-tracer
