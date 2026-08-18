const std = @import("std");
const string = []const u8;
const tracer = @import("tracer");
const extras = @import("extras");
const builtin = @import("builtin");

const Driver = @This();

db: *c.sqlite3,

pub fn connect(allocator: std.mem.Allocator, path: [:0]const u8) !Driver {
    var driver: Driver = try .connect_only(allocator, path);
    errdefer driver.close();
    _ = try driver.first(allocator, void, "PRAGMA journal_mode = WAL", .{});
    _ = try driver.first(allocator, void, "PRAGMA busy_timeout = 5000", .{});
    _ = try driver.first(allocator, void, "PRAGMA synchronous = NORMAL", .{});
    _ = try driver.first(allocator, void, "PRAGMA cache_size = 1000000000", .{});
    _ = try driver.first(allocator, void, "PRAGMA foreign_keys = true", .{});
    _ = try driver.first(allocator, void, "PRAGMA temp_store = memory", .{});
    _ = try driver.first(allocator, void, "PRAGMA mmap_size = 1073741824", .{});
    _ = try driver.first(allocator, void, "PRAGMA auto_vacuum = INCREMENTAL", .{});
    return driver;
}
pub fn connect_only(allocator: std.mem.Allocator, path: [:0]const u8) !Driver {
    std.log.scoped(.zorm).info("connecting to {s} @ {s}", .{ "sqlite3", path });
    _ = allocator;
    var db: ?*c.sqlite3 = null;
    var flags: c_int = 0;
    flags |= c.SQLITE_OPEN_READWRITE;
    flags |= c.SQLITE_OPEN_CREATE;
    flags |= c.SQLITE_OPEN_FULLMUTEX;
    s.assert(c.sqlite3_open_v2(path, &db, flags, null));
    std.debug.assert(c.sqlite3_threadsafe() > 0);
    return .{ .db = db.? };
}

pub fn close(self: *Driver) void {
    s.assert(c.sqlite3_close_v2(self.db));
}

pub fn collect(self: *Driver, alloc: std.mem.Allocator, comptime T: type, comptime query: string, args: anytype) ![]T {
    const t = tracer.trace(@src(), " {s}", .{query});
    defer t.end();

    var list = std.array_list.Managed(T).init(alloc);
    errdefer list.deinit();
    var stmt: Statement = try .prepare(self, query);
    defer stmt.finalize();
    try stmt.bindArgs(alloc, args);
    const iter = stmt.iterate();
    errdefer iter.reset();
    while (try iter.step(alloc, T)) |row| try list.append(row);
    return list.toOwnedSlice();
}

pub fn exec(self: *Driver, alloc: std.mem.Allocator, comptime query: string, args: anytype) !void {
    const t = tracer.trace(@src(), " {s}", .{query});
    defer t.end();

    var stmt: Statement = try .prepare(self, query);
    defer stmt.finalize();
    try stmt.bindArgs(alloc, args);
    try stmt.exec(alloc);
}

pub fn first(self: *Driver, alloc: std.mem.Allocator, comptime T: type, comptime query: string, args: anytype) !?T {
    const t = tracer.trace(@src(), " {s}", .{query});
    defer t.end();

    var stmt: Statement = try .prepare(self, query);
    defer stmt.finalize();
    try stmt.bindArgs(alloc, args);
    const iter = stmt.iterate();
    errdefer iter.reset();
    return iter.step(alloc, T);
}

pub fn doesTableExist(self: *Driver, alloc: std.mem.Allocator, name: string) !bool {
    const t = tracer.trace(@src(), " {s}", .{name});
    defer t.end();

    for (try self.collect(alloc, string, "select name from sqlite_master where type = ? AND name = ?", .{ .type = "table", .name = name })) |item| {
        if (std.mem.eql(u8, item, name)) {
            return true;
        }
    }
    return false;
}

pub fn hasColumnWithName(self: *Driver, alloc: std.mem.Allocator, comptime table: string, comptime column: string) !bool {
    const t = tracer.trace(@src(), " {s}.{s}", .{ table, column });
    defer t.end();

    for (try pragma.table_info(self, alloc, table)) |item| {
        if (std.mem.eql(u8, item.name, column)) {
            return true;
        }
    }
    return false;
}

pub fn createTable(self: *Driver, alloc: std.mem.Allocator, comptime name: []const u8, comptime pk_name: []const u8, pk_type: type) !void {
    const t = tracer.trace(@src(), " {s} ({s})", .{ name, pk_name });
    defer t.end();
    try self.exec(alloc, comptime std.fmt.comptimePrint("create table {s}({s} {s} primary key not null)", .{ name, pk_name, nameForType2(pk_type) }), .{});
}

pub fn addColumn(self: *Driver, alloc: std.mem.Allocator, comptime table_name: []const u8, comptime col_name: []const u8, T: type) !void {
    const t = tracer.trace(@src(), " {s}.{s}", .{ table_name, col_name });
    defer t.end();
    try self.exec(alloc, comptime std.fmt.comptimePrint("alter table {s} add \"{s}\" {s}", .{ table_name, col_name, nameForType(T) }), .{});
}

pub fn addColumnForeign(self: *Driver, alloc: std.mem.Allocator, comptime table_name: []const u8, comptime col_name: []const u8, T: type, comptime table_name2: []const u8, comptime col_name2: []const u8) !void {
    const t = tracer.trace(@src(), " {s}.{s}", .{ table_name, col_name });
    defer t.end();
    try self.exec(alloc, comptime std.fmt.comptimePrint("alter table {s} add \"{s}\" {s} references \"{s}\" (\"{s}\")", .{ table_name, col_name, nameForType(T), table_name2, col_name2 }), .{});
}

pub fn nameForType(T: type) []const u8 {
    if (@typeInfo(T) == .optional) {
        return nameForType2(@typeInfo(T).optional.child);
    }
    return nameForType2(T) ++ " not null default (" ++ defaultForType(T) ++ ")";
}

pub fn nameForType2(T: type) []const u8 {
    const tinfo = @typeInfo(T);

    if (comptime extras.isZigString(T)) {
        return "text";
    }
    if (tinfo == .@"struct") {
        const info = tinfo.@"struct";
        if (@hasDecl(T, "BaseType") and T.BaseType != []const u8) return T.baseTypeName;
        if (@hasDecl(T, "BaseType")) return nameForType2(T.BaseType);
        if (info.layout == .@"packed") return nameForType2(info.backing_integer.?);
        return nameForType2(T.BaseType);
    }
    if (tinfo == .@"enum") {
        if (@hasDecl(T, "BaseType") and T.BaseType != []const u8) return T.baseTypeName;
        return nameForType2(T.BaseType);
    }
    if (tinfo == .int or tinfo == .bool) {
        return "integer";
    }
    if (comptime extras.isArrayOf(u8)(T)) {
        return "blob";
    }
    @compileError(@typeName(T)); // TODO
}

pub fn defaultForType(T: type) []const u8 {
    const info = @typeInfo(T);
    if (info == .bool) {
        return "false";
    }
    if (info == .@"struct") {
        const sinfo = info.@"struct";
        if (@hasDecl(T, "BaseType")) return defaultForType(T.BaseType);
        if (sinfo.layout == .@"packed") return defaultForType(sinfo.backing_integer.?);
    }
    if (comptime extras.isZigString(T)) {
        return "''";
    }
    if (info == .int) {
        return "0";
    }
    if (info == .@"enum") {
        return defaultForType(T.BaseType);
    }
    @compileError(@typeName(T)); // TODO
}

pub const Pragma = struct {
    pub const TableInfo = struct {
        cid: u16,
        name: string,
        type: string,
        notnull: bool,
        dflt_value: string,
        pk: bool,
    };
};

pub const pragma = struct {
    pub fn table_info(self: *Driver, alloc: std.mem.Allocator, comptime name: string) ![]const Pragma.TableInfo {
        const t = tracer.trace(@src(), " {s}", .{name});
        defer t.end();

        return try self.collect(alloc, Pragma.TableInfo, "pragma table_info(" ++ name ++ ")", .{});
    }
};

pub const c = @cImport({
    @cInclude("sqlite3.h");
});

pub const s = struct {
    const Error = error{
        SQLITE_ERROR,
        SQLITE_INTERNAL,
        SQLITE_PERM,
        SQLITE_ABORT,
        SQLITE_BUSY,
        SQLITE_LOCKED,
        OutOfMemory,
        SQLITE_READONLY,
        SQLITE_INTERRUPT,
        SQLITE_IOERR,
        SQLITE_CORRUPT,
        SQLITE_NOTFOUND,
        SQLITE_FULL,
        SQLITE_CANTOPEN,
        SQLITE_PROTOCOL,
        SQLITE_EMPTY,
        SQLITE_SCHEMA,
        SQLITE_TOOBIG,
        SQLITE_CONSTRAINT,
        SQLITE_MISMATCH,
        SQLITE_MISUSE,
        SQLITE_NOLFS,
        SQLITE_AUTH,
        SQLITE_FORMAT,
        SQLITE_RANGE,
        SQLITE_NOTADB,
        SQLITE_NOTICE,
        SQLITE_WARNING,
    };
    pub fn rc2e(code: c_int) Error {
        if (code == c.SQLITE_ERROR) return error.SQLITE_ERROR;
        if (code == c.SQLITE_INTERNAL) return error.SQLITE_INTERNAL;
        if (code == c.SQLITE_PERM) return error.SQLITE_PERM;
        if (code == c.SQLITE_ABORT) return error.SQLITE_ABORT;
        if (code == c.SQLITE_BUSY) return error.SQLITE_BUSY;
        if (code == c.SQLITE_LOCKED) return error.SQLITE_LOCKED;
        if (code == c.SQLITE_NOMEM) return error.OutOfMemory;
        if (code == c.SQLITE_READONLY) return error.SQLITE_READONLY;
        if (code == c.SQLITE_INTERRUPT) return error.SQLITE_INTERRUPT;
        if (code == c.SQLITE_IOERR) return error.SQLITE_IOERR;
        if (code == c.SQLITE_CORRUPT) return error.SQLITE_CORRUPT;
        if (code == c.SQLITE_NOTFOUND) return error.SQLITE_NOTFOUND;
        if (code == c.SQLITE_FULL) return error.SQLITE_FULL;
        if (code == c.SQLITE_CANTOPEN) return error.SQLITE_CANTOPEN;
        if (code == c.SQLITE_PROTOCOL) return error.SQLITE_PROTOCOL;
        if (code == c.SQLITE_EMPTY) return error.SQLITE_EMPTY;
        if (code == c.SQLITE_SCHEMA) return error.SQLITE_SCHEMA;
        if (code == c.SQLITE_TOOBIG) return error.SQLITE_TOOBIG;
        if (code == c.SQLITE_CONSTRAINT) return error.SQLITE_CONSTRAINT;
        if (code == c.SQLITE_MISMATCH) return error.SQLITE_MISMATCH;
        if (code == c.SQLITE_MISUSE) return error.SQLITE_MISUSE;
        if (code == c.SQLITE_NOLFS) return error.SQLITE_NOLFS;
        if (code == c.SQLITE_AUTH) return error.SQLITE_AUTH;
        if (code == c.SQLITE_FORMAT) return error.SQLITE_FORMAT;
        if (code == c.SQLITE_RANGE) return error.SQLITE_RANGE;
        if (code == c.SQLITE_NOTADB) return error.SQLITE_NOTADB;
        if (code == c.SQLITE_NOTICE) return error.SQLITE_NOTICE;
        if (code == c.SQLITE_WARNING) return error.SQLITE_WARNING;
        unreachable;
    }
    pub fn rc2p(code: c_int) Error {
        if (builtin.mode == .Debug) @panic(std.mem.sliceTo(c.sqlite3_errstr(code), 0));
        return rc2e(code);
    }
    pub fn assert(code: c_int) void {
        if (code == c.SQLITE_OK) return;
        @panic(std.mem.sliceTo(c.sqlite3_errstr(code), 0));
    }
    pub fn please(code: c_int) !void {
        if (code == c.SQLITE_OK) return;
        return rc2p(code);
    }
    pub fn rc2p_d(db: *c.sqlite3, code: c_int) Error {
        if (builtin.mode == .Debug) @panic(std.mem.sliceTo(c.sqlite3_errmsg(db), 0));
        return rc2e(code);
    }
    pub fn assert_d(db: *c.sqlite3, code: c_int) void {
        if (code == c.SQLITE_OK) return;
        @panic(std.mem.sliceTo(c.sqlite3_errmsg(db), 0));
    }
    pub fn please_d(db: *c.sqlite3, code: c_int) !void {
        if (code == c.SQLITE_OK) return;
        return rc2p_d(db, code);
    }
};

pub const Statement = struct {
    db: *c.sqlite3,
    stmt: *c.sqlite3_stmt,

    pub fn prepare(driver: *Driver, query: []const u8) !Statement {
        var stmt: ?*c.sqlite3_stmt = null;
        var flags: c_uint = 0;
        _ = &flags;
        try s.please_d(driver.db, c.sqlite3_prepare_v3(driver.db, query.ptr, @intCast(query.len), flags, &stmt, null));
        return .{ .db = driver.db, .stmt = stmt.? };
    }

    pub fn finalize(stmt: Statement) void {
        s.assert_d(stmt.db, c.sqlite3_finalize(stmt.stmt));
    }

    pub fn bindArgs(stmt: Statement, allocator: std.mem.Allocator, args: anytype) !void {
        if (comptime extras.isSlice(@TypeOf(args))) {
            for (args, 0..) |a, i| {
                const A = @TypeOf(a);
                try bindType(stmt, allocator, i + 1, A, a);
            }
            return;
        }
        inline for (@typeInfo(@TypeOf(args)).@"struct".fields, 0..) |f, i| {
            try bindType(stmt, allocator, i + 1, f.type, @field(args, f.name));
        }
    }

    fn bindType(stmt: Statement, allocator: std.mem.Allocator, idx: usize, T: type, value: T) !void {
        if (comptime extras.isZigString(T)) {
            return s.please_d(stmt.db, c.sqlite3_bind_text64(stmt.stmt, @intCast(idx), value.ptr, value.len, c.SQLITE_STATIC, c.SQLITE_UTF8));
        }
        if (comptime extras.isArrayOf(u8)(T)) {
            return s.please_d(stmt.db, c.sqlite3_bind_blob64(stmt.stmt, @intCast(idx), &value, value.len, c.SQLITE_TRANSIENT));
        }
        switch (@typeInfo(T)) {
            .@"struct" => |info| {
                if (@hasDecl(T, "BaseType")) return bindBaseType(stmt, allocator, idx, T, value);
                if (info.layout == .@"packed") return bindType(stmt, allocator, idx, info.backing_integer.?, @bitCast(value));
                return bindBaseType(stmt, allocator, idx, T, value);
            },
            .int => |info| {
                comptime std.debug.assert(info.bits <= 64);
                if (value > std.math.maxInt(c.sqlite_int64)) return error.Overflow;
                if (value < std.math.minInt(c.sqlite_int64)) return error.Overflow;
                return s.please_d(stmt.db, c.sqlite3_bind_int64(stmt.stmt, @intCast(idx), @intCast(value)));
            },
            .optional => |info| {
                if (value == null) return s.please(c.sqlite3_bind_null(stmt.stmt, @intCast(idx)));
                return bindType(stmt, allocator, idx, info.child, value.?);
            },
            .bool => {
                return bindType(stmt, allocator, idx, u1, @intFromBool(value));
            },
            .@"enum" => {
                if (T.BaseType == []const u8 and !@hasDecl(T, "bindField")) {
                    return bindType(stmt, allocator, idx, []const u8, @tagName(value));
                }
                return bindBaseType(stmt, allocator, idx, T, value);
            },
            .@"union" => {
                switch (value) {
                    inline else => |val| return bindType(stmt, allocator, idx, @TypeOf(val), val),
                }
            },
            else => @compileError(@typeName(T)),
        }
    }

    fn bindBaseType(stmt: Statement, allocator: std.mem.Allocator, idx: usize, T: type, value: T) !void {
        const bind_fn = T.bindField;
        const info = @typeInfo(@TypeOf(bind_fn)).@"fn";
        switch (info.params.len) {
            1 => {
                const base = try value.bindField();
                return bindType(stmt, allocator, idx, @TypeOf(base), base);
            },
            2 => {
                const base = try value.bindField(allocator);
                return bindType(stmt, allocator, idx, @TypeOf(base), base);
            },
            else => comptime unreachable,
        }
    }

    pub fn exec(stmt: Statement, allocator: std.mem.Allocator) !void {
        const iter = stmt.iterate();
        errdefer iter.reset();
        const row = try iter.step(allocator, void);
        if (builtin.mode == .Debug) std.debug.assert(row == null);
    }

    pub fn iterate(stmt: Statement) Iterator {
        return .{ .db = stmt.db, .stmt = stmt.stmt };
    }

    pub const Iterator = struct {
        db: *c.sqlite3,
        stmt: *c.sqlite3_stmt,

        pub fn reset(iter: Iterator) void {
            return s.please_d(iter.db, c.sqlite3_reset(iter.stmt)) catch {};
        }

        pub fn step(iter: Iterator, allocator: std.mem.Allocator, T: type) !?T {
            const code = c.sqlite3_step(iter.stmt);
            if (code == c.SQLITE_DONE) return null;
            if (code != c.SQLITE_ROW) return s.rc2p_d(iter.db, code);
            if (T == void) return;
            if (T == string) return try readType(iter, allocator, 0, string);
            if (@typeInfo(T) == .int) return try readType(iter, allocator, 0, T);
            var result: T = undefined;
            inline for (@typeInfo(T).@"struct".fields, 0..) |f, i| {
                @field(result, f.name) = try readType(iter, allocator, i, f.type);
            }
            return result;
        }

        fn readType(iter: Iterator, allocator: std.mem.Allocator, idx: usize, T: type) !T {
            if (comptime extras.isZigString(T)) {
                const res = c.sqlite3_column_text(iter.stmt, @intCast(idx));
                if (res == null) return "";
                const ptr: [*:0]const u8 = @ptrCast(res);
                const len = c.sqlite3_column_bytes(iter.stmt, @intCast(idx));
                return try allocator.dupe(u8, ptr[0..@intCast(len) :0]);
            }
            if (comptime extras.isArrayOf(u8)(T)) {
                const info = @typeInfo(T).array;
                const ptr: [*:0]const u8 = @ptrCast(c.sqlite3_column_blob(iter.stmt, @intCast(idx)));
                const len = c.sqlite3_column_bytes(iter.stmt, @intCast(idx));
                std.debug.assert(len == info.len);
                return ptr[0..info.len].*;
            }
            switch (@typeInfo(T)) {
                .int => |info| {
                    comptime std.debug.assert(info.bits <= 64);
                    const res = c.sqlite3_column_int64(iter.stmt, @intCast(idx));
                    if (res > std.math.maxInt(T)) return error.Overflow;
                    if (res < std.math.minInt(T)) return error.Overflow;
                    return @intCast(res);
                },
                .bool => {
                    return @bitCast(try readType(iter, allocator, idx, u1));
                },
                .@"struct" => |info| {
                    if (@hasDecl(T, "BaseType")) return readBaseType(iter, allocator, idx, T);
                    if (info.layout == .@"packed") return @bitCast(try readType(iter, allocator, idx, info.backing_integer.?));
                    return readBaseType(iter, allocator, idx, T);
                },
                .optional => |info| {
                    if (c.sqlite3_column_type(iter.stmt, @intCast(idx)) == c.SQLITE_NULL) return null;
                    return try readType(iter, allocator, idx, info.child);
                },
                .@"enum" => {
                    if (T.BaseType == string) {
                        const ptr: [*:0]const u8 = @ptrCast(c.sqlite3_column_text(iter.stmt, @intCast(idx)));
                        const len = c.sqlite3_column_bytes(iter.stmt, @intCast(idx));
                        const str = ptr[0..@intCast(len) :0];
                        const enm = std.meta.stringToEnum(T, str);
                        return enm orelse T.default;
                    }
                },
                else => {}, // else => @compileError(T), // https://codeberg.org/ziglang/zig/issues/32119
            }
        }

        fn readBaseType(iter: Iterator, allocator: std.mem.Allocator, idx: usize, T: type) !T {
            const base = try readType(iter, allocator, idx, T.BaseType);
            return T.readField(allocator, base);
        }
    };
};
