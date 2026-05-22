const std = @import("std");
const string = []const u8;
const sqlite = @import("sqlite");
const tracer = @import("tracer");
const extras = @import("extras");

const Self = @This();

db: sqlite.Db = undefined,
mutex: std.Thread.Mutex,

pub fn connect(allocator: std.mem.Allocator, path: [:0]const u8) !Self {
    std.log.scoped(.zorm).info("connecting to {s} @ {s}", .{ "sqlite3", path });
    _ = allocator;
    return Self{
        .db = try sqlite.Db.init(.{
            .mode = .{ .File = path },
            .open_flags = .{
                .write = true,
                .create = true,
            },
            .threading_mode = .SingleThread,
        }),
        .mutex = std.Thread.Mutex{},
    };
}

pub fn close(self: *Self) void {
    self.db.deinit();
}

pub fn lock(self: *Self) void {
    self.mutex.lock();
}

pub fn unlock(self: *Self) void {
    self.mutex.unlock();
}

fn prepare(self: *Self, comptime query: string) !sqlite.StatementType(.{}, query) {
    return self.db.prepare(query) catch |err| switch (err) {
        error.SQLiteError => std.debug.panic("{s}", .{self.db.getDetailedError()}),
        else => return err,
    };
}

pub fn collect(self: *Self, alloc: std.mem.Allocator, comptime T: type, comptime query: string, args: anytype) ![]T {
    const t = tracer.trace(@src(), " {s}", .{query});
    defer t.end();

    var stmt = try self.prepare(query);
    defer stmt.deinit();
    var iter = try stmt.iteratorAlloc(T, alloc, args);
    var list = std.ArrayList(T).init(alloc);
    errdefer list.deinit();
    while (try iter.nextAlloc(alloc, .{})) |row| {
        try list.append(row);
    }
    return list.toOwnedSlice();
}

pub fn exec(self: *Self, alloc: std.mem.Allocator, comptime query: string, args: anytype) !void {
    const t = tracer.trace(@src(), " {s}", .{query});
    defer t.end();

    var stmt = try self.prepare(query);
    defer stmt.deinit();
    try stmt.execAlloc(alloc, .{}, args);
}

pub fn first(self: *Self, alloc: std.mem.Allocator, comptime T: type, comptime query: string, args: anytype) !?T {
    const t = tracer.trace(@src(), " {s}", .{query});
    defer t.end();

    var stmt = try self.prepare(query);
    defer stmt.deinit();
    return try stmt.oneAlloc(T, alloc, .{}, args);
}

pub fn prepareDynamic(self: *Self, query: string) !sqlite.DynamicStatement {
    return self.db.prepareDynamic(query);
}

pub fn doesTableExist(self: *Self, alloc: std.mem.Allocator, name: string) !bool {
    const t = tracer.trace(@src(), " {s}", .{name});
    defer t.end();

    for (try self.collect(alloc, string, "select name from sqlite_master where type=? AND name=?", .{ .type = "table", .name = name })) |item| {
        if (std.mem.eql(u8, item, name)) {
            return true;
        }
    }
    return false;
}

pub fn hasColumnWithName(self: *Self, alloc: std.mem.Allocator, comptime table: string, comptime column: string) !bool {
    const t = tracer.trace(@src(), " {s}.{s}", .{ table, column });
    defer t.end();

    for (try pragma.table_info(self, alloc, table)) |item| {
        if (std.mem.eql(u8, item.name, column)) {
            return true;
        }
    }
    return false;
}

pub fn createTable(self: *Self, alloc: std.mem.Allocator, comptime name: []const u8, comptime pk_name: []const u8, pk_type: type) !void {
    const t = tracer.trace(@src(), " {s} ({s})", .{ name, pk_name });
    defer t.end();
    try self.exec(alloc, comptime std.fmt.comptimePrint("create table {s}({s} {s} primary key not null)", .{ name, pk_name, nameForType2(pk_type) }), .{});
}

pub fn addColumn(self: *Self, alloc: std.mem.Allocator, comptime table_name: []const u8, comptime col_name: []const u8, T: type) !void {
    const t = tracer.trace(@src(), " {s}.{s}", .{ table_name, col_name });
    defer t.end();
    try self.exec(alloc, comptime std.fmt.comptimePrint("alter table {s} add \"{s}\" {s}", .{ table_name, col_name, nameForType(T) }), .{});
}

pub fn nameForType(T: type) []const u8 {
    if (@typeInfo(T) == .optional) {
        return nameForType2(@typeInfo(T).optional.child);
    }
    return nameForType2(T) ++ " not null";
}

pub fn nameForType2(T: type) []const u8 {
    const tinfo = @typeInfo(T);

    if (comptime extras.isZigString(T)) {
        return "text";
    }
    if (tinfo == .@"struct") {
        const info = tinfo.@"struct";
        if (@hasDecl(T, "BaseType")) return nameForType2(T.BaseType);
        if (info.layout == .@"packed") return nameForType2(info.backing_integer.?);
    }
    if (tinfo == .@"enum") {
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
    pub fn table_info(self: *Self, alloc: std.mem.Allocator, comptime name: string) ![]const Pragma.TableInfo {
        const t = tracer.trace(@src(), " {s}", .{name});
        defer t.end();

        return try self.collect(alloc, Pragma.TableInfo, "pragma table_info(" ++ name ++ ")", .{});
    }
};
