const std = @import("std");
const string = []const u8;
const sqlite = @import("sqlite");

const Self = @This();

db: sqlite.Db = undefined,
mutex: std.Thread.Mutex,

pub fn connect(path: [:0]const u8) !Self {
    return Self{
        .db = try sqlite.Db.init(.{
            .mode = sqlite.Db.Mode{ .File = path },
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

fn prepare(self: *Self, comptime query: string) !sqlite.StatementType(.{}, query) {
    return self.db.prepare(query) catch |err| switch (err) {
        error.SQLiteError => std.debug.panic("{s}", .{self.db.getDetailedError()}),
        else => return err,
    };
}

pub fn collect(self: *Self, alloc: *std.mem.Allocator, comptime T: type, comptime query: string, args: anytype) ![]const T {
    var stmt = try self.prepare(query);
    defer stmt.deinit();
    var iter = try stmt.iteratorAlloc(T, alloc, args);
    var list = std.ArrayList(T).init(alloc);
    while (try iter.nextAlloc(alloc, .{})) |row| {
        try list.append(row);
    }
    return list.toOwnedSlice();
}

pub fn exec(self: *Self, alloc: *std.mem.Allocator, comptime query: string, args: anytype) !void {
    var stmt = try self.prepare(query);
    defer stmt.deinit();
    try stmt.execAlloc(alloc, .{}, args);
}

pub fn first(self: *Self, alloc: *std.mem.Allocator, comptime T: type, comptime query: string, args: anytype) !?T {
    var stmt = try self.prepare(query);
    defer stmt.deinit();
    return try stmt.oneAlloc(T, alloc, .{}, args);
}

pub fn doesTableExist(self: *Self, alloc: *std.mem.Allocator, name: string) !bool {
    for (try self.collect(alloc, string, "select name from sqlite_master where type=? AND name=?", .{ .type = "table", .name = name })) |item| {
        if (std.mem.eql(u8, item, name)) {
            return true;
        }
    }
    return false;
}

pub fn hasColumnWithName(self: *Self, alloc: *std.mem.Allocator, comptime table: string, comptime column: string) !bool {
    for (try pragma.table_info(self, alloc, table)) |item| {
        if (std.mem.eql(u8, item.name, column)) {
            return true;
        }
    }
    return false;
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
    pub fn table_info(self: *Self, alloc: *std.mem.Allocator, comptime name: string) ![]const Pragma.TableInfo {
        return try self.collect(alloc, Pragma.TableInfo, "pragma table_info(" ++ name ++ ")", .{});
    }
};
