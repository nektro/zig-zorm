const std = @import("std");
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

pub fn collect(self: *Self, alloc: *std.mem.Allocator, comptime T: type, comptime query: []const u8) ![]const T {
    var stmt = try self.db.prepare(query);
    defer stmt.deinit();
    var iter = try stmt.iterator(T, .{});
    var list = std.ArrayList(T).init(alloc);
    while (try iter.nextAlloc(alloc, .{})) |row| {
        try list.append(row);
    }
    return list.toOwnedSlice();
}

pub fn exec(self: *Self, comptime query: []const u8, args: anytype) !void {
    var stmt = try self.db.prepare(query);
    defer stmt.deinit();
    try stmt.exec(args);
}
