const std = @import("std");

pub const DriverType = enum {
    sqlite3,
    postgresql,
};

pub fn Driver(comptime etype: DriverType) type {
    return switch (etype) {
        .sqlite3 => @import("./sqlite3.zig"),
        .postgresql => @import("./postgresql.zig"),
    };
}

pub fn connect(_type: DriverType, allocator: std.mem.Allocator, connection: [:0]const u8) !Engine {
    return switch (_type) {
        inline else => |t| @unionInit(Engine, @tagName(t), try .connect(allocator, connection)),
    };
}

pub const Engine = union(DriverType) {
    sqlite3: Driver(.sqlite3),
    postgresql: Driver(.postgresql),

    pub fn close(engine: *Engine) void {
        return switch (engine.*) {
            inline else => |*e| e.close(),
        };
    }

    pub fn lock(engine: *Engine) void {
        return switch (engine.*) {
            inline else => |*e| e.lock(),
        };
    }

    pub fn unlock(engine: *Engine) void {
        return switch (engine.*) {
            inline else => |*e| e.unlock(),
        };
    }

    pub fn exec(engine: *Engine, alloc: std.mem.Allocator, comptime query: []const u8, args: anytype) !void {
        return switch (engine.*) {
            inline else => |*e| e.exec(alloc, query, args),
        };
    }

    pub fn first(engine: *Engine, alloc: std.mem.Allocator, comptime T: type, comptime query: []const u8, args: anytype) !?T {
        return switch (engine.*) {
            inline else => |*e| e.first(alloc, T, query, args),
        };
    }

    pub fn collect(engine: *Engine, alloc: std.mem.Allocator, comptime T: type, comptime query: []const u8, args: anytype) ![]T {
        return switch (engine.*) {
            inline else => |*e| e.collect(alloc, T, query, args),
        };
    }

    pub fn collectDyn(engine: *Engine, alloc: std.mem.Allocator, comptime T: type, query: []const u8, args: anytype) ![]T {
        return switch (engine.*) {
            .sqlite3 => |*e| {
                var stmt = try e.prepareDynamic(query);
                defer stmt.deinit();
                var iter = try stmt.iteratorAlloc(T, alloc, args);
                var list = std.ArrayList(T).init(alloc);
                errdefer list.deinit();
                while (try iter.nextAlloc(alloc, .{})) |row| {
                    try list.append(row);
                }
                return list.toOwnedSlice();
            },
            .postgresql => {
                @panic("TODO");
            },
        };
    }

    //

    pub fn doesTableExist(engine: *Engine, alloc: std.mem.Allocator, name: []const u8) !bool {
        return switch (engine.*) {
            inline else => |*e| e.doesTableExist(alloc, name),
        };
    }

    pub fn hasColumnWithName(engine: *Engine, alloc: std.mem.Allocator, comptime table: []const u8, comptime column: []const u8) !bool {
        return switch (engine.*) {
            inline else => |*e| e.hasColumnWithName(alloc, table, column),
        };
    }
};
