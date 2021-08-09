const std = @import("std");
const zorm = @import("zorm");

const string = []const u8;
const ULID = string;
const Time = string;

const Package = struct {
    id: u64,
    uuid: ULID,
    owner: ULID,
    name: string,
    created_on: Time,
    remote: u64,
    remote_id: string,
    remote_name: string,
    description: string,
    license: string,
    latest_version: string,
    hook_secret: string,
    star_count: u64,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const alloc = &gpa.allocator;

    var db = try zorm.engine(.sqlite3).connect("/home/snuc/dev/zig-zorm/access.db");
    var list = try db.collect(alloc, Package, "select * from packages order by star_count desc limit 25");

    for (list) |item| {
        std.log.info("{s}", .{item.remote_name});
    }
}
