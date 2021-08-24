const std = @import("std");

const EngineType = enum {
    sqlite3,
};

pub fn engine(etype: EngineType) type {
    return switch (etype) {
        .sqlite3 => @import("./sqlite3.zig"),
    };
}

pub const backer = struct {
    pub const sqlite = @import("sqlite");
};
