const std = @import("std");
const zorm = @import("zorm");

// TODO: add more behavior tests that create+update+delete dbs
test {
    std.testing.refAllDeclsRecursive(zorm.engine(.sqlite3));
}
