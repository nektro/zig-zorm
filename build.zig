const std = @import("std");
const deps = @import("./deps.zig");

pub fn build(b: *std.build.Builder) void {
    var target = b.standardTargetOptions(.{});
    if (target.isGnuLibC()) target.setGnuLibCVersion(2, 28, 0);
    const mode = b.option(std.builtin.Mode, "mode", "") orelse .Debug;

    const exe = b.addExecutable(.{
        .name = "zig-zorm",
        .root_source_file = .{ .path = "src/main.zig" },
        .target = target,
        .optimize = mode,
    });
    deps.addAllTo(exe);
    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const run_step = b.step("run", "Run the app");
    run_step.dependOn(&run_cmd.step);
}
