//! Reference: https://www.postgresql.org/docs/current/protocol.html
//! Implements version 3.0 (PostgreSQL 7.4 and later)

const std = @import("std");
const builtin = @import("builtin");
const url = @import("url");
const net = @import("net");
const nio = @import("nio");
const extras = @import("extras");
const tracer = @import("tracer");

const sys = switch (builtin.target.os.tag) {
    .linux => @import("sys-linux"),
    else => unreachable,
};

const Driver = @This();

mutex: std.Thread.Mutex,
conn: net.Stream,
bufw: nio.BufferedWriter(4096, net.Stream),
bufr: nio.BufferedReader(4096, net.Stream),

pub fn connect(allocator: std.mem.Allocator, connect_s: [:0]const u8) !Driver {
    std.log.scoped(.zorm).info("connecting to {s} @ {s}", .{ "postgresql", connect_s });
    const connect_url = try url.URL.parse(allocator, connect_s, null);
    defer allocator.free(connect_url.href);

    const addr: net.Address = try .fromUrl(&connect_url, allocator);
    const conn = try addr.tcpConnect();
    errdefer conn.close();

    var bufw: nio.BufferedWriter(4096, net.Stream) = .init(conn);
    _ = &bufw;

    var bufr: nio.BufferedReader(4096, net.Stream) = .init(conn);
    _ = &bufr;

    // https://github.com/postgres/postgres/blob/REL_18_0/src/include/common/scram-common.h#L32-L37
    const nonce_len = 18;
    var cnonce_buf: [nonce_len]u8 = @splat(0);
    var cnonce = try sys.getrandom(&cnonce_buf, 0);
    _ = &cnonce;

    var snonce_b64_buf: [128]u8 = @splat(0);
    var snonce_b64: []u8 = snonce_b64_buf[0..];

    var scram_i_buf: [16]u8 = @splat(0);
    var scram_i_s: []u8 = scram_i_buf[0..];
    var scram_i: u32 = 0;

    const Base64Enc = std.base64.standard.Encoder;
    const Base64Dec = std.base64.standard.Decoder;

    {
        try proto.StartupMessage.write(
            &bufw,
            connect_url.username,
            connect_url.pathname[1..],
        );
        try bufw.flush();
    }
    {
        const t: BackendMessageType = @enumFromInt(try bufr.readByte());
        std.debug.assert(t == .Authentication);
        const auth_len = try bufr.readInt(u32, .big);
        std.log.warn("auth_len={d}", .{auth_len});
        const auth = try bufr.readInt(u32, .big);
        std.log.warn("auth={d}", .{auth});
        switch (auth) {
            10 => { // AuthenticationSASL
                const methods = try bufr.readAlloc(allocator, auth_len - 4 - 4);
                defer allocator.free(methods);
                var methods_iter = std.mem.splitScalar(u8, methods, '\x00');

                while (methods_iter.next()) |method| {
                    const method_z = method.ptr[0..method.len :0];
                    if (method_z.len == 0) break;
                    std.log.warn("method={s}", .{method_z});

                    // https://datatracker.ietf.org/doc/html/rfc7677
                    // https://datatracker.ietf.org/doc/html/rfc5802
                    // https://datatracker.ietf.org/doc/html/rfc4422
                    // Salted Challenge Response Authentication Mechanism (SCRAM) SASL and GSS-API Mechanisms

                    const mechanisms = [_]struct { []const u8, type }{
                        .{ "SCRAM-SHA-256", std.crypto.hash.sha2.Sha256 },
                        .{ "SCRAM-SHA-1", std.crypto.hash.Sha1 },
                    };
                    inline for (&mechanisms) |mechanism| {
                        const name, const Hash = mechanism;
                        const Hmac = std.crypto.auth.hmac.Hmac(Hash);
                        comptime std.debug.assert(Hash.digest_length == Hmac.mac_length);

                        if (std.mem.eql(u8, method_z, name)) {
                            // This is a simple example of a SCRAM-SHA-256 authentication exchange when the client doesn't support channel bindings.
                            // The username 'user' and password 'pencil' are being used.
                            // C: n,,n=user,r=rOprNGfwEbeRWgbNEkqO
                            // S: r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096
                            // C: c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=
                            // S: v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=

                            // SaltedPassword  := Hi(Normalize(password), salt, i)
                            // ClientKey       := HMAC(SaltedPassword, "Client Key")
                            // StoredKey       := H(ClientKey)
                            // AuthMessage     := client-first-message-bare + "," + server-first-message + "," + client-final-message-without-proof
                            // ClientSignature := HMAC(StoredKey, AuthMessage)
                            // ClientProof     := ClientKey XOR ClientSignature
                            // ServerKey       := HMAC(SaltedPassword, "Server Key")
                            // ServerSignature := HMAC(ServerKey, AuthMessage)

                            // server-final-message = (verifier / server-error) ["," extensions]
                            // verifier        = "v=" base64
                            // server-error = "e=" server-error-value
                            // server-error-value = "invalid-encoding" / "extensions-not-supported" / "invalid-proof" / "channel-bindings-dont-match" / "server-does-support-channel-binding" / "channel-binding-not-supported" / "unsupported-channel-binding-type" / "unknown-user" / "invalid-username-encoding" / "no-resources" / "other-error" / server-error-value-ext

                            var cnonce_b64_buf: [Base64Enc.calcSize(nonce_len)]u8 = @splat(0);
                            const cnonce_b64 = Base64Enc.encode(&cnonce_b64_buf, cnonce);

                            var salt_b64_buf: [128]u8 = @splat(0);
                            var salt_b64: []u8 = salt_b64_buf[0..];

                            var salt_buf: [128]u8 = @splat(0);
                            var salt_dec: []u8 = salt_buf[0..];

                            { //->SASLInitialResponse (scram client-first-message)

                                try proto.SASLInitialResponse.writev(
                                    &bufw,
                                    "SCRAM-SHA-256",
                                    &.{
                                        "n",   "",
                                        ",",   "",
                                        ",n=", connect_url.username,
                                        ",r=", cnonce_b64,
                                    },
                                );
                                try bufw.flush();
                            }
                            { //<-AuthenticationSASLContinue (scram server-first-message)
                                const t2: BackendMessageType = @enumFromInt(try bufr.readByte());
                                if (t2 == .ErrorResponse) try printError(&bufr, allocator);
                                std.debug.assert(t2 == .Authentication);
                                const len = try bufr.readInt(u32, .big);
                                const ok = try bufr.readInt(u32, .big);
                                std.debug.assert(ok == 11);
                                const data = try bufr.readAlloc(allocator, len - 8);
                                defer allocator.free(data);
                                var iter = std.mem.splitScalar(u8, data, ',');
                                while (iter.next()) |field| {
                                    if (extras.trimPrefixEnsure(field, "r=")) |r| {
                                        snonce_b64 = snonce_b64_buf[0..r.len];
                                        @memcpy(snonce_b64, r);
                                    }
                                    if (extras.trimPrefixEnsure(field, "s=")) |s| {
                                        salt_b64 = salt_b64_buf[0..s.len];
                                        @memcpy(salt_b64, s);
                                        const s_len = try Base64Dec.calcSizeForSlice(s);
                                        salt_dec = salt_buf[0..s_len];
                                        try Base64Dec.decode(salt_dec, s);
                                    }
                                    if (extras.trimPrefixEnsure(field, "i=")) |i| {
                                        scram_i_s = scram_i_buf[0..i.len];
                                        @memcpy(scram_i_s, i);
                                        scram_i = extras.parseDigits(u32, i, 10) catch 0;
                                        if (scram_i < 4096) return error.WeakParameters;
                                    }
                                }
                            }
                            { //->SASLResponse (scram client-final-message)
                                var buf: [1024]u8 = @splat(0);
                                var fbs: nio.FixedBufferStream([]u8) = .init(&buf);
                                try fbs.writeAll("c=biws");
                                try fbs.writeAll(",r=");
                                try fbs.writeAll(snonce_b64);

                                const salted_password = hi(Hmac, connect_url.password, salt_dec, scram_i);
                                const client_key = hmac(Hmac, &salted_password, "Client Key");
                                const auth_messagev: []const []const u8 = &.{ "n=", connect_url.username, ",r=", cnonce_b64, ",", "r=", snonce_b64, ",s=", salt_b64, ",i=", scram_i_s, ",", fbs.written() };
                                const stored_key = h(Hash, &client_key);
                                const client_signature = hmacv(Hmac, &stored_key, auth_messagev);
                                const client_proof = xor(client_key, client_signature);
                                try fbs.writeAll(",p=");
                                try Base64Enc.encodeWriter(&fbs, &client_proof);

                                try proto.SASLResponse.write(
                                    &bufw,
                                    fbs.written(),
                                );
                                try bufw.flush();
                            }
                            { //<-AuthenticationSASLFinal (scram server-final-message)
                                const t2: BackendMessageType = @enumFromInt(try bufr.readByte());
                                if (t2 == .ErrorResponse) try printError(&bufr, allocator);
                                std.debug.assert(t2 == .Authentication);
                                const len = try bufr.readInt(u32, .big);
                                const ok = try bufr.readInt(u32, .big);
                                std.debug.assert(ok == 12);
                                const data = try bufr.readAlloc(allocator, len - 8);
                                defer allocator.free(data);
                                std.log.warn("AuthenticationSASLFinal = {s}", .{data});
                            }
                            { //<-AuthenticationOk
                                const t2: BackendMessageType = @enumFromInt(try bufr.readByte());
                                if (t2 == .ErrorResponse) try printError(&bufr, allocator);
                                std.debug.assert(t2 == .Authentication);
                                const len = try bufr.readInt(u32, .big);
                                std.debug.assert(len == 8);
                                const ok = try bufr.readInt(u32, .big);
                                std.debug.assert(ok == 0);
                                std.log.warn("AuthenticationOk", .{});
                            }
                        }
                    }
                }
            },
            else => unreachable, // TODO
        }
    }

    return .{
        .mutex = .{},
        .conn = conn,
        .bufw = bufw,
        .bufr = bufr,
    };
}

pub fn close(driver: *Driver) void {
    driver.conn.close();
}

pub fn lock(driver: *Driver) void {
    driver.mutex.lock();
}

pub fn unlock(driver: *Driver) void {
    driver.mutex.unlock();
}

//

pub fn exec(driver: *Driver, alloc: std.mem.Allocator, comptime query: []const u8, args: anytype) !void {
    _ = driver;
    _ = alloc;
    _ = query;
    _ = args;
    @panic("TODO");
}

pub fn first(driver: *Driver, alloc: std.mem.Allocator, comptime T: type, comptime query: []const u8, args: anytype) !?T {
    _ = driver;
    _ = alloc;
    _ = query;
    _ = args;
    @panic("TODO");
}

pub fn collect(driver: *Driver, alloc: std.mem.Allocator, comptime T: type, comptime query: []const u8, args: anytype) ![]T {
    _ = driver;
    _ = alloc;
    _ = query;
    _ = args;
    @panic("TODO");
}

//

pub fn doesTableExist(driver: *Driver, alloc: std.mem.Allocator, name: []const u8) !bool {
    _ = driver;
    _ = alloc;
    _ = name;
    @panic("TODO");
}

pub fn hasColumnWithName(driver: *Driver, alloc: std.mem.Allocator, comptime table: []const u8, comptime column: []const u8) !bool {
    _ = driver;
    _ = alloc;
    _ = table;
    _ = column;
    @panic("TODO");
}

pub fn createTable(driver: *Driver, alloc: std.mem.Allocator, comptime name: []const u8, comptime pk_name: []const u8, pk_type: type) !void {
    _ = driver;
    _ = alloc;
    _ = name;
    _ = pk_name;
    _ = pk_type;
    @panic("TODO");
}

pub fn addColumn(driver: *Driver, alloc: std.mem.Allocator, comptime table_name: []const u8, comptime col_name: []const u8, T: type) !void {
    _ = driver;
    _ = alloc;
    _ = table_name;
    _ = col_name;
    _ = T;
    @panic("TODO");
}

pub fn nameForType(T: type) []const u8 {
    if (@typeInfo(T) == .optional) {
        return nameForType2(T);
    }
    return nameForType2(T) ++ " not null";
}

pub fn nameForType2(T: type) []const u8 {
    _ = T;
    @panic("TODO");
}

//
// CancelRequest
// GSSENCRequest
// SSLRequest
// StartupMessage

pub const BackendMessageType = enum(u8) {
    Authentication = 'R',
    BackendKeyData = 'K',
    BindComplete = '2',
    CloseComplete = '3',
    CommandComplete = 'C',
    CopyData = 'd',
    CopyDone = 'c',
    CopyInResponse = 'G',
    CopyOutResponse = 'H',
    CopyBothResponse = 'W',
    DataRow = 'D',
    EmptyQueryResponse = 'I',
    ErrorResponse = 'E',
    FunctionCallResponse = 'V',
    NegotiateProtocolVersion = 'v',
    NoData = 'n',
    NoticeResponse = 'N',
    NotificationResponse = 'A',
    ParameterDescription = 't',
    ParameterStatus = 'S',
    ParseComplete = '1',
    PortalSuspended = 's',
    ReadyForQuery = 'Z',
    RowDescription = 'T',
};

pub const FrontendMessageType = enum(u8) {
    Bind = 'B',
    Close = 'C',
    CopyData = 'd',
    CopyDone = 'c',
    CopyFail = 'f',
    Describe = 'D',
    Execute = 'E',
    Flush = 'H',
    FunctionCall = 'F',
    GSSResponse = 'p',
    Parse = 'P',
    PasswordMessage = 'p',
    Query = 'Q',
    // SASLInitialResponse = 'p',
    // SASLResponse = 'p',
    Sync = 'S',
    Terminate = 'X',
};

const proto = struct {
    const StartupMessage = struct {
        fn write(writer: anytype, username: []const u8, database: []const u8) !void {
            const length: u32 = 4 + 4 +
                4 + 1 + @as(u32, @intCast(username.len)) + 1 +
                8 + 1 + @as(u32, @intCast(database.len)) + 1 +
                1;
            try writer.writeInt(u32, length, .big);
            try writer.writeAll(&.{ 0, 3, 0, 0 });
            try writer.writevAll(&.{ "user", "\x00", username, "\x00" });
            try writer.writevAll(&.{ "database", "\x00", database, "\x00" });
            try writer.writeAll("\x00");
        }
    };

    const SASLInitialResponse = struct {
        fn writev(writer: anytype, mechanism: []const u8, response_parts: []const []const u8) !void {
            const parts_len = extras.sumLen(u8, response_parts);
            const length: u32 = 4 +
                @as(u32, @intCast(mechanism.len)) + 1 +
                4 +
                @as(u32, @intCast(parts_len)) +
                0;
            try writer.writeAll("p");
            try writer.writeInt(u32, length, .big);
            try writer.writevAll(&.{ mechanism, "\x00" });
            try writer.writeInt(u32, @intCast(parts_len), .big);
            try writer.writevAll(response_parts);
        }
    };

    const SASLResponse = struct {
        fn write(writer: anytype, response: []const u8) !void {
            const length: u32 = 4 +
                @as(u32, @intCast(response.len)) +
                0;
            try writer.writeAll("p");
            try writer.writeInt(u32, length, .big);
            try writer.writeAll(response);
        }
    };
};

fn h(Hash: type, str: []const u8) [Hash.digest_length]u8 {
    var s: Hash = .init(.{});
    s.update(str);
    var out: [Hash.digest_length]u8 = @splat(0);
    s.final(&out);
    return out;
}

fn hmac(Hmac: type, key: []const u8, str: []const u8) [Hmac.mac_length]u8 {
    var s: Hmac = .init(key);
    s.update(str);
    var out: [Hmac.mac_length]u8 = @splat(0);
    s.final(&out);
    return out;
}
fn hmacv(Hmac: type, key: []const u8, strs: []const []const u8) [Hmac.mac_length]u8 {
    var s: Hmac = .init(key);
    for (strs) |str| s.update(str);
    var out: [Hmac.mac_length]u8 = @splat(0);
    s.final(&out);
    return out;
}

fn hi(Hmac: type, str: []const u8, salt: []const u8, i: u32) [Hmac.mac_length]u8 {
    var dk: [Hmac.mac_length]u8 = @splat(0);
    std.crypto.pwhash.pbkdf2(&dk, str, salt, i, Hmac) catch unreachable;
    return dk;
}

fn xor(l: anytype, r: anytype) @TypeOf(l, r) {
    var out: @TypeOf(l, r) = @splat(0);
    for (&l, &r, &out) |a, b, *o| o.* = a ^ b;
    return out;
}

fn printError(bufr: *nio.BufferedReader(4096, net.Stream), allocator: std.mem.Allocator) !noreturn {
    const len = try bufr.readInt(u32, .big);
    const data = try bufr.readAlloc(allocator, len - 4);
    defer allocator.free(data);
    var iter = std.mem.splitScalar(u8, data, 0);
    while (iter.next()) |f| if (f.len > 0) std.log.err("{c}: {s}", .{ f[0], f[1..] });
    std.posix.exit(1);
}
