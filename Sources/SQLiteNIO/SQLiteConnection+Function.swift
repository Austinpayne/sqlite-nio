import CSQLite
import NIO

public extension SQLiteConnection {
    /// Create a new application-defined SQL function.
    ///
    /// See also: https://www.sqlite.org/appfunc.html
    func createScalarFunction(
        named name: String,
        argumentCount: Int,
        _ body: @escaping SQLiteScalarFunction
    ) -> EventLoopFuture<Void> {
        // We need an unmanaged reference to pass in our body function but we are now responsible
        // for managing the reference count. To prevent leaks we:
        // 1. Get an unmanaged context and increment the reference count (passRetained) so the object stays alive during `handleFunction`.
        // 2. `handleFunction` may be called multiple times so do not decrement the reference count (takeUnretainedValue).
        // 3. Manually release the context in `deinitFunction` to balance out the retain in 1.
        let functionContext = SQLiteFunctionContext(body)
        let unmanagedContext = Unmanaged<SQLiteFunctionContext>.passRetained(functionContext).toOpaque()

        let promise = eventLoop.makePromise(of: Void.self)
        threadPool.submit { _ in
            let status = sqlite3_create_function_v2(
                self.handle,
                name,
                Int32(argumentCount),
                SQLITE_UTF8,
                unmanagedContext,
                handleFunction,
                nil,
                nil,
                deinitFunction
            )

            if status == SQLITE_OK {
                promise.succeed(())
            } else {
                promise.fail(SQLiteError(statusCode: status, connection: self))
            }
        }
        return promise.futureResult
    }
}

public typealias SQLiteScalarFunction = ([SQLiteData?]) -> SQLiteDataConvertible?

/// Wrapper around a `SQLiteScalarFunction` so that we can pass it into a C
/// function parameter (i.e. into `sqlite3_create_function_v2`).
private class SQLiteFunctionContext {
    let body: SQLiteScalarFunction

    init(_ body: @escaping SQLiteScalarFunction) {
        self.body = body
    }
}

/// Entrypoint for the `xFunc` parameter in `sqlite3_create_function_v2`
/// which unwraps C SQLite types into swift and calls user provided function.
private func handleFunction(
    context: OpaquePointer?,
    argc: Int32,
    argv: UnsafeMutablePointer<OpaquePointer?>?
) -> Void {
    guard argc >= 0, let userData = sqlite3_user_data(context) else {
        return
    }
    let functionContext = Unmanaged<SQLiteFunctionContext>.fromOpaque(userData).takeUnretainedValue()

    let swiftArgs = (0 ..< argc).map { index -> SQLiteData? in
        guard let value = argv?.advanced(by: Int(index)).pointee else {
            return nil
        }

        switch sqlite3_value_type(value) {
            case SQLITE_INTEGER:
                return .integer(Int(sqlite3_value_int(value)))
            case SQLITE_FLOAT:
                return .float(sqlite3_value_double(value))
            case SQLITE_TEXT:
                guard let bytes = sqlite3_value_text(value) else {
                    return nil
                }
                return .text(String(cString: bytes))
            case SQLITE_BLOB:
                let length = Int(sqlite3_value_bytes(value))
                var buffer = ByteBufferAllocator().buffer(capacity: length)
                guard let blobPointer = sqlite3_value_blob(value) else {
                    return nil
                }
                buffer.writeBytes(UnsafeBufferPointer(
                    start: blobPointer.assumingMemoryBound(to: UInt8.self),
                    count: length
                ))
                return .blob(buffer)
            case SQLITE_NULL:
                return .null
            default:
                return nil
        }
    }

    let data = functionContext.body(swiftArgs)

    switch data?.sqliteData {
        case let .blob(value):
            let count = Int32(value.readableBytes)
            value.withUnsafeReadableBytes { pointer in
                sqlite3_result_blob(context, pointer.baseAddress, count, SQLITE_TRANSIENT)
            }
        case let .float(value):
            sqlite3_result_double(context, value)
        case let .integer(value):
            sqlite3_result_int64(context, Int64(value))
        case .null, .none:
            sqlite3_result_null(context)
        case let .text(value):
            let strlen = Int32(value.utf8.count)
            sqlite3_result_text(context, value, strlen, SQLITE_TRANSIENT)
    }
}

/// Entrypoint for the `xDestroy` parameter in `sqlite3_create_function_v2`
/// which properly releases memory associated with a `SQLiteFunctionContext`.
private func deinitFunction(userData: UnsafeMutableRawPointer?) -> Void {
    guard let userData = userData else {
        return
    }
    Unmanaged<SQLiteFunctionContext>.fromOpaque(userData).release()
}
