import XCTest
import SQLiteNIO

final class SQLiteNIOTests: XCTestCase {
    func testBasicConnection() throws {
        let conn = try SQLiteConnection.open(storage: .memory, threadPool: self.threadPool, on: self.eventLoop).wait()
        defer { try! conn.close().wait() }

        let rows = try conn.query("SELECT sqlite_version()").wait()
        print(rows)
    }

    func testZeroLengthBlob() throws {
        let conn = try SQLiteConnection.open(storage: .memory, threadPool: self.threadPool, on: self.eventLoop).wait()
        defer { try! conn.close().wait() }

        let rows = try conn.query("SELECT zeroblob(0) as zblob").wait()
        print(rows)
    }

    func testTimestampStorage() throws {
        let conn = try SQLiteConnection.open(storage: .memory, threadPool: self.threadPool, on: self.eventLoop).wait()
        defer { try! conn.close().wait() }

        let date = Date()
        let rows = try conn.query("SELECT ? as date", [date.sqliteData!]).wait()
        XCTAssertEqual(rows[0].column("date"), .float(date.timeIntervalSince1970))
        XCTAssertEqual(Date(sqliteData: rows[0].column("date")!)?.description, date.description)
    }

    func testTimestampStorageInDateColumnIntegralValue() throws {
        let conn = try SQLiteConnection.open(storage: .memory, threadPool: self.threadPool, on: self.eventLoop).wait()
        defer { try! conn.close().wait() }

        let date = Date(timeIntervalSince1970: 42)
        // This is how a column of type .date is created when using Vapor’s
        // scheme table creation.
        _ = try conn.query(#"CREATE TABLE "test" ("date" DATE NOT NULL);"#).wait()
        _ = try conn.query(#"INSERT INTO test (date) VALUES (?);"#, [date.sqliteData!]).wait()
        let rows = try conn.query("SELECT * FROM test;").wait()
        XCTAssertTrue(rows[0].column("date") == .float(date.timeIntervalSince1970) || rows[0].column("date") == .integer(Int(date.timeIntervalSince1970)))
        XCTAssertEqual(Date(sqliteData: rows[0].column("date")!)?.description, date.description)
    }

    func testDuplicateColumnName() throws {
        let conn = try SQLiteConnection.open(storage: .memory, threadPool: self.threadPool, on: self.eventLoop).wait()
        defer { try! conn.close().wait() }

        let rows = try conn.query("SELECT 1 as foo, 2 as foo").wait()
        var i = 0
        for column in rows[0].columns {
            XCTAssertEqual(column.name, "foo")
            i += column.data.integer!
        }
        XCTAssertEqual(i, 3)
        XCTAssertEqual(rows[0].column("foo")?.integer, 1)
        XCTAssertEqual(rows[0].columns.filter { $0.name == "foo" }[1].data.integer, 2)
    }

    func testCreateFunction() throws {
        let testFunctionName = "testFn"
        let conn = try SQLiteConnection.open(storage: .memory, threadPool: threadPool, on: eventLoop).wait()
        defer { try! conn.close().wait() }

        // Function doesn't exist yet
        XCTAssertThrowsError(try conn.query("SELECT \(testFunctionName)()").wait()) { error in
            guard let sqliteError = error as? SQLiteError else {
                XCTFail("Expected an SQLiteError")
                return
            }
            XCTAssertEqual(sqliteError.message, "no such function: \(testFunctionName)")
        }

        // Create the function
        try conn.createScalarFunction(named: testFunctionName, argumentCount: 5, { args in
            XCTAssertEqual(args, [
                .integer(1),
                .float(2.0),
                .text("3"),
                .blob(ByteBuffer(bytes: [0x04])),
                .null
            ])
            return "ok"
        }).wait()

        // Pass one of each type to make sure we got decoding right
        let rows = try conn.query("SELECT \(testFunctionName)(1, 2.0, '3', x'04', NULL) AS result").wait()
        XCTAssertEqual(rows.count, 1)
        XCTAssertEqual(rows[0].column("result")?.string, "ok")
    }

    var threadPool: NIOThreadPool!
    var eventLoopGroup: EventLoopGroup!
    var eventLoop: EventLoop {
        return self.eventLoopGroup.next()
    }

    override func setUp() {
        self.threadPool = .init(numberOfThreads: 8)
        self.threadPool.start()
        self.eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 8)
    }

    override func tearDown() {
        try! self.threadPool.syncShutdownGracefully()
        try! self.eventLoopGroup.syncShutdownGracefully()
    }
}
