//
//  RethinkDbClientTests.m
//  RethinkDbClientTests
//
//  Created by Daniel Parnell on 14/12/2013.
//  Copyright (c) 2013 Daniel Parnell. All rights reserved.
//

#import <XCTest/XCTest.h>
#import "RethinkDbClient.h"

@interface RethinkDbClient (Private)

- (id) term;

@end

@interface RethinkDbClientTests : XCTestCase

@end

@implementation RethinkDbClientTests

- (void)setUp
{
    [super setUp];
    // Put setup code here. This method is called before the invocation of each test method in the class.
}

- (void)tearDown
{
    // Put teardown code here. This method is called after the invocation of each test method in the class.
    [super tearDown];
}

- (void)testLocalConnection
{
    NSURL* url = [NSURL URLWithString: @"rethink://localhost"];
    NSError* error = nil;
    RethinkDbClient* client = [RethinkDbClient clientWithURL: url andError: &error];
    XCTAssertNotNil(client, @"Connection failed: %@", error);
}

- (void) testDbMethods
{
    NSURL* url = [NSURL URLWithString: @"rethink://localhost"];
    NSError* error = nil;
    RethinkDbClient* r = [RethinkDbClient clientWithURL: url andError: &error];
    XCTAssertNotNil(r, @"Connection failed: %@", error);
    
    id response = [[r dbCreate: @"testing1234"] run: &error];
    XCTAssertNotNil(response, @"dbCreate failed: %@", error);
    
    NSArray* db_list = [[r dbList] run: &error];
    XCTAssertNotNil(response, @"dbList failed failed: %@", error);
    XCTAssert([db_list indexOfObject: @"testing1234"] != NSNotFound, @"dbList should include 'testing1234'");
    
    response = [[r dbDrop: @"testing1234"] run: &error];
    XCTAssertNotNil(response, @"dbDrop failed: %@", error);
}

- (void) testTableMethods {
    NSURL* url = [NSURL URLWithString: @"rethink://localhost"];
    NSError* error = nil;
    RethinkDbClient* r = [[RethinkDbClient clientWithURL: url andError: &error] db: @"test"];
    XCTAssertNotNil(r, @"Connection failed: %@", error);

    id response = [[r tableCreate: @"blah"] run: &error];
    XCTAssertNotNil(response, @"createTable failed: %@", error);
    
    NSArray* tables = [[r tableList] run: &error];
    XCTAssertNotNil(tables, @"tableList failed: %@", error);
    XCTAssert([tables indexOfObject: @"blah"] != NSNotFound, @"table should include 'blah'");
  
    response = [[[r table: @"blah"] insert: [NSDictionary dictionaryWithObject: @"Daniel" forKey: @"name"]] run: &error];
    XCTAssertNotNil(response, @"insert failed: %@", error);
    XCTAssertEqualObjects([response objectForKey: @"inserted"], [NSNumber numberWithInt: 1], @"one document should have been inserted: %@", response);
    XCTAssertNotNil([response objectForKey: @"generated_keys"], @"generated keys not found: %@", response);
    
    response = [[r tableDrop: @"blah"] run: &error];
    XCTAssertNotNil(response, @"tableDrop failed: %@", error);
}

- (void) testFilters {
    NSURL* url = [NSURL URLWithString: @"rethink://localhost"];
    NSError* error = nil;
    RethinkDbClient* r = [[RethinkDbClient clientWithURL: url andError: &error] db: @"test"];
    XCTAssertNotNil(r, @"Connection failed: %@", error);

    id response = [[r tableCreate: @"filterTest"] run: &error];
    XCTAssertNotNil(response, @"createTable failed: %@", error);

    RethinkDbClient* table = [r table: @"filterTest"];
    
    for(int i=0; i<10; i++) {
        response = [[table insert: [NSDictionary dictionaryWithObject: [NSNumber numberWithInt: i] forKey: @"number"]] run: &error];
        XCTAssertNotNil(response, @"insert failed: %@", error);
        XCTAssertEqualObjects([response objectForKey: @"inserted"], [NSNumber numberWithInt: 1], @"one document should have been inserted: %@", response);
        XCTAssertNotNil([response objectForKey: @"generated_keys"], @"generated keys not found: %@", response);
    }
    
    RethinkDbClient* query = [table filter: [[r row: @"number"] gt: [NSNumber numberWithInt: 5]]];
    
    NSArray* rows = [query run: &error];
    XCTAssertNotNil(rows, @"filter failed: %@", error);
    XCTAssertEqual((int)[rows count], 4, @"there should only be 4 rows");

    query = [table filter: [[[r row: @"number"] gt: [NSNumber numberWithInt: 5]] not]];
    
    rows = [query run: &error];
    XCTAssertNotNil(rows, @"filter failed: %@", error);
    XCTAssertEqual((int)[rows count], 6, @"there should only be 6 rows");

    response = [[r tableDrop: @"filterTest"] run: &error];
    XCTAssertNotNil(response, @"tableDrop failed: %@", error);
    
}

@end
