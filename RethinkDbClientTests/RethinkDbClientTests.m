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

@interface RethinkDbClientTests : XCTestCase {
    RethinkDbClient* r;
}

@end

@implementation RethinkDbClientTests

- (void)setUp
{
    [super setUp];
    
    NSURL* url = [NSURL URLWithString: @"rethink://localhost"];
    NSError* error = nil;
    r = [RethinkDbClient clientWithURL: url andError: &error];
}

- (void)tearDown
{
    [r close: nil];
    
    [super tearDown];
}

- (void)testLocalConnection
{
    XCTAssertNotNil(r, @"Connection failed");
}

- (void) testDbMethods
{
    NSError* error = nil;
    XCTAssertNotNil(r, @"Connection failed");
    
    id response = [[r dbCreate: @"testing1234"] run: &error];
    XCTAssertNotNil(response, @"dbCreate failed: %@", error);
    
    NSArray* db_list = [[r dbList] run: &error];
    XCTAssertNotNil(db_list, @"dbList failed failed: %@", error);
    XCTAssert([db_list indexOfObject: @"testing1234"] != NSNotFound, @"dbList should include 'testing1234'");
    
    response = [[r dbDrop: @"testing1234"] run: &error];
    XCTAssertNotNil(response, @"dbDrop failed: %@", error);
}

- (void) testTableMethods {
    NSError* error = nil;
    XCTAssertNotNil(r, @"Connection failed");

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
    NSError* error = nil;
    XCTAssertNotNil(r, @"Connection failed");

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

    query = [table filter: [[[r row: @"number"] gt: [NSNumber numberWithInt: 5]] or: [[r row: @"number"] lt: [NSNumber numberWithInteger: 5]]]];
    rows = [query run: &error];
    XCTAssertNotNil(rows, @"filter failed: %@", error);
    XCTAssertEqual((int)[rows count], 9, @"there should only be 9 rows");
    
    response = [[r tableDrop: @"filterTest"] run: &error];
    XCTAssertNotNil(response, @"tableDrop failed: %@", error);
    
}

- (void) testJoins {
    NSError* error = nil;
    XCTAssertNotNil(r, @"Connection failed");
    
    RethinkDbClient* db = [r db: @"test"];
    NSArray* db_list = [[db tableList] run: &error];
    XCTAssertNotNil(db_list, @"dbList failed failed: %@", error);
    
    if([db_list indexOfObject: @"input_polls"] == NSNotFound || [db_list indexOfObject: @"county_stats"] == NSNotFound) {
        XCTFail(@"both the 'input_polls' and 'county_status' tables must be present");
    }
    
    RethinkDbClient* query = [[[db table: @"input_polls"] innerJoin: [db table: @"county_stats"] on:^RethinkDbClient *(RethinkDbClient *left, RethinkDbClient *right) {
        return [[left field: @"id"] eq: [right field: @"Stname"]];
    }] count];
    
    NSNumber* count = [query run: &error];
    XCTAssertNotNil(count, @"query failed: %@", error);
    
    XCTAssertEqual((int)[count integerValue], 39934, @"count didn't return the right value");
}

- (void) testControlStructures {
    NSError* error = nil;
    XCTAssertNotNil(r, @"Connection failed");
    RethinkDbClient* db = [r db: @"test"];
    
    RethinkDbClient* query = [db do:^RethinkDbClient *(NSArray *arguments) {
        RethinkDbClient* arg1 = [arguments objectAtIndex: 0];
        RethinkDbClient* arg2 = [arguments objectAtIndex: 1];
        
        return [arg1 add: arg2];
    } withArguments: [NSArray arrayWithObjects: [NSNumber numberWithInt: 3], [NSNumber numberWithInt: 4], nil]];
    
    id response = [query run: &error];
    XCTAssertNotNil(response, @"query failed: %@", error);
    XCTAssertEqualObjects(response, [NSNumber numberWithInt: 7], @"the result should be 7");
}

@end
