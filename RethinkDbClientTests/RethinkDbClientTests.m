//
//  RethinkDbClientTests.m
//  RethinkDbClientTests
//
//  Created by Daniel Parnell on 14/12/2013.
//  Copyright (c) 2013 Daniel Parnell. All rights reserved.
//

#import <XCTest/XCTest.h>
#import "RethinkDbClient.h"

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
    
    response = [[r tableDrop: @"blah"] run: &error];
    XCTAssertNotNil(response, @"createDrop failed: %@", error);
}

@end
