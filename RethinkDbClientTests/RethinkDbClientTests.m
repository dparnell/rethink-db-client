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

@end
