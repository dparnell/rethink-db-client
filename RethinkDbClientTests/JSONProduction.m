//
//  JSONProduction.m
//  RethinkDbClient
//
//  Created by Daniel Parnell on 26/04/2016.
//  Copyright © 2016 Daniel Parnell. All rights reserved.
//

#import <XCTest/XCTest.h>
#import "RethinkDbClient.h"
#import "RethinkDbClient-Private.h"
#import "QL2+JSON.h"

@interface JSONProduction : XCTestCase

@end

@implementation JSONProduction

- (void)setUp {
    [super setUp];
    // Put setup code here. This method is called before the invocation of each test method in the class.
}

- (void)tearDown {
    // Put teardown code here. This method is called after the invocation of each test method in the class.
    [super tearDown];
}

- (void)testJsonGeneration {
    RethinkDbClient *r = [[RethinkDbClient alloc] initWithConnection: nil];
    
    RethinkDbClient *num = (RethinkDbClient*)[r expr: [NSNumber numberWithDouble: 1234.4567]];
    Query *query = [num query];
    NSData *json = [query toJSON];
    NSLog(@"json data = %@", json);
    NSLog(@"json = %@", [[NSString alloc] initWithData: json encoding: NSUTF8StringEncoding]);
}

@end
