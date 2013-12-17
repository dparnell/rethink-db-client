//
//  RethinkDbClient.h
//  RethinkDbClient
//
//  Created by Daniel Parnell on 14/12/2013.
//  Copyright (c) 2013 Daniel Parnell. All rights reserved.
//

#import <Foundation/Foundation.h>

@interface RethinkDbClient : NSObject

+ (RethinkDbClient*) clientWithURL:(NSURL*)url andError:(NSError**)error;

- (RethinkDbClient*) db: (NSString*)name;

- (id) run:(NSError**)error;

- (RethinkDbClient*) dbCreate:(NSString*)name;
- (RethinkDbClient*) dbDrop:(NSString*)name;
- (RethinkDbClient*) dbList;

- (RethinkDbClient*) tableCreate:(NSString*)name options:(NSDictionary*)options;
- (RethinkDbClient*) tableCreate:(NSString*)name;

@property (retain) NSString* defaultDatabase;

@end
