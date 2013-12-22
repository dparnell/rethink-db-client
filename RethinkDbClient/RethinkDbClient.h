//
//  RethinkDbClient.h
//  RethinkDbClient
//
//  Created by Daniel Parnell on 14/12/2013.
//  Copyright (c) 2013 Daniel Parnell. All rights reserved.
//

#import <Foundation/Foundation.h>

@class RethinkDbClient;
typedef RethinkDbClient* (^RethinkDbJoinPredicate)(RethinkDbClient* left, RethinkDbClient* right);
typedef RethinkDbClient* (^RethinkDbMappingFunction)(RethinkDbClient* row);
typedef RethinkDbClient* (^RethinkDbReductionFunction)(RethinkDbClient* accumulator, RethinkDbClient* value);
typedef RethinkDbClient* (^RethinkDbGroupByFunction)(RethinkDbClient* row);

@interface RethinkDbClient : NSObject

+ (RethinkDbClient*) clientWithURL:(NSURL*)url andError:(NSError**)error;

- (BOOL) close:(NSError**)error;
- (id) run:(NSError**)error;

- (RethinkDbClient*) db: (NSString*)name;


- (RethinkDbClient*) dbCreate:(NSString*)name;
- (RethinkDbClient*) dbDrop:(NSString*)name;
- (RethinkDbClient*) dbList;

- (RethinkDbClient*) tableCreate:(NSString*)name options:(NSDictionary*)options;
- (RethinkDbClient*) tableCreate:(NSString*)name;
- (RethinkDbClient*) tableDrop:(NSString*)name;
- (RethinkDbClient*) tableList;

- (RethinkDbClient*) indexCreate:(NSString*)name;
- (RethinkDbClient*) indexDrop:(NSString*)name;
- (RethinkDbClient*) indexList;
- (RethinkDbClient*) indexStatus:(id)names;
- (RethinkDbClient*) indexWait:(id)names;

- (RethinkDbClient*) table:(NSString*)name options:(NSDictionary*)options;
- (RethinkDbClient*) table:(NSString*)name;

- (RethinkDbClient*) insert:(id)object options:(NSDictionary*)options;
- (RethinkDbClient*) insert:(id)object;

- (RethinkDbClient*) update:(id)object options:(NSDictionary*)options;
- (RethinkDbClient*) update:(id)object;

- (RethinkDbClient*) replace:(id)object options:(NSDictionary*)options;
- (RethinkDbClient*) replace:(id)object;

- (RethinkDbClient*) delete:(NSDictionary*)options;
- (RethinkDbClient*) delete;

- (RethinkDbClient*) sync;

- (RethinkDbClient*) get:(id)key;
- (RethinkDbClient*) getAll:(NSArray*)keys options:(NSDictionary*)options;
- (RethinkDbClient*) getAll:(NSArray *)keys;

- (RethinkDbClient*) between:(id)lower and:(id)upper options:(NSDictionary*)options;
- (RethinkDbClient*) between:(id)lower and:(id)upper;

- (RethinkDbClient*) filter:(id)predicate options:(NSDictionary*)options;
- (RethinkDbClient*) filter:(id)predicate;

- (RethinkDbClient*) row;
- (RethinkDbClient*) row:(NSString*)key;

- (RethinkDbClient*) field:(NSString*)key;

- (RethinkDbClient*) eq:(id)expr;
- (RethinkDbClient*) ne:(id)expr;

- (RethinkDbClient*) gt:(id)expr;
- (RethinkDbClient*) ge:(id)expr;

- (RethinkDbClient*) lt:(id)expr;
- (RethinkDbClient*) le:(id)expr;

- (RethinkDbClient*) not;
- (RethinkDbClient*) and:(id)expr;
- (RethinkDbClient*) or:(id)expr;
- (RethinkDbClient*) any:(NSArray*)expressions;
- (RethinkDbClient*) all:(NSArray*)expressions;

- (RethinkDbClient*) innerJoin:(id)sequence on:(RethinkDbJoinPredicate)predicate;
- (RethinkDbClient*) outerJoin:(id)sequence on:(RethinkDbJoinPredicate)predicate;
- (RethinkDbClient*) eqJoin:(NSString*)key to:(id)sequence options:(NSDictionary*)options;
- (RethinkDbClient*) eqJoin:(NSString*)key to:(id)sequence;
- (RethinkDbClient*) zip;

- (RethinkDbClient*) map:(RethinkDbMappingFunction)function;
- (RethinkDbClient*) withFields:(NSArray*)fields;
- (RethinkDbClient*) concatMap:(RethinkDbMappingFunction)function;
- (RethinkDbClient*) orderBy:(id)order;
- (RethinkDbClient*) skip:(NSInteger)count;
- (RethinkDbClient*) limit:(NSInteger)count;
- (RethinkDbClient*) slice:(NSInteger)start to:(NSInteger)end;
- (RethinkDbClient*) nth:(NSInteger)index;
- (RethinkDbClient*) indexesOf:(id)datum;
- (RethinkDbClient*) indexesOfPredicate:(RethinkDbMappingFunction)function;
- (RethinkDbClient*) inEmpty;
- (RethinkDbClient*) union:(RethinkDbClient*)sequence;
- (RethinkDbClient*) sample:(NSInteger)count;

- (RethinkDbClient*) reduce:(RethinkDbReductionFunction)function base:(id)base;
- (RethinkDbClient*) reduce:(RethinkDbReductionFunction)function;
- (RethinkDbClient*) count;
- (RethinkDbClient*) dictinct;
- (RethinkDbClient*) group:(RethinkDbGroupByFunction)groupFunction map:(RethinkDbMappingFunction)mapFunction andReduce:(RethinkDbReductionFunction)reduceFunction withBase:(id)base;
- (RethinkDbClient*) group:(RethinkDbGroupByFunction)groupFunction map:(RethinkDbMappingFunction)mapFunction andReduce:(RethinkDbReductionFunction)reduceFunction;
- (RethinkDbClient*) groupBy:(id)columns reduce:(NSDictionary*)reductionObject;
- (RethinkDbClient*) groupByAndCount:(id)columns;
- (RethinkDbClient*) groupBy:(id)columns sum:(NSString*)attribute;
- (RethinkDbClient*) groupBy:(id)columns average:(NSString*)attribute;
- (RethinkDbClient*) contains:(id)values;

@property (retain) NSString* defaultDatabase;
 
@end
