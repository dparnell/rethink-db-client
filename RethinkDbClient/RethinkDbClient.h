//
//  RethinkDbClient.h
//  RethinkDbClient
//
//  Created by Daniel Parnell on 14/12/2013.
//  Copyright (c) 2013 Daniel Parnell. All rights reserved.
//

#import <Foundation/Foundation.h>

@protocol RethinkDBRunnable;
@protocol RethinkDBSequence;
@protocol RethinkDBObject;
@protocol RethinkDBArray;

typedef id <RethinkDBRunnable> (^RethinkDbJoinPredicate)(id <RethinkDBSequence> left, id <RethinkDBSequence> right);
typedef id <RethinkDBRunnable> (^RethinkDbMappingFunction)(id <RethinkDBObject> row);
typedef id <RethinkDBRunnable> (^RethinkDbReductionFunction)(id <RethinkDBObject> accumulator, id <RethinkDBObject> value);
typedef id <RethinkDBRunnable> (^RethinkDbGroupByFunction)(id <RethinkDBObject> row);
typedef id <RethinkDBRunnable> (^RethinkDbExpressionFunction)(NSArray* arguments);

@protocol RethinkDBRunnable <NSObject>

- (id) run:(NSError**)error;
- (id <RethinkDBObject>) row;
- (id <RethinkDBObject>) row:(NSString*)key;

@end

@protocol RethinkDBObject <RethinkDBRunnable>

- (RethinkDbClient*) pluck:(id)fields;
- (RethinkDbClient*) without:(id)fields;
- (RethinkDbClient*) merge:(id)object;

@end

@protocol RethinkDBSequence <RethinkDBObject>

- (id <RethinkDBSequence>) filter:(id)predicate options:(NSDictionary*)options;
- (id <RethinkDBSequence>) filter:(id)predicate;
- (id <RethinkDBSequence>) innerJoin:(id <RethinkDBSequence>)sequence on:(RethinkDbJoinPredicate)predicate;
- (id <RethinkDBSequence>) outerJoin:(id <RethinkDBSequence>)sequence on:(RethinkDbJoinPredicate)predicate;
- (id <RethinkDBSequence>) eqJoin:(NSString*)key to:(id <RethinkDBSequence>)sequence options:(NSDictionary*)options;
- (id <RethinkDBSequence>) eqJoin:(NSString*)key to:(id <RethinkDBSequence>)sequence;
- (id <RethinkDBSequence>) zip;

- (id <RethinkDBSequence>) map:(RethinkDbMappingFunction)function;
- (id <RethinkDBSequence>) withFields:(NSArray*)fields;
- (id <RethinkDBSequence>) concatMap:(RethinkDbMappingFunction)function;
- (id <RethinkDBSequence>) orderBy:(id)order;
- (id <RethinkDBSequence>) skip:(NSInteger)count;
- (id <RethinkDBSequence>) limit:(NSInteger)count;
- (id <RethinkDBSequence>) slice:(NSInteger)start to:(NSInteger)end;
- (id <RethinkDBObject>) nth:(NSInteger)index;
- (id <RethinkDBArray>) indexesOf:(id)datum;
- (id <RethinkDBArray>) indexesOfPredicate:(RethinkDbMappingFunction)function;
- (id <RethinkDBObject>) inEmpty;
- (id <RethinkDBArray>) union:(id <RethinkDBSequence>)sequence;
- (id <RethinkDBSequence>) sample:(NSInteger)count;

- (id <RethinkDBObject>) reduce:(RethinkDbReductionFunction)function base:(id)base;
- (id <RethinkDBObject>) reduce:(RethinkDbReductionFunction)function;
- (id <RethinkDBObject>) count;
- (id <RethinkDBArray>) distinct;
- (id <RethinkDBObject>) group:(RethinkDbGroupByFunction)groupFunction map:(RethinkDbMappingFunction)mapFunction andReduce:(RethinkDbReductionFunction)reduceFunction withBase:(id)base;
- (id <RethinkDBObject>) group:(RethinkDbGroupByFunction)groupFunction map:(RethinkDbMappingFunction)mapFunction andReduce:(RethinkDbReductionFunction)reduceFunction;
- (id <RethinkDBArray>) groupBy:(id)columns reduce:(NSDictionary*)reductionObject;
- (id <RethinkDBArray>) groupByAndCount:(id)columns;
- (id <RethinkDBArray>) groupBy:(id)columns sum:(NSString*)attribute;
- (id <RethinkDBArray>) groupBy:(id)columns average:(NSString*)attribute;
- (id <RethinkDBObject>) contains:(id)values;


@end

@protocol RethinkDBArray <RethinkDBSequence>


@end

@protocol RethinkDBStream <RethinkDBSequence>


@end

@protocol RethinkDBTable <RethinkDBStream>

- (id <RethinkDBObject>) insert:(id)object options:(NSDictionary*)options;
- (id <RethinkDBObject>) insert:(id)object;
- (id <RethinkDBObject>) update:(id)object options:(NSDictionary*)options;
- (id <RethinkDBObject>) update:(id)object;
- (id <RethinkDBObject>) replace:(id)object options:(NSDictionary*)options;
- (id <RethinkDBObject>) replace:(id)object;
- (id <RethinkDBObject>) delete:(NSDictionary*)options;
- (id <RethinkDBObject>) delete;
- (id <RethinkDBObject>) sync;

- (id <RethinkDBObject>) get:(id)key;
- (id <RethinkDBSequence>) getAll:(NSArray*)keys options:(NSDictionary*)options;
- (id <RethinkDBSequence>) getAll:(NSArray *)keys;
- (id <RethinkDBSequence>) between:(id)lower and:(id)upper options:(NSDictionary*)options;
- (id <RethinkDBSequence>) between:(id)lower and:(id)upper;

@end


@protocol RethinkDBDatabase <RethinkDBRunnable>

- (id <RethinkDBObject>) tableCreate:(NSString*)name options:(NSDictionary*)options;
- (id <RethinkDBObject>) tableCreate:(NSString*)name;
- (id <RethinkDBObject>) tableDrop:(NSString*)name;
- (id <RethinkDBArray>) tableList;
- (id <RethinkDBObject>) indexCreate:(NSString*)name;
- (id <RethinkDBObject>) indexDrop:(NSString*)name;
- (id <RethinkDBArray>) indexList;
- (id <RethinkDBArray>) indexStatus:(id)names;
- (id <RethinkDBArray>) indexWait:(id)names;

- (id <RethinkDBTable>) table:(NSString*)name options:(NSDictionary*)options;
- (id <RethinkDBTable>) table:(NSString*)name;

@end

@interface RethinkDbClient : NSObject

+ (RethinkDbClient*) clientWithURL:(NSURL*)url andError:(NSError**)error;

- (BOOL) close:(NSError**)error;

- (id <RethinkDBDatabase>) db: (NSString*)name;
- (id <RethinkDBObject>) dbCreate:(NSString*)name;
- (id <RethinkDBObject>) dbDrop:(NSString*)name;
- (id <RethinkDBArray>) dbList;




- (RethinkDbClient*) append:(id)object;
- (RethinkDbClient*) prepend:(id)object;
- (RethinkDbClient*) difference:(NSArray*)array;
- (RethinkDbClient*) setInsert:(id)value;
- (RethinkDbClient*) setUnion:(NSArray*)array;
- (RethinkDbClient*) setIntersection:(NSArray*)array;
- (RethinkDbClient*) setDifference:(NSArray*)array;
- (RethinkDbClient*) field:(NSString*)key;
- (RethinkDbClient*) hasFields:(id)fields;
- (RethinkDbClient*) insert:(id)object at:(NSUInteger)index;
- (RethinkDbClient*) splice:(NSArray*)objects at:(NSUInteger)index;
- (RethinkDbClient*) deleteAt:(NSUInteger)index to:(NSUInteger)end_index;
- (RethinkDbClient*) deleteAt:(NSUInteger)index;
- (RethinkDbClient*) changeAt:(NSUInteger)index value:(id)value;
- (RethinkDbClient*) keys;

- (RethinkDbClient*) match:(NSString*)regex;

- (RethinkDbClient*) add:(id)expr;
- (RethinkDbClient*) sub:(id)expr;
- (RethinkDbClient*) mul:(id)expr;
- (RethinkDbClient*) div:(id)expr;
- (RethinkDbClient*) mod:(id)expr;
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

- (RethinkDbClient*) now;
- (RethinkDbClient*) timeWithYear:(NSInteger)year month:(NSInteger)month day:(NSInteger)day timezone:(NSString*)time_zone;
- (RethinkDbClient*) timeWithYear:(NSInteger)year month:(NSInteger)month day:(NSInteger)day hour:(NSInteger)hour minute:(NSInteger)minute seconds:(NSInteger)seconds timezone:(NSString*)time_zone;
- (RethinkDbClient*) time:(NSDate*)date;
- (RethinkDbClient*) epochTime:(id)seconds;
- (RethinkDbClient*) ISO8601:(id)time;
- (RethinkDbClient*) inTimezone:(id)time_zone;
- (RethinkDbClient*) timezone;
- (RethinkDbClient*) during:(id)from to:(id)to options:(NSDictionary*)options;
- (RethinkDbClient*) during:(id)from to:(id)to;
- (RethinkDbClient*) date;
- (RethinkDbClient*) timeOfDay;
- (RethinkDbClient*) year;
- (RethinkDbClient*) month;
- (RethinkDbClient*) day;
- (RethinkDbClient*) dayOfWeek;
- (RethinkDbClient*) dayOfYear;
- (RethinkDbClient*) hours;
- (RethinkDbClient*) minutes;
- (RethinkDbClient*) seconds;
- (RethinkDbClient*) toISO8601;
- (RethinkDbClient*) toEpochTime;

- (RethinkDbClient*) do:(RethinkDbExpressionFunction)expression withArguments:(NSArray*)arguments;
- (RethinkDbClient*) branch:(RethinkDbClient*) test then:(RethinkDbClient*) then otherwise:(RethinkDbClient*) otherwise;
- (RethinkDbClient*) forEach:(RethinkDbMappingFunction)function;
- (RethinkDbClient*) error:(id)message;
- (RethinkDbClient*) error;
- (RethinkDbClient*) default:(id)value;
- (RethinkDbClient*) expr:(id)value;
- (RethinkDbClient*) js:(NSString*)script;
- (RethinkDbClient*) coerceTo:(NSString*)type;
- (RethinkDbClient*) typeOf;
- (RethinkDbClient*) info;
- (RethinkDbClient*) json:(NSString*)json;

@end
