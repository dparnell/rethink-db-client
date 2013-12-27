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
@protocol RethinkDBDateTime;

typedef id <RethinkDBRunnable> (^RethinkDbJoinPredicate)(id <RethinkDBSequence> left, id <RethinkDBSequence> right);
typedef id <RethinkDBRunnable> (^RethinkDbMappingFunction)(id <RethinkDBObject> row);
typedef id <RethinkDBRunnable> (^RethinkDbReductionFunction)(id <RethinkDBObject> accumulator, id <RethinkDBObject> value);
typedef id <RethinkDBRunnable> (^RethinkDbGroupByFunction)(id <RethinkDBObject> row);
typedef id <RethinkDBRunnable> (^RethinkDbExpressionFunction)(NSArray* arguments);

@protocol RethinkDBRunnable <NSObject>

- (id) run:(NSError**)error;
- (id <RethinkDBObject>) row;
- (id <RethinkDBObject>) row:(NSString*)key;

- (id <RethinkDBObject>) do:(RethinkDbExpressionFunction)expression withArguments:(NSArray*)arguments;

@end

@protocol RethinkDBObject <RethinkDBRunnable>

- (id <RethinkDBSequence>) pluck:(id)fields;
- (id <RethinkDBSequence>) without:(id)fields;
- (id <RethinkDBSequence>) merge:(id)object;

- (id <RethinkDBArray>) match:(NSString*)regex;

- (id <RethinkDBObject>) add:(id)expr;
- (id <RethinkDBObject>) sub:(id)expr;
- (id <RethinkDBObject>) mul:(id)expr;
- (id <RethinkDBObject>) div:(id)expr;
- (id <RethinkDBObject>) mod:(id)expr;
- (id <RethinkDBObject>) eq:(id)expr;
- (id <RethinkDBObject>) ne:(id)expr;
- (id <RethinkDBObject>) gt:(id)expr;
- (id <RethinkDBObject>) ge:(id)expr;
- (id <RethinkDBObject>) lt:(id)expr;
- (id <RethinkDBObject>) le:(id)expr;
- (id <RethinkDBObject>) not;
- (id <RethinkDBObject>) and:(id)expr;
- (id <RethinkDBObject>) or:(id)expr;
- (id <RethinkDBObject>) any:(NSArray*)expressions;
- (id <RethinkDBObject>) all:(NSArray*)expressions;

- (id <RethinkDBDateTime>) now;
- (id <RethinkDBDateTime>) timeWithYear:(NSInteger)year month:(NSInteger)month day:(NSInteger)day timezone:(NSString*)time_zone;
- (id <RethinkDBDateTime>) timeWithYear:(NSInteger)year month:(NSInteger)month day:(NSInteger)day hour:(NSInteger)hour minute:(NSInteger)minute seconds:(NSInteger)seconds timezone:(NSString*)time_zone;
- (id <RethinkDBDateTime>) time:(NSDate*)date;
- (id <RethinkDBDateTime>) epochTime:(id)seconds;
- (id <RethinkDBDateTime>) ISO8601:(id)time;

- (id <RethinkDBObject>) branch:(id <RethinkDBObject>) test then:(id <RethinkDBObject>) then otherwise:(id <RethinkDBObject>) otherwise;
- (id <RethinkDBObject>) forEach:(RethinkDbMappingFunction)function;
- (id <RethinkDBObject>) error:(id)message;
- (id <RethinkDBObject>) error;
- (id <RethinkDBObject>) default:(id)value;
- (id <RethinkDBObject>) expr:(id)value;
- (id <RethinkDBObject>) js:(NSString*)script;
- (id <RethinkDBObject>) coerceTo:(NSString*)type;
- (id <RethinkDBObject>) typeOf;
- (id <RethinkDBObject>) info;
- (id <RethinkDBObject>) json:(NSString*)json;

@end

@protocol RethinkDBDateTime <RethinkDBObject>

- (id <RethinkDBObject>) inTimezone:(id)time_zone;
- (id <RethinkDBObject>) timezone;
- (id <RethinkDBObject>) during:(id)from to:(id)to options:(NSDictionary*)options;
- (id <RethinkDBObject>) during:(id)from to:(id)to;
- (id <RethinkDBDateTime>) date;
- (id <RethinkDBDateTime>) timeOfDay;
- (id <RethinkDBObject>) year;
- (id <RethinkDBObject>) month;
- (id <RethinkDBObject>) day;
- (id <RethinkDBObject>) dayOfWeek;
- (id <RethinkDBObject>) dayOfYear;
- (id <RethinkDBObject>) hours;
- (id <RethinkDBObject>) minutes;
- (id <RethinkDBObject>) seconds;
- (id <RethinkDBObject>) toISO8601;
- (id <RethinkDBObject>) toEpochTime;

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

- (id <RethinkDBSequence>) append:(id)object;
- (id <RethinkDBSequence>) prepend:(id)object;
- (id <RethinkDBSequence>) difference:id;
- (id <RethinkDBSequence>) setInsert:(id)value;
- (id <RethinkDBSequence>) setUnion:(id)array;
- (id <RethinkDBSequence>) setIntersection:(id)array;
- (id <RethinkDBSequence>) setDifference:(id)array;
- (id <RethinkDBSequence>) field:(NSString*)key;
- (id <RethinkDBSequence>) hasFields:(id)fields;
- (id <RethinkDBSequence>) insert:(id)object at:(NSUInteger)index;
- (id <RethinkDBSequence>) splice:(id)objects at:(NSUInteger)index;
- (id <RethinkDBSequence>) deleteAt:(NSUInteger)index to:(NSUInteger)end_index;
- (id <RethinkDBSequence>) deleteAt:(NSUInteger)index;
- (id <RethinkDBSequence>) changeAt:(NSUInteger)index value:(id)value;
- (id <RethinkDBArray>) keys;
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

@interface RethinkDbClient : NSObject <RethinkDBRunnable, RethinkDBObject, RethinkDBSequence, RethinkDBArray, RethinkDBStream, RethinkDBTable, RethinkDBDateTime, RethinkDBDatabase>

+ (RethinkDbClient*) clientWithURL:(NSURL*)url andError:(NSError**)error;

- (BOOL) close:(NSError**)error;

- (id <RethinkDBDatabase>) db: (NSString*)name;
- (id <RethinkDBObject>) dbCreate:(NSString*)name;
- (id <RethinkDBObject>) dbDrop:(NSString*)name;
- (id <RethinkDBArray>) dbList;

@end
