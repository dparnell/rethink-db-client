//
//  RethinkDbClient.m
//  RethinkDbClient
//
//  Created by Daniel Parnell on 14/12/2013.
//  Copyright (c) 2013 Daniel Parnell
//
// Permission is hereby granted, free of charge, to any person
// obtaining a copy of this software and associated documentation
// files (the "Software"), to deal in the Software without
// restriction, including without limitation the rights to use,
// copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following
// conditions:
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
// OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
// HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
// WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
// OTHER DEALINGS IN THE SOFTWARE.
//

#import "RethinkDbClient.h"
#import <ProtocolBuffers/ProtocolBuffers.h>
#import "Ql2.pb.h"

static NSString* rethink_error = @"RethinkDB Error";

#define ERROR(x) if(error) *error = x
#define RETHINK_ERROR(x,y) if(error) *error = [NSError errorWithDomain: rethink_error code: x userInfo: [NSDictionary dictionaryWithObject: y forKey: NSLocalizedDescriptionKey]]
#define CHECK_NULL(x) (x == nil ? [NSNull null] : x)

@interface RethinkDbClient  (Private)

- (id) initWithConnection:(RethinkDbClient*)parent;

@property (retain) Term* term;

@end

@implementation RethinkDbClient {
    int64_t token;
    NSLock* lock;
    NSInteger variable_number;
    
    __strong RethinkDbClient* connection;
    __strong NSInputStream* input_stream;
    __strong NSOutputStream* output_stream;
    __strong PBCodedOutputStream* pb_output_stream;
    __strong PBCodedInputStream* pb_input_stream;
    
    __strong Query* _query;
    __strong Term* _term;
}

#pragma mark -
#pragma mark Initialization

+ (RethinkDbClient*) clientWithURL:(NSURL*)url andError:(NSError**)error {
    return [[RethinkDbClient alloc] initWithURL: url andError: error];
}

- (id) initWithURL:(NSURL*)url andError:(NSError**)error {
    self = [super init];
    
    if(self) {
        NSString* host_name = [url host];
        if(host_name) {
            NSNumber* port = [url port];
            NSInteger port_number;
            
            if(port) {
                port_number = [port integerValue];
            } else {
                port_number = 28015;
            }
            
            NSHost* host = [NSHost hostWithName: host_name];
            if(host) {
                NSInputStream* in_stream = nil;
                NSOutputStream* out_stream = nil;
                
                [NSStream getStreamsToHost: host port: port_number inputStream: &in_stream outputStream: &out_stream];
                
                if(in_stream && out_stream) {
                    NSError* stream_error;
                    NSString* auth_key = [url user];
                    NSData* auth_key_data = [auth_key dataUsingEncoding: NSUTF8StringEncoding];
                    NSMutableData* auth_response = [NSMutableData new];
                    int8_t byte;
                    NSString* auth_response_string;
                    
                    input_stream = in_stream;
                    output_stream = out_stream;
                    
                    pb_output_stream = [PBCodedOutputStream streamWithOutputStream: output_stream];
                    [output_stream open];
                    stream_error = [output_stream streamError];
                    if(stream_error) {
                        ERROR(stream_error);
                        return nil;
                    }
                    pb_input_stream = [PBCodedInputStream streamWithInputStream: input_stream];
                    stream_error = [input_stream streamError];
                    if(stream_error) {
                        ERROR(stream_error);
                        return nil;
                    }
                    
                    // send the protocol version down the socket
                    [pb_output_stream writeRawLittleEndian32: VersionDummy_VersionV02];
                    [pb_output_stream flush];

                    stream_error = [output_stream streamError];
                    if(stream_error) {
                        ERROR(stream_error);
                        return nil;
                    }
                    
                    // now send the auth key
                    [pb_output_stream writeRawLittleEndian32: (int32_t)[auth_key_data length]];
                    [pb_output_stream writeRawData: auth_key_data];
                    [pb_output_stream flush];
                    
                    stream_error = [output_stream streamError];
                    if(stream_error) {
                        ERROR(stream_error);
                        return nil;
                    }
                    
                    while((byte = [pb_input_stream readRawByte])) {
                        [auth_response appendBytes: &byte length: 1];
                    }
                    
                    auth_response_string = [[NSString alloc] initWithData: auth_response encoding: NSUTF8StringEncoding];
                    
                    if(![auth_response_string isEqualToString: @"SUCCESS"]) {
                        RETHINK_ERROR(NSURLErrorCannotConnectToHost, auth_response_string);
                        return nil;
                    }
                } else {
                    RETHINK_ERROR(NSURLErrorCannotConnectToHost, @"Connection failed");
                    return nil;
                }
            } else {
                RETHINK_ERROR(NSURLErrorDNSLookupFailed, @"Could not find host");
                return nil;
            }
            
        } else {
            RETHINK_ERROR(NSURLErrorBadURL, @"Host name is required");
            return nil;
        }
        
        lock = [NSLock new];
    }
    
    return self;
}

- (id) initWithConnection:(RethinkDbClient*)parent {
    self = [super init];
    if(self) {
        connection = parent;
    }
    
    return self;
}

- (void)dealloc
{
    [input_stream close];
    [output_stream close];
}

#pragma mark -
#pragma mark Properties

- (void) setTerm:(Term *)term {
    _term = term;
}

- (Term*) term {
    return _term;
}

#pragma mark -
#pragma mark Utility stuff

- (id) decodeDatum:(Datum*)datum {
    id result;
    switch (datum.type) {
        case Datum_DatumTypeRNull:
            result = [NSNull null];
            break;
        case Datum_DatumTypeRBool:
            result = [NSNumber numberWithBool: datum.rBool];
            break;
        case Datum_DatumTypeRNum:
            result = [NSNumber numberWithDouble: datum.rNum];
            break;
        case Datum_DatumTypeRStr:
            result = datum.rStr;
            break;
        case Datum_DatumTypeRArray:
            result = [self decodeArray: datum.rArray];
            break;
        case Datum_DatumTypeRObject:
            result = [self decodeObject: datum.rObject];
            break;
        case Datum_DatumTypeRJson:
            result = datum.rStr;
            break;
        default:
            result = [NSError errorWithDomain: rethink_error code: -1 userInfo: [NSDictionary dictionaryWithObject: @"Unknown datum type" forKey: NSLocalizedDescriptionKey]];
            break;
    }

    return result;
}

- (id) decodeObject:(NSArray*)object {
    NSMutableDictionary* result = [NSMutableDictionary dictionaryWithCapacity: [object count]];

    [object enumerateObjectsUsingBlock:^(Datum_AssocPair* pair, NSUInteger idx, BOOL *stop) {
        [result setObject: [self decodeDatum: pair.val] forKey: pair.key];
    }];
    
    return result;
    
}

- (NSArray*) decodeArray:(NSArray*)array {
    NSMutableArray* result = [NSMutableArray arrayWithCapacity: [array count]];
    
    [array enumerateObjectsUsingBlock:^(Datum* datum, NSUInteger idx, BOOL *stop) {
        [result addObject: [self decodeDatum: datum]];
    }];
    
    return result;
}

- (id) decodeAtomResponse:(Response*) response {
    Datum* datum = [response.response objectAtIndex: 0];
    
    return [self decodeDatum: datum];
}

- (id) decodeErrorResponse:(Response*) response {
    // TODO: properly decode an error response into a dictionary
    return [response description];
}

- (id) decodeResponse:(Response*) response {
    switch (response.type) {
        case Response_ResponseTypeClientError:
        case Response_ResponseTypeRuntimeError:
        case Response_ResponseTypeCompileError:
            return [self decodeErrorResponse: response];

        case Response_ResponseTypeSuccessAtom:
            return [self decodeAtomResponse: response];

        case Response_ResponseTypeSuccessSequence:
            return [self decodeArray: response.response];
            
        case Response_ResponseTypeSuccessPartial:
            return [self decodeArray: response.response];
            
        case Response_ResponseTypeWaitComplete:
            return [NSError errorWithDomain: rethink_error code: -1 userInfo: [NSDictionary dictionaryWithObject: @"WAIT_COMPLETE responses not yet implemented" forKey: NSLocalizedDescriptionKey]];
    }
    
    return [NSError errorWithDomain: rethink_error code: -1 userInfo: [NSDictionary dictionaryWithObject: @"Invalid response type" forKey: NSLocalizedDescriptionKey]];
}

- (Datum*) datumFromNSObject:(id) object {
    Datum_Builder* result = [Datum_Builder new];
    
    if(object == nil || [object isKindOfClass: [NSNull class]]) {
        result.type = Datum_DatumTypeRNull;
    } else if([object isKindOfClass: [NSString class]]) {
        result.type = Datum_DatumTypeRStr;
        result.rStr = object;
    } else if([object isKindOfClass: [NSNumber class]]) {
        NSNumber* num = (NSNumber*)object;
        
        if (num == (void*)kCFBooleanFalse || num == (void*)kCFBooleanTrue) {
            result.type = Datum_DatumTypeRBool;
            result.rBool = [num boolValue];
        } else {
            result.type = Datum_DatumTypeRNum;
            result.rNum = [object doubleValue];
        }
    } else if([object isKindOfClass: [NSArray class]]) {
        NSArray* array = (NSArray*)object;
        result.type = Datum_DatumTypeRArray;
        for(id obj in array) {
            [result addRArray: [self datumFromNSObject: obj]];
        }
    } else if([object isKindOfClass: [NSDictionary class]]) {
        NSDictionary* dict = (NSDictionary*)object;
        result.type = Datum_DatumTypeRObject;
        
        [dict enumerateKeysAndObjectsUsingBlock:^(NSString* key, id obj, BOOL *stop) {
            Datum_AssocPair_Builder* pair = [Datum_AssocPair_Builder new];
            pair.key = key;
            pair.val = [self datumFromNSObject: obj];
            
            [result addRObject: [pair build]];
        }];
    }
    return [result build];
}

- (Term*) exprTerm:(id)object {
    if([object isKindOfClass: [Term class]]) {
        return object;
    }
    if([object isKindOfClass: [RethinkDbClient class]]) {
        RethinkDbClient* client = (RethinkDbClient*)object;
        return client.term;
    }
    if([object isKindOfClass: [NSPredicate class]]) {
        @throw [NSException exceptionWithName: rethink_error reason: @"NSPredicate support is not yet implemented" userInfo: nil];
    }
    
    Term_Builder* term = [Term_Builder new];
    term.type = Term_TermTypeDatum;
    term.datum = [self datumFromNSObject: object];
    
    return [term build];
}

- (Query_Builder*) queryBuilder {
    RethinkDbClient* client = connection;
    while(_query == nil && client) {
        _query = connection->_query;
        client = client->connection;
    }
    
    Query_Builder* result = [Query_Builder new];
    if(_query) {
        [result mergeFrom: _query];
    }
    
    return result;
}

- (Term*) termWithType:(Term_TermType)type args:(NSArray*) args andOptions:(NSDictionary*)options {
    Term_Builder* term = [Term_Builder new];
    term.type = type;
    
    if(args) {
        for(id arg in args) {
            [term addArgs: [self exprTerm: arg]];
        }
    }
    
    if(options) {
        [options enumerateKeysAndObjectsUsingBlock:^(NSString* key, id obj, BOOL *stop) {
            Term_AssocPair_Builder* pair = [Term_AssocPair_Builder new];
            pair.key = key;
            pair.val = [self exprTerm: obj];
            
            [term addOptargs: [pair build]];
        }];
    }
    
    return [term build];
}

- (Term*) termWithType:(Term_TermType)type andArgs:(NSArray*)args {
    return [self termWithType: type args: args andOptions: nil];
}

- (Term*) termWithType:(Term_TermType)type andOptions:(NSDictionary*) options {
    return [self termWithType: type args: nil andOptions: options];
}

- (Term*) termWithType:(Term_TermType)type {
    return [self termWithType: type args: nil andOptions: nil];
}

- (Term*) termWithType:(Term_TermType)type arg:(id)arg andOptions:(NSDictionary*) options {
    return [self termWithType: type args: [NSArray arrayWithObject: CHECK_NULL(arg)] andOptions: options];
}

- (Term*) termWithType:(Term_TermType)type andArg:(id)arg {
    return [self termWithType: type args: [NSArray arrayWithObject: CHECK_NULL(arg)] andOptions: nil];
}

- (RethinkDbClient*) clientWithTerm:(Term*) term {
    RethinkDbClient* client = [[RethinkDbClient alloc] initWithConnection: self];
    client.term = term;
    
    return client;
}

- (Response*) transmit:(Query_Builder*) query {
    NSData* response_data;
    
    // make sure only one thread can access the actual socket at any one time!
    [lock lock];
    @try {
        [query setToken: token++];
        Query* q = [query build];
        
        int32_t size = [q serializedSize];
        [pb_output_stream writeRawLittleEndian32: size];
        [q writeToCodedOutputStream: pb_output_stream];
        [pb_output_stream flush];
        
        int32_t response_size = [pb_input_stream readRawLittleEndian32];
        response_data = [pb_input_stream readRawData: response_size];
    } @finally {
        [lock unlock];
    }
    return [Response parseFromData: response_data];
}

#pragma mark -
#pragma mark common functions

- (NSInteger) nextVariable {
    if(connection) {
        return [connection nextVariable];
    }
    
    return variable_number++;
}

- (id) run:(Term*) toRun withQuery:(Query*)query error:(NSError**) error {
    if(query == nil) {
        query = _query;
    }
    
    if(connection) {
        return [connection run: toRun withQuery: query error: error];
    }
    
    if(input_stream && output_stream) {
        Query_Builder* toExecute = [Query_Builder new];
        if(query) {
            [toExecute mergeFrom: query];
        }
        toExecute.type = Query_QueryTypeStart;
        toExecute.query = toRun;

        
        @try {
            Response* response = [self transmit: toExecute];
            if(response.type == Response_ResponseTypeClientError || response.type == Response_ResponseTypeCompileError || response.type == Response_ResponseTypeRuntimeError) {
                if(error) {
                    // TODO: give more details when something goes wrong
                    Datum* errorDatum = [[response response] objectAtIndex: 0];
                    *error = [NSError errorWithDomain: rethink_error code: response.type userInfo: [NSDictionary dictionaryWithObjectsAndKeys:
                                                                                                    errorDatum.rStr, NSLocalizedDescriptionKey,
                                                                                                    [self decodeErrorResponse: response], @"RethinkDB Response",
                                                                                                    nil]];
                }
                return nil;
            }
            
            return [self decodeResponse: response];
            
        }
        @catch (NSException *exception) {
            RETHINK_ERROR(-3, [exception reason]);
            return nil;
        }
    }
    
    RETHINK_ERROR(-2, @"Connection is not open");
    return nil;
}

- (id) run:(NSError**)error {
    if(_term) {
        id result = [self run: _term withQuery: _query error: error];
        
        return result;
    }
    
    RETHINK_ERROR(-1, @"No query specified");
    return nil;
}

- (BOOL) close:(NSError**)error {
    [input_stream close];
    [output_stream close];
    pb_input_stream = nil;
    pb_output_stream = nil;
    input_stream = nil;
    output_stream = nil;
    
    return YES;
}

#pragma mark -
#pragma mark database functions

- (RethinkDbClient*) dbCreate:(NSString*)name {
    return [self clientWithTerm: [self termWithType: Term_TermTypeDbCreate andArg: name]];
}

- (RethinkDbClient*) dbDrop:(NSString*)name {
    return [self clientWithTerm: [self termWithType: Term_TermTypeDbDrop andArg: name]];
}

- (RethinkDbClient*) dbList {
    return [self clientWithTerm: [self termWithType: Term_TermTypeDbList]];
}

#pragma mark -
#pragma mark table functions

- (RethinkDbClient*) tableCreate:(NSString*)name options:(NSDictionary*)options {
    return [self clientWithTerm: [self termWithType: Term_TermTypeTableCreate arg: name andOptions: options]];
}

- (id <RethinkDBObject>) tableCreate:(NSString*)name {
    return [self tableCreate: name options: nil];
}

- (RethinkDbClient*) tableDrop:(NSString*)name {
    return [self clientWithTerm: [self termWithType: Term_TermTypeTableDrop andArg: name]];
}

- (RethinkDbClient*) tableList {
    return [self clientWithTerm: [self termWithType: Term_TermTypeTableList]];
}

- (RethinkDbClient*) indexCreate:(NSString*)name {
    return [self clientWithTerm: [self termWithType: Term_TermTypeIndexCreate andArg: name]];
}

- (RethinkDbClient*) indexDrop:(NSString*)name {
    return [self clientWithTerm: [self termWithType: Term_TermTypeIndexDrop andArg: name]];
}

- (RethinkDbClient*) indexList {
    return [self clientWithTerm: [self termWithType: Term_TermTypeIndexList]];
}

- (RethinkDbClient*) indexStatus:(id)names {
    if([names isKindOfClass: [NSString class]]) {
        return [self clientWithTerm: [self termWithType: Term_TermTypeIndexStatus andArg: names]];
    } else {
        return [self clientWithTerm: [self termWithType: Term_TermTypeIndexStatus andArgs: names]];
    }
}

- (RethinkDbClient*) indexWait:(id)names {
    if([names isKindOfClass: [NSString class]]) {
        return [self clientWithTerm: [self termWithType: Term_TermTypeIndexWait andArg: names]];
    } else {
        return [self clientWithTerm: [self termWithType: Term_TermTypeIndexWait andArgs: names]];
    }
}

#pragma mark -
#pragma mark Writing data

- (RethinkDbClient*) insert:(id)object options:(NSDictionary*)options {
    return [self clientWithTerm: [self termWithType: Term_TermTypeInsert
                                               args: [NSArray arrayWithObjects: self, CHECK_NULL(object), nil]
                                         andOptions: options]];
}

- (id <RethinkDBObject>) insert:(id)object {
    return [self insert: object options: nil];
}

- (RethinkDbClient*) update:(id)object options:(NSDictionary*)options {
    return [self clientWithTerm: [self termWithType: Term_TermTypeUpdate
                                               args: [NSArray arrayWithObjects: self, CHECK_NULL(object), nil]
                                         andOptions: options]];
}

- (id <RethinkDBObject>) update:(id)object {
    return [self update: object options: nil];
}

- (RethinkDbClient*) replace:(id)object options:(NSDictionary*)options {
    return [self clientWithTerm: [self termWithType: Term_TermTypeReplace
                                               args: [NSArray arrayWithObjects: self, CHECK_NULL(object), nil]
                                         andOptions: options]];
}

- (id <RethinkDBObject>) replace:(id)object {
    return [self replace: object options: nil];
}

- (RethinkDbClient*) delete:(NSDictionary*)options {
    return [self clientWithTerm: [self termWithType: Term_TermTypeDelete
                                         andOptions: options]];
}

- (id <RethinkDBObject>) delete {
    return [self delete: nil];
}

- (RethinkDbClient*) sync {
    return [self clientWithTerm: [self termWithType: Term_TermTypeSync]];
}

#pragma mark -
#pragma mark Selecting data

- (RethinkDbClient*) db: (NSString*)name {
    Query_Builder* query = [self queryBuilder];
    
    RethinkDbClient* db = [[RethinkDbClient alloc] initWithConnection: self];
    
    Term_Builder* term_builder = [Term_Builder new];
    term_builder.type = Term_TermTypeDb;
    [term_builder addArgs: [self exprTerm: name]];
    
    Query_AssocPair_Builder* args_builder = [Query_AssocPair_Builder new];
    args_builder.key = @"db";
    args_builder.val = [term_builder build];
    
    Query_AssocPair* args = [args_builder build];
    [query addGlobalOptargs: args];
    
    db->_query = [query build];
    
    return db;
}

- (RethinkDbClient*) table:(NSString*)name options:(NSDictionary*)options {
    return [self clientWithTerm: [self termWithType: Term_TermTypeTable arg: name andOptions: options]];
}

- (id <RethinkDBTable>) table:(NSString*)name {
    return [self table: name options: nil];
}

- (RethinkDbClient*) get:(id)key {
    return [self clientWithTerm: [self termWithType: Term_TermTypeGet andArg: key]];
}

- (RethinkDbClient*) getAll:(NSArray*)keys options:(NSDictionary*)options {
    return [self clientWithTerm: [self termWithType: Term_TermTypeGetAll args: keys andOptions: options]];
}

- (id <RethinkDBSequence>) getAll:(NSArray *)keys {
    return [self getAll: keys options: nil];
}

- (RethinkDbClient*) between:(id)lower and:(id)upper options:(NSDictionary*)options {
    NSArray* args = [NSArray arrayWithObjects: CHECK_NULL(lower), CHECK_NULL(upper), nil];
    
    return [self clientWithTerm: [self termWithType: Term_TermTypeBetween args: args andOptions: options]];
}

- (id <RethinkDBSequence>) between:(id)lower and:(id)upper {
    return [self between: lower and: upper options: nil];
}

- (RethinkDbClient*) filter:(id)predicate options:(NSDictionary *)options {
    if([predicate isKindOfClass: [NSDictionary class]]) {
        return [self clientWithTerm: [self termWithType: Term_TermTypeFilter args: [NSArray arrayWithObjects: self, CHECK_NULL(predicate), nil] andOptions: options]];
    }
    
    NSInteger variable = [self nextVariable];
    
    Term* arg_array = [self termWithType: Term_TermTypeMakeArray andArg: [NSNumber numberWithInteger: variable]];
    Term* func = [self termWithType: Term_TermTypeFunc andArgs: [NSArray arrayWithObjects: arg_array, CHECK_NULL(predicate), nil]];

    return [self clientWithTerm: [self termWithType: Term_TermTypeFilter args: [NSArray arrayWithObjects: self, func, nil] andOptions: options]];
}

- (id <RethinkDBSequence>) filter:(id)predicate {
    return [self filter: predicate options: nil];
}

#pragma mark -
#pragma mark Joins

- (RethinkDbClient*) join:(id)sequence on:(RethinkDbJoinPredicate)predicate inner:(BOOL)inner {
    NSNumber* left_num = [NSNumber numberWithInteger: [self nextVariable]];
    NSNumber* right_num = [NSNumber numberWithInteger: [self nextVariable]];
    
    RethinkDbClient* left = [self clientWithTerm: [self termWithType: Term_TermTypeVar andArg: left_num]];
    RethinkDbClient* right = [self clientWithTerm: [self termWithType: Term_TermTypeVar andArg: right_num]];
    
    id <RethinkDBRunnable> body = predicate(left, right);
    
    NSArray* args = [NSArray arrayWithObjects: left_num, right_num, nil];
    Term* func = [self termWithType: Term_TermTypeFunc andArgs: [NSArray arrayWithObjects: args, body, nil]];
    
    return [self clientWithTerm: [self termWithType: (inner ? Term_TermTypeInnerJoin : Term_TermTypeOuterJoin) andArgs: [NSArray arrayWithObjects: self, CHECK_NULL(sequence), func, nil]]];
}

- (RethinkDbClient*) innerJoin:(id)sequence on:(RethinkDbJoinPredicate)predicate {
    return [self join: sequence on: predicate inner: YES];
}

- (RethinkDbClient*) outerJoin:(id)sequence on:(RethinkDbJoinPredicate)predicate {
    return [self join: sequence on: predicate inner: NO];
}

- (RethinkDbClient*) eqJoin:(NSString*)key to:(id)sequence options:(NSDictionary*)options {
    return [self clientWithTerm: [self termWithType: Term_TermTypeEqJoin args: [NSArray arrayWithObjects: CHECK_NULL(key), CHECK_NULL(sequence), nil] andOptions: options]];
}

- (id <RethinkDBSequence>) eqJoin:(NSString*)key to:(id)sequence {
    return [self eqJoin: key to: sequence options: nil];
}

- (RethinkDbClient*) zip {
    return [self clientWithTerm: [self termWithType: Term_TermTypeZip andArg: self]];
}

#pragma mark -
#pragma mark Transformations

- (RethinkDbClient*) mapLike:(RethinkDbMappingFunction) function type:(Term_TermType) type {
    NSNumber* param_num = [NSNumber numberWithInteger: [self nextVariable]];
    
    RethinkDbClient* row = [self clientWithTerm: [self termWithType: Term_TermTypeVar andArg: param_num]];
    
    id <RethinkDBRunnable> body = function(row);
    
    NSArray* args = [NSArray arrayWithObject: param_num];
    Term* func = [self termWithType: Term_TermTypeFunc andArgs: [NSArray arrayWithObjects: args, body, nil]];
    
    return [self clientWithTerm: [self termWithType: type andArgs: [NSArray arrayWithObjects: self, func, nil]]];
}

- (RethinkDbClient*) map:(RethinkDbMappingFunction)function {
    return [self mapLike: function type: Term_TermTypeMap];
}

- (RethinkDbClient*) withFields:(NSArray*)fields {
    NSArray* args = [[NSArray arrayWithObject: self] arrayByAddingObjectsFromArray: fields];
    return [self clientWithTerm: [self termWithType: Term_TermTypeWithFields andArgs: args]];
}

- (RethinkDbClient*) concatMap:(RethinkDbMappingFunction)function {
    return [self mapLike: function type: Term_TermTypeConcatmap];
}

- (RethinkDbClient*) orderBy:(id)order {
    if([order isKindOfClass: [NSString class]]) {
        return [self clientWithTerm: [self termWithType: Term_TermTypeOrderby andArgs: [NSArray arrayWithObjects: self, order, nil]]];
    }
    
    NSArray* args = [[NSArray arrayWithObject: self] arrayByAddingObjectsFromArray: order];
    return [self clientWithTerm: [self termWithType: Term_TermTypeOrderby andArgs: args]];
}

- (RethinkDbClient*) skip:(NSInteger)count {
    return [self clientWithTerm: [self termWithType: Term_TermTypeSkip andArgs: [NSArray arrayWithObjects: self, [NSNumber numberWithInteger: count], nil]]];
}

- (RethinkDbClient*) limit:(NSInteger)count {
    return [self clientWithTerm: [self termWithType: Term_TermTypeLimit andArgs: [NSArray arrayWithObjects: self, [NSNumber numberWithInteger: count], nil]]];
}

- (RethinkDbClient*) slice:(NSInteger)start to:(NSInteger)end {
    return [self clientWithTerm: [self termWithType: Term_TermTypeSlice andArgs: [NSArray arrayWithObjects: self, [NSNumber numberWithInteger: start], [NSNumber numberWithInteger: end], nil]]];
}

- (RethinkDbClient*) nth:(NSInteger)index {
    return [self clientWithTerm: [self termWithType: Term_TermTypeNth andArgs: [NSArray arrayWithObjects: self, [NSNumber numberWithInteger: index], nil]]];
}

- (RethinkDbClient*) indexesOf:(id)datum {
    return [self clientWithTerm: [self termWithType: Term_TermTypeIndexesOf andArgs: [NSArray arrayWithObjects: self, CHECK_NULL(datum), nil]]];
}

- (RethinkDbClient*) indexesOfPredicate:(RethinkDbMappingFunction)function {
    return [self mapLike: function type: Term_TermTypeIndexesOf];
}

- (RethinkDbClient*) inEmpty {
    return [self clientWithTerm: [self termWithType: Term_TermTypeIsEmpty andArg: self]];
}

- (RethinkDbClient*) union:(RethinkDbClient*)sequence {
    return [self clientWithTerm: [self termWithType: Term_TermTypeUnion andArgs: [NSArray arrayWithObjects: self, sequence, nil]]];
}

- (RethinkDbClient*) sample:(NSInteger)count {
    return [self clientWithTerm: [self termWithType: Term_TermTypeSample andArgs: [NSArray arrayWithObjects: self, [NSNumber numberWithInteger: count], nil]]];
}

#pragma mark -
#pragma mark Agregation

- (RethinkDbClient*) reduce:(RethinkDbReductionFunction)function base:(id)base {
    NSNumber* acc_num = [NSNumber numberWithInteger: [self nextVariable]];
    NSNumber* val_num = [NSNumber numberWithInteger: [self nextVariable]];
    
    RethinkDbClient* acc = [self clientWithTerm: [self termWithType: Term_TermTypeVar andArg: acc_num]];
    RethinkDbClient* val = [self clientWithTerm: [self termWithType: Term_TermTypeVar andArg: acc_num]];
    
    id <RethinkDBRunnable> body = function(acc, val);
    
    NSArray* arg_nums = [NSArray arrayWithObjects: acc_num, val_num, nil];
    NSArray* args;
    if(base) {
        args = [NSArray arrayWithObjects: arg_nums, body, base, nil];
    } else {
        args = [NSArray arrayWithObjects: arg_nums, body, nil];
    }
    Term* func = [self termWithType: Term_TermTypeFunc andArgs: args];
    
    return [self clientWithTerm: [self termWithType: Term_TermTypeReduce andArgs: [NSArray arrayWithObjects: self, func, nil]]];
}

- (id <RethinkDBObject>) reduce:(RethinkDbReductionFunction)function {
    return [self reduce: function base: nil];
}

- (RethinkDbClient*) count {
    return [self clientWithTerm: [self termWithType: Term_TermTypeCount andArg: self]];
}

- (RethinkDbClient*) distinct {
    return [self clientWithTerm: [self termWithType: Term_TermTypeDistinct andArg: self]];
}

- (RethinkDbClient*) group:(RethinkDbGroupByFunction)groupFunction map:(RethinkDbMappingFunction)mapFunction andReduce:(RethinkDbReductionFunction)reduceFunction withBase:(id)base {
    NSNumber* group_num = [NSNumber numberWithInteger: [self nextVariable]];
    NSNumber* map_num = [NSNumber numberWithInteger: [self nextVariable]];
    NSNumber* acc_num = [NSNumber numberWithInteger: [self nextVariable]];
    NSNumber* val_num = [NSNumber numberWithInteger: [self nextVariable]];
    
    RethinkDbClient* group_var = [self clientWithTerm: [self termWithType: Term_TermTypeVar andArg: group_num]];
    id <RethinkDBRunnable> group_body = groupFunction(group_var);
    
    RethinkDbClient* map_row = [self clientWithTerm: [self termWithType: Term_TermTypeVar andArg: map_num]];
    id <RethinkDBRunnable> map_body = mapFunction(map_row);
    
    RethinkDbClient* acc = [self clientWithTerm: [self termWithType: Term_TermTypeVar andArg: acc_num]];
    RethinkDbClient* val = [self clientWithTerm: [self termWithType: Term_TermTypeVar andArg: acc_num]];
    
    id <RethinkDBRunnable> reduce_body = reduceFunction(acc, val);
    
    RethinkDbClient* group_func = [self clientWithTerm: [self termWithType: Term_TermTypeFunc andArgs: [NSArray arrayWithObjects: [NSArray arrayWithObject: group_num], group_body, nil]]];
    RethinkDbClient* map_func = [self clientWithTerm: [self termWithType: Term_TermTypeFunc andArgs: [NSArray arrayWithObjects: [NSArray arrayWithObject: map_num], map_body, nil]]];
    RethinkDbClient* reduce_func = [self clientWithTerm: [self termWithType: Term_TermTypeFunc andArgs: [NSArray arrayWithObjects: [NSArray arrayWithObjects: acc_num, val_num, nil], reduce_body, nil]]];
    
    NSArray* args;
    if(base) {
        args = [NSArray arrayWithObjects: self, group_func, map_func, reduce_func, base, nil];
    } else {
        args = [NSArray arrayWithObjects: self, group_func, map_func, reduce_func, nil];
    }
    
    return [self clientWithTerm: [self termWithType: Term_TermTypeGroupedMapReduce andArgs: args]];
}

- (id <RethinkDBObject>) group:(RethinkDbGroupByFunction)groupFunction map:(RethinkDbMappingFunction)mapFunction andReduce:(RethinkDbReductionFunction)reduceFunction {
    return [self group: groupFunction map: mapFunction andReduce: reduceFunction withBase: nil];
}

- (RethinkDbClient*) groupBy:(id)columns reduce:(NSDictionary*)reductionObject {
    if([columns isKindOfClass: [NSString class]]) {
        columns = [NSArray arrayWithObject: columns];
    }
    
    Term* reduction_literal = [self termWithType: Term_TermTypeMakeObj andArg: reductionObject];    
    return [self clientWithTerm: [self termWithType: Term_TermTypeGroupby andArgs: [NSArray arrayWithObjects: self, columns, reduction_literal, nil]]];
}

- (id <RethinkDBObject>) groupByAndCount:(id)columns {
    return [self groupBy: columns reduce: [NSDictionary dictionaryWithObject: [NSNumber numberWithBool: YES] forKey: @"COUNT"]];
}

- (id <RethinkDBObject>) groupBy:(id)columns sum:(NSString*)attribute {
    return [self groupBy: columns reduce: [NSDictionary dictionaryWithObject: attribute forKey: @"SUM"]];
}

- (id <RethinkDBObject>) groupBy:(id)columns average:(NSString*)attribute {
    return [self groupBy: columns reduce: [NSDictionary dictionaryWithObject: attribute forKey: @"AVG"]];
}

- (RethinkDbClient*) contains:(id)values {
    if(![values isKindOfClass: [NSArray class]]) {
        values = [NSArray arrayWithObject: CHECK_NULL(values)];
    }
    
    NSArray* args = [[NSArray arrayWithObject: self] arrayByAddingObjectsFromArray: values];
    return [self clientWithTerm: [self termWithType: Term_TermTypeContains andArgs: args]];
}

#pragma mark -
#pragma mark Document manipulation

- (RethinkDbClient*) row {
    return [self clientWithTerm: [self termWithType: Term_TermTypeImplicitVar]];
}

- (RethinkDbClient*) row:(NSString*)key {
    return [self clientWithTerm: [self termWithType: Term_TermTypeGetField andArgs: [NSArray arrayWithObjects:
                                                                                     [self termWithType: Term_TermTypeImplicitVar],
                                                                                     CHECK_NULL(key),
                                                                                     nil]]];
}

- (RethinkDbClient*) pluck:(id)fields {
    if([fields isKindOfClass: [NSString class]]) {
        fields = [NSArray arrayWithObject: fields];
    }
    
    NSArray* args = [[NSArray arrayWithObject: self] arrayByAddingObjectsFromArray: fields];
    return [self clientWithTerm: [self termWithType: Term_TermTypePluck andArgs: args]];
}

- (RethinkDbClient*) without:(id)fields {
    if([fields isKindOfClass: [NSString class]]) {
        fields = [NSArray arrayWithObject: fields];
    }
    
    NSArray* args = [[NSArray arrayWithObject: self] arrayByAddingObjectsFromArray: fields];
    return [self clientWithTerm: [self termWithType: Term_TermTypeWithout andArgs: args]];
}

- (RethinkDbClient*) merge:(id)object {
    return [self clientWithTerm: [self termWithType: Term_TermTypeMerge andArgs: [NSArray arrayWithObjects: self, CHECK_NULL(object), nil]]];
}

- (RethinkDbClient*) append:(id)object {
    return [self clientWithTerm: [self termWithType: Term_TermTypeAppend andArgs: [NSArray arrayWithObjects: self, CHECK_NULL(object), nil]]];
}

- (RethinkDbClient*) prepend:(id)object {
    return [self clientWithTerm: [self termWithType: Term_TermTypePrepend andArgs: [NSArray arrayWithObjects: self, CHECK_NULL(object), nil]]];
}

- (RethinkDbClient*) difference:(NSArray *)array {
    return [self clientWithTerm: [self termWithType: Term_TermTypeDifference andArgs: [NSArray arrayWithObjects: self, CHECK_NULL(array), nil]]];
}

- (RethinkDbClient*) setInsert:(id)value {
    return [self clientWithTerm: [self termWithType: Term_TermTypeSetInsert andArgs: [NSArray arrayWithObjects: self, CHECK_NULL(value), nil]]];
}

- (RethinkDbClient*) setUnion:(NSArray*)array {
    return [self clientWithTerm: [self termWithType: Term_TermTypeSetUnion andArgs: [NSArray arrayWithObjects: self, CHECK_NULL(array), nil]]];
}

- (RethinkDbClient*) setIntersection:(NSArray*)array {
    return [self clientWithTerm: [self termWithType: Term_TermTypeSetIntersection andArgs: [NSArray arrayWithObjects: self, CHECK_NULL(array), nil]]];
}

- (RethinkDbClient*) setDifference:(NSArray*)array {
    return [self clientWithTerm: [self termWithType: Term_TermTypeSetDifference andArgs: [NSArray arrayWithObjects: self, CHECK_NULL(array), nil]]];
}

- (RethinkDbClient*) field:(NSString*)key {
    return [self clientWithTerm: [self termWithType: Term_TermTypeGetField andArgs: [NSArray arrayWithObjects: self, CHECK_NULL(key), nil]]];
}

- (RethinkDbClient*) hasFields:(id)fields {
    if([fields isKindOfClass: [NSString class]]) {
        fields = [NSArray arrayWithObject: fields];
    }
    
    NSArray* args = [[NSArray arrayWithObject: self] arrayByAddingObjectsFromArray: fields];
    return [self clientWithTerm: [self termWithType: Term_TermTypeHasFields andArgs: args]];
}

- (RethinkDbClient*) insert:(id)object at:(NSUInteger)index {
    return [self clientWithTerm: [self termWithType: Term_TermTypeInsertAt andArgs: [NSArray arrayWithObjects: self, [NSNumber numberWithInteger: index], CHECK_NULL(object), nil]]];
}

- (RethinkDbClient*) splice:(NSArray*)objects at:(NSUInteger)index {
    return [self clientWithTerm: [self termWithType: Term_TermTypeSpliceAt andArgs: [NSArray arrayWithObjects: self, [NSNumber numberWithInteger: index], CHECK_NULL(objects), nil]]];
}

- (RethinkDbClient*) deleteAt:(NSUInteger)index to:(NSUInteger)end_index {
    return [self clientWithTerm: [self termWithType: Term_TermTypeDeleteAt andArgs: [NSArray arrayWithObjects: self, [NSNumber numberWithInteger: index], [NSNumber numberWithInteger: end_index], nil]]];
}

- (RethinkDbClient*) deleteAt:(NSUInteger)index {
    return [self clientWithTerm: [self termWithType: Term_TermTypeDeleteAt andArgs: [NSArray arrayWithObjects: self, [NSNumber numberWithInteger: index], nil]]];
}

- (RethinkDbClient*) changeAt:(NSUInteger)index value:(id)value {
    return [self clientWithTerm: [self termWithType: Term_TermTypeChangeAt andArgs: [NSArray arrayWithObjects: self, [NSNumber numberWithInteger: index], CHECK_NULL(value), nil]]];
}

- (RethinkDbClient*) keys {
    return [self clientWithTerm: [self termWithType: Term_TermTypeKeys andArg: self]];
}

#pragma mark -
#pragma mark String manipulations

- (RethinkDbClient*) match:(NSString*)regex {
    return [self clientWithTerm: [self termWithType: Term_TermTypeMatch andArgs: [NSArray arrayWithObjects: self, regex, nil]]];
}

#pragma mark -
#pragma mark Math and Logic

- (RethinkDbClient*) add:(id)expr {
    return [self clientWithTerm: [self termWithType: Term_TermTypeAdd andArgs: [NSArray arrayWithObjects: self, CHECK_NULL(expr), nil]]];
}

- (RethinkDbClient*) sub:(id)expr {
    return [self clientWithTerm: [self termWithType: Term_TermTypeSub andArgs: [NSArray arrayWithObjects: self, CHECK_NULL(expr), nil]]];
}

- (RethinkDbClient*) mul:(id)expr {
    return [self clientWithTerm: [self termWithType: Term_TermTypeMul andArgs: [NSArray arrayWithObjects: self, CHECK_NULL(expr), nil]]];
}

- (RethinkDbClient*) div:(id)expr {
    return [self clientWithTerm: [self termWithType: Term_TermTypeDiv andArgs: [NSArray arrayWithObjects: self, CHECK_NULL(expr), nil]]];
}

- (RethinkDbClient*) mod:(id)expr {
    return [self clientWithTerm: [self termWithType: Term_TermTypeMod andArgs: [NSArray arrayWithObjects: self, CHECK_NULL(expr), nil]]];
}

- (RethinkDbClient*) eq:(id)expr {
    return [self clientWithTerm: [self termWithType: Term_TermTypeEq andArgs: [NSArray arrayWithObjects: self, CHECK_NULL(expr), nil]]];
}

- (RethinkDbClient*) ne:(id)expr {
    return [self clientWithTerm: [self termWithType: Term_TermTypeNe andArgs: [NSArray arrayWithObjects: self, CHECK_NULL(expr), nil]]];
}

- (RethinkDbClient*) gt:(id)expr {
    return [self clientWithTerm: [self termWithType: Term_TermTypeGt andArgs: [NSArray arrayWithObjects: self, CHECK_NULL(expr), nil]]];
}

- (RethinkDbClient*) ge:(id)expr {
    return [self clientWithTerm: [self termWithType: Term_TermTypeGe andArgs: [NSArray arrayWithObjects: self, CHECK_NULL(expr), nil]]];
}

- (RethinkDbClient*) lt:(id)expr {
    return [self clientWithTerm: [self termWithType: Term_TermTypeLt andArgs: [NSArray arrayWithObjects: self, CHECK_NULL(expr), nil]]];
}

- (RethinkDbClient*) le:(id)expr {
    return [self clientWithTerm: [self termWithType: Term_TermTypeLe andArgs: [NSArray arrayWithObjects: self, CHECK_NULL(expr), nil]]];
}

- (RethinkDbClient*) not {
    return [self clientWithTerm: [self termWithType: Term_TermTypeNot andArg: self]];
}

- (RethinkDbClient*) and:(id)expr {
    return [self clientWithTerm: [self termWithType: Term_TermTypeAll andArgs: [NSArray arrayWithObjects: self, CHECK_NULL(expr), nil]]];
}

- (RethinkDbClient*) or:(id)expr {
    return [self clientWithTerm: [self termWithType: Term_TermTypeAny andArgs: [NSArray arrayWithObjects: self, CHECK_NULL(expr), nil]]];
}

- (RethinkDbClient*) any:(NSArray*)expressions {
    return [self clientWithTerm: [self termWithType: Term_TermTypeAny andArgs: expressions]];
}

- (RethinkDbClient*) all:(NSArray*)expressions {
    return [self clientWithTerm: [self termWithType: Term_TermTypeAll andArgs: expressions]];
}

#pragma mark -
#pragma mark Dates and Times

- (RethinkDbClient*) now {
    return [self clientWithTerm: [self termWithType: Term_TermTypeNow]];
}

- (RethinkDbClient*) timeWithYear:(NSInteger)year month:(NSInteger)month day:(NSInteger)day timezone:(NSString*)time_zone {
    return [self clientWithTerm: [self termWithType: Term_TermTypeTime andArgs: [NSArray arrayWithObjects:
                                                                                 [NSNumber numberWithInteger: year],
                                                                                 [NSNumber numberWithInteger: month],
                                                                                 [NSNumber numberWithInteger: day],
                                                                                 CHECK_NULL(time_zone),
                                                                                 nil]]];
}

- (RethinkDbClient*) timeWithYear:(NSInteger)year month:(NSInteger)month day:(NSInteger)day hour:(NSInteger)hour minute:(NSInteger)minute seconds:(NSInteger)seconds timezone:(NSString*)time_zone {
    return [self clientWithTerm: [self termWithType: Term_TermTypeTime andArgs: [NSArray arrayWithObjects:
                                                                                 [NSNumber numberWithInteger: year],
                                                                                 [NSNumber numberWithInteger: month],
                                                                                 [NSNumber numberWithInteger: day],
                                                                                 [NSNumber numberWithInteger: hour],
                                                                                 [NSNumber numberWithInteger: minute],
                                                                                 [NSNumber numberWithInteger: seconds],
                                                                                 CHECK_NULL(time_zone),
                                                                                 nil]]];
}

- (id <RethinkDBDateTime>) time:(NSDate*)date {
    NSCalendar* calendar = [NSCalendar currentCalendar];
    NSDateComponents* comps = [calendar components: NSCalendarUnitYear | NSCalendarUnitMonth | NSCalendarUnitDay | NSCalendarUnitHour | NSCalendarUnitMinute | NSCalendarUnitSecond | NSCalendarUnitTimeZone fromDate: date];
    
    return [self timeWithYear: comps.year month: comps.month day: comps.day hour: comps.hour minute: comps.minute seconds: comps.second timezone: [comps.timeZone name]];
}

- (RethinkDbClient*) epochTime:(id)seconds {
    return [self clientWithTerm: [self termWithType: Term_TermTypeEpochTime andArg: seconds]];
}

- (RethinkDbClient*) ISO8601:(id)time {
    return [self clientWithTerm: [self termWithType: Term_TermTypeIso8601 andArg: time]];
}

- (RethinkDbClient*) inTimezone:(id)time_zone {
    return [self clientWithTerm: [self termWithType: Term_TermTypeInTimezone andArg: time_zone]];
}

- (RethinkDbClient*) timezone {
    return [self clientWithTerm: [self termWithType: Term_TermTypeTimezone andArg: self]];
}

- (RethinkDbClient*) during:(id)from to:(id)to options:(NSDictionary*)options {
    if([from isKindOfClass: [NSDate class]]) {
        from = [self time: from];
    }
    if([to isKindOfClass: [NSDate class]]) {
        to = [self time: to];
    }
    
    return [self clientWithTerm: [self termWithType: Term_TermTypeDuring andArgs: [NSArray arrayWithObjects: self, CHECK_NULL(from), CHECK_NULL(to), nil]]];
}

- (id <RethinkDBObject>) during:(id)from to:(id)to {
    return [self during: from to: to options: nil];
}

- (RethinkDbClient*) date {
    return [self clientWithTerm: [self termWithType: Term_TermTypeDate]];
}

- (RethinkDbClient*) timeOfDay {
    return [self clientWithTerm: [self termWithType: Term_TermTypeTimeOfDay andArg: self]];
}

- (RethinkDbClient*) year {
    return [self clientWithTerm: [self termWithType: Term_TermTypeYear andArg: self]];
}

- (RethinkDbClient*) month {
    return [self clientWithTerm: [self termWithType: Term_TermTypeMonth andArg: self]];
}

- (RethinkDbClient*) day {
    return [self clientWithTerm: [self termWithType: Term_TermTypeDay andArg: self]];
}

- (RethinkDbClient*) dayOfWeek {
    return [self clientWithTerm: [self termWithType: Term_TermTypeDayOfWeek andArg: self]];
}

- (RethinkDbClient*) dayOfYear {
    return [self clientWithTerm: [self termWithType: Term_TermTypeDayOfYear andArg: self]];
}

- (RethinkDbClient*) hours {
    return [self clientWithTerm: [self termWithType: Term_TermTypeHours andArg: self]];
}

- (RethinkDbClient*) minutes {
    return [self clientWithTerm: [self termWithType: Term_TermTypeMinutes andArg: self]];
}

- (RethinkDbClient*) seconds {
    return [self clientWithTerm: [self termWithType: Term_TermTypeSeconds andArg: self]];
}

- (RethinkDbClient*) toISO8601 {
    return [self clientWithTerm: [self termWithType: Term_TermTypeToIso8601 andArg: self]];
}

- (RethinkDbClient*) toEpochTime {
    return [self clientWithTerm: [self termWithType: Term_TermTypeToEpochTime andArg: self]];
}

#pragma mark -
#pragma mark Control Structures

- (RethinkDbClient*) do:(RethinkDbExpressionFunction)expression withArguments:(NSArray*)arguments {
    NSMutableArray* arg_nums = [NSMutableArray arrayWithCapacity: [arguments count]];
    NSMutableArray* args = [NSMutableArray arrayWithCapacity: [arguments count]];
    [arguments enumerateObjectsUsingBlock:^(id obj, NSUInteger idx, BOOL *stop) {
        NSNumber* arg_num = [NSNumber numberWithInteger: [self nextVariable]];
        [arg_nums addObject: arg_num];
        [args addObject: [self clientWithTerm: [self termWithType: Term_TermTypeVar andArg: arg_num]]];
    }];
    
    id <RethinkDBRunnable> body = expression(args);
    Term* func = [self termWithType: Term_TermTypeFunc andArgs: [NSArray arrayWithObjects: arg_nums, body, nil]];
    
    return [self clientWithTerm: [self termWithType: Term_TermTypeFuncall andArgs: [[NSArray arrayWithObject: func] arrayByAddingObjectsFromArray: arguments]]];
}

- (RethinkDbClient*) branch:(RethinkDbClient*) test then:(RethinkDbClient*) then otherwise:(RethinkDbClient*) otherwise {
    return [self clientWithTerm: [self termWithType: Term_TermTypeBranch andArgs: [NSArray arrayWithObjects: test, then, otherwise, nil]]];
}

- (RethinkDbClient*) forEach:(RethinkDbMappingFunction)function {
    return [self mapLike: function type: Term_TermTypeForeach];
}

- (RethinkDbClient*) error:(id)message {
    if(message) {
        return [self clientWithTerm: [self termWithType: Term_TermTypeError andArg: message]];
    } else {
        return [self clientWithTerm: [self termWithType: Term_TermTypeError]];
    }
}

- (id <RethinkDBObject>) error {
    return [self error: nil];
}

- (RethinkDbClient*) default:(id)value {
    return [self clientWithTerm: [self termWithType: Term_TermTypeDefault andArgs: [NSArray arrayWithObjects: self, value, nil]]];
}

- (RethinkDbClient*) expr:(id)value {
    return [self clientWithTerm: [self exprTerm: value]];
}

- (RethinkDbClient*) js:(NSString*)script {
    return [self clientWithTerm: [self termWithType: Term_TermTypeJavascript andArg: script]];
}

- (RethinkDbClient*) coerceTo:(NSString*)type {
    return [self clientWithTerm: [self termWithType: Term_TermTypeCoerceTo andArgs: [NSArray arrayWithObjects: self, type, nil]]];
}

- (RethinkDbClient*) typeOf {
    return [self clientWithTerm: [self termWithType: Term_TermTypeTypeof andArg: self]];
}

- (RethinkDbClient*) info {
    return [self clientWithTerm: [self termWithType: Term_TermTypeInfo andArg: self]];

}
- (RethinkDbClient*) json:(NSString*)json {
    return [self clientWithTerm: [self termWithType: Term_TermTypeJson andArg: json]];
}

@end
