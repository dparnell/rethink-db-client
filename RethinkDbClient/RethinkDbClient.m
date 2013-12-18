//
//  RethinkDbClient.m
//  RethinkDbClient
//
//  Created by Daniel Parnell on 14/12/2013.
//  Copyright (c) 2013 Daniel Parnell. All rights reserved.
//

#import "RethinkDbClient.h"
#import <ProtocolBuffers/ProtocolBuffers.h>
#import "Ql2.pb.h"

static NSString* rethink_error = @"RethinkDB Error";

#define ERROR(x) if(error) *error = x
#define RETHINK_ERROR(x,y) if(error) *error = [NSError errorWithDomain: rethink_error code: x userInfo: [NSDictionary dictionaryWithObject: y forKey: NSLocalizedDescriptionKey]]

@interface RethinkDbClient (Private)

- (id) initWithConnection:(RethinkDbClient*)parent;

@property (retain) Term* term;

@end

@implementation RethinkDbClient {
    int64_t token;
    NSLock* lock;
    
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

                    // now send the auth key
                    [pb_output_stream writeRawLittleEndian32: (int32_t)[auth_key_data length]];
                    [pb_output_stream writeRawData: auth_key_data];
                    [pb_output_stream flush];
                    
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

- (Term*) expr:(id)object {
    if([object isKindOfClass: [Term class]]) {
        return object;
    }
    
    Term_Builder* term = [Term_Builder new];
    term.type = Term_TermTypeDatum;
    term.datum = [self datumFromNSObject: object];
    
    return [term build];
}

#pragma mark -
#pragma mark common functions

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
            [term addArgs: [self expr: arg]];
        }
    }
    
    if(options) {
        [options enumerateKeysAndObjectsUsingBlock:^(NSString* key, id obj, BOOL *stop) {
            Term_AssocPair_Builder* pair = [Term_AssocPair_Builder new];
            pair.key = key;
            pair.val = [self expr: obj];
            
            [term addOptargs: [pair build]];
        }];
    }
    
    return [term build];
}

- (Term*) termWithType:(Term_TermType)type andArgs:(NSArray*)args {
    return [self termWithType: type args: args andOptions: nil];
}

- (Term*) termWithType:(Term_TermType)type {
    return [self termWithType: type args: nil andOptions: nil];
}

- (Term*) termWithType:(Term_TermType)type arg:(id)arg andOptions:(NSDictionary*) options {
    return [self termWithType: type args: [NSArray arrayWithObject: arg] andOptions: options];
}

- (Term*) termWithType:(Term_TermType)type andArg:(id)arg {
    return [self termWithType: type args: [NSArray arrayWithObject: arg] andOptions: nil];
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

- (id) run:(Term*) toRun withQuery:(Query*)query error:(NSError**) error {
    if(query == nil) {
        query = _query;
    }
    
    if(connection) {
        return [connection run: toRun withQuery: query error: error];
    }
    
    Query_Builder* toExecute = [Query_Builder new];
    if(query) {
        [toExecute mergeFrom: query];
    }
    toExecute.type = Query_QueryTypeStart;
    toExecute.query = toRun;
    
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

- (id) run:(NSError**)error {
    if(_term) {
        id result = [self run: _term withQuery: _query error: error];
        
        return result;
    }
    
    RETHINK_ERROR(-1, @"No query specified");
    return nil;
}

#pragma mark -
#pragma mark database functions

- (RethinkDbClient*) db: (NSString*)name {
    Query_Builder* query = [self queryBuilder];
    
    RethinkDbClient* db = [[RethinkDbClient alloc] initWithConnection: self];
    db.defaultDatabase = name;
    
    Term_Builder* term_builder = [Term_Builder new];
    term_builder.type = Term_TermTypeDb;
    [term_builder addArgs: [self expr: name]];
    
    Query_AssocPair_Builder* args_builder = [Query_AssocPair_Builder new];
    args_builder.key = @"db";
    args_builder.val = [term_builder build];
    
    Query_AssocPair* args = [args_builder build];
    [query addGlobalOptargs: args];
    
    db->_query = [query build];
    
    return db;
}

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

- (RethinkDbClient*) tableCreate:(NSString*)name {
    return [self tableCreate: name options: nil];
}

- (RethinkDbClient*) tableDrop:(NSString*)name {
    return [self clientWithTerm: [self termWithType: Term_TermTypeTableDrop andArg: name]];
}

- (RethinkDbClient*) tableList {
    return [self clientWithTerm: [self termWithType: Term_TermTypeTableList]];
}

- (RethinkDbClient*) table:(NSString*)name options:(NSDictionary*)options {
    return [self clientWithTerm: [self termWithType: Term_TermTypeTable arg: name andOptions: options]];
}

- (RethinkDbClient*) table:(NSString*)name {
    return [self table: name options: nil];
}

- (RethinkDbClient*) insert:(id)object options:(NSDictionary*)options {
    if(self.term.type != Term_TermTypeTable) {
        @throw [NSException exceptionWithName: rethink_error reason: @"insert requires a table object" userInfo: nil];
    }

    
    return [self clientWithTerm: [self termWithType: Term_TermTypeInsert
                                               args: [NSArray arrayWithObjects: self.term, object, nil]
                                         andOptions: options]];
}

- (RethinkDbClient*) insert:(id)object {
    return [self insert: object options: nil];
}


@end
