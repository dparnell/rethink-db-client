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
- (id) initWithConnection:(RethinkDbClient *)parent andQueryBuilder:(Query_Builder*)builder;

@end

@implementation RethinkDbClient {
    int64_t token;
    NSLock* lock;
    
    __strong RethinkDbClient* connection;
    __strong NSInputStream* input_stream;
    __strong NSOutputStream* output_stream;
    __strong PBCodedOutputStream* pb_output_stream;
    __strong PBCodedInputStream* pb_input_stream;
    __strong Query_Builder* query_builder;
}

#pragma mark -
#pragma mark Initialization

+ (RethinkDbClient*) clientWithURL:(NSURL*)url andError:(NSError**)error {
    return [[RethinkDbClient alloc] initWithURL: url andError: error];
}

- (id) initWithURL:(NSURL*)url andError:(NSError**)error {
    self = [super init];
    
    if(self) {
        lock = [NSLock new];
        
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
    }
    
    return self;
}

- (id) initWithConnection:(RethinkDbClient*)parent {
    self = [super init];
    if(self) {
        connection = parent;
        
        if(parent->query_builder) {
            query_builder = [parent->query_builder clone];
        }
    }
    
    return self;
}

- (id) initWithConnection:(RethinkDbClient *)parent andQueryBuilder:(Query_Builder*)builder {
    self = [super init];
    if(self) {
        connection = parent;
        query_builder = builder;
    }
    
    return self;
}

- (void)dealloc
{
    [input_stream close];
    [output_stream close];
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

#pragma mark -
#pragma mark common functions


- (RethinkDbClient*) db: (NSString*)name {
    RethinkDbClient* db = [[RethinkDbClient alloc] initWithConnection: self];
    db.defaultDatabase = name;
    
    return db;
}

- (Response*) transmit:(Query_Builder*) builder {
    NSData* response_data;
    
    if(connection) {
        return [connection transmit: builder];
    }
    
    // make sure only one thread can access the actual socket at any one time!
    [lock lock];
    @try {
        Query* query = [[builder setToken: token++] build];
        int32_t size = [query serializedSize];
        [pb_output_stream writeRawLittleEndian32: size];
        [query writeToCodedOutputStream: pb_output_stream];
        [pb_output_stream flush];
        
        int32_t response_size = [pb_input_stream readRawLittleEndian32];
        response_data = [pb_input_stream readRawData: response_size];
    } @finally {
        [lock unlock];
    }
    return [Response parseFromData: response_data];
}

- (id) run:(Query_Builder*) builder error:(NSError**) error {
    if(_defaultDatabase) {
        Datum_Builder* db = [Datum_Builder new];
        db.type = Datum_DatumTypeRStr;
        db.rStr = _defaultDatabase;
        
        Term_Builder* db_term = [Term_Builder new];
        db_term.type = Term_TermTypeDatum;
        db_term.datum = [db build];
        
        Term_Builder* term_builder = [Term_Builder new];
        term_builder.type = Term_TermTypeDb;
        [term_builder addArgs: [db_term build]];
        
        Term* term = [term_builder build];
        
        Query_AssocPair_Builder* args_builder = [Query_AssocPair_Builder new];
        args_builder.key = @"db";
        args_builder.val = term;
        
        Query_AssocPair* args = [args_builder build];
        [builder addGlobalOptargs: args];
    }
    
    Response* response = [self transmit: builder];
    
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
    if(query_builder) {
        id result = [self run: query_builder error: error];
        [query_builder clear];
        
        return result;
    }
    
    RETHINK_ERROR(-1, @"No query specified");
    return nil;
}

#pragma mark -
#pragma mark database functions

- (RethinkDbClient*) dbCreate:(NSString*)name {
    Term_Builder* tb = [Term_Builder new];
    tb.type = Term_TermTypeDbCreate;
    
    Datum_Builder* datum = [Datum_Builder new];
    datum.type = Datum_DatumTypeRStr;
    datum.rStr = name;
    
    Term_Builder* arg = [Term_Builder new];
    arg.type = Term_TermTypeDatum;
    arg.datum = [datum build];
    
    [tb addArgs: [arg build]];
    
    query_builder = [Query_Builder new];
    query_builder.type = Query_QueryTypeStart;
    query_builder.query = tb.build;
    
    return self;
}

- (RethinkDbClient*) dbDrop:(NSString*)name {
    Term_Builder* tb = [Term_Builder new];
    tb.type = Term_TermTypeDbDrop;
    
    Datum_Builder* datum = [Datum_Builder new];
    datum.type = Datum_DatumTypeRStr;
    datum.rStr = name;
    
    Term_Builder* arg = [Term_Builder new];
    arg.type = Term_TermTypeDatum;
    arg.datum = [datum build];
    
    [tb addArgs: [arg build]];
    
    query_builder = [Query_Builder new];
    query_builder.type = Query_QueryTypeStart;
    query_builder.query = tb.build;
    
    return self;
}

- (RethinkDbClient*) dbList {
    Term_Builder* tb = [Term_Builder new];
    tb.type = Term_TermTypeDbList;
    
    query_builder = [Query_Builder new];
    query_builder.type = Query_QueryTypeStart;
    query_builder.query = tb.build;
    
    return self;
}

#pragma mark -
#pragma mark table functions

- (RethinkDbClient*) createTable:(NSString*)name options:(NSDictionary*)options {
    Term_Builder* tb = [Term_Builder new];
    tb.type = Term_TermTypeTableCreate;
    
    Datum_Builder* datum = [Datum_Builder new];
    datum.type = Datum_DatumTypeRStr;
    datum.rStr = name;

    Term_Builder* arg = [Term_Builder new];
    arg.type = Term_TermTypeDatum;
    arg.datum = datum.build;
    
    [tb addArgs: [arg build]];
    
    query_builder = [Query_Builder new];
    query_builder.type = Query_QueryTypeStart;
    query_builder.query = [tb build];
    
    return self;
}

- (RethinkDbClient*) createTable:(NSString*)name {
    return [self createTable: name options: nil];
}


@end
