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

#pragma mark -
#pragma mark common functions

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

- (RethinkDbClient*) db: (NSString*)name {
    Query_Builder* query = [self queryBuilder];
    
    RethinkDbClient* db = [[RethinkDbClient alloc] initWithConnection: self];
    db.defaultDatabase = name;
    
    Datum_Builder* db_datum = [Datum_Builder new];
    db_datum.type = Datum_DatumTypeRStr;
    db_datum.rStr = name;
    
    Term_Builder* db_term = [Term_Builder new];
    db_term.type = Term_TermTypeDatum;
    db_term.datum = [db_datum build];
    
    Term_Builder* term_builder = [Term_Builder new];
    term_builder.type = Term_TermTypeDb;
    [term_builder addArgs: [db_term build]];
    
    Query_AssocPair_Builder* args_builder = [Query_AssocPair_Builder new];
    args_builder.key = @"db";
    args_builder.val = [term_builder build];
    
    Query_AssocPair* args = [args_builder build];
    [query addGlobalOptargs: args];
    
    db->_query = [query build];
    
    return db;
}

- (RethinkDbClient*) dbCreate:(NSString*)name {
    RethinkDbClient* dbCreate = [[RethinkDbClient alloc] initWithConnection: self];
    Term_Builder* dbCreateTerm = [Term_Builder new];
    dbCreateTerm.type = Term_TermTypeDbCreate;
    
    Datum_Builder* datum = [Datum_Builder new];
    datum.type = Datum_DatumTypeRStr;
    datum.rStr = name;
    
    Term_Builder* arg = [Term_Builder new];
    arg.type = Term_TermTypeDatum;
    arg.datum = [datum build];
    
    [dbCreateTerm addArgs: [arg build]];

    dbCreate.term = [dbCreateTerm build];
    
    return dbCreate;
}

- (RethinkDbClient*) dbDrop:(NSString*)name {
    RethinkDbClient* dbDrop = [[RethinkDbClient alloc] initWithConnection: self];
    Term_Builder* dbDropTerm = [Term_Builder new];
    dbDropTerm.type = Term_TermTypeDbDrop;
    
    Datum_Builder* datum = [Datum_Builder new];
    datum.type = Datum_DatumTypeRStr;
    datum.rStr = name;
    
    Term_Builder* arg = [Term_Builder new];
    arg.type = Term_TermTypeDatum;
    arg.datum = [datum build];
    
    [dbDropTerm addArgs: [arg build]];
    
    dbDrop.term = [dbDropTerm build];
    
    return dbDrop;
}

- (RethinkDbClient*) dbList {
    RethinkDbClient* dbList = [[RethinkDbClient alloc] initWithConnection: self];
    Term_Builder* dbListTerm = [Term_Builder new];
    dbListTerm.type = Term_TermTypeDbList;
    
    dbList.term = [dbListTerm build];
    
    return dbList;
}

#pragma mark -
#pragma mark table functions

- (RethinkDbClient*) tableCreate:(NSString*)name options:(NSDictionary*)options {
    RethinkDbClient* tableCreate = [[RethinkDbClient alloc] initWithConnection: self];
    
    Term_Builder* tableCreateTerm = [Term_Builder new];
    tableCreateTerm.type = Term_TermTypeTableCreate;
    
    Datum_Builder* datum = [Datum_Builder new];
    datum.type = Datum_DatumTypeRStr;
    datum.rStr = name;

    Term_Builder* arg = [Term_Builder new];
    arg.type = Term_TermTypeDatum;
    arg.datum = datum.build;
    
    [tableCreateTerm addArgs: [arg build]];

    tableCreate.term = [tableCreateTerm build];
    
    return tableCreate;
}

- (RethinkDbClient*) tableCreate:(NSString*)name {
    return [self tableCreate: name options: nil];
}


@end
