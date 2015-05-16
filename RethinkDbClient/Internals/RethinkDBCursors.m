//
//  RethinkDBCursors.m
//  RethinkDbClient
//
//  Created by Daniel Parnell on on 16/05/2015.
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
#import <Foundation/Foundation.h>
#import "RethinkDbClient.h"
#import "RethinkDBClient-Private.h"
#import "RethinkDBCursors-Private.h"

@implementation RethinkDBCursor {
    __weak RethinkDbClient *client;
    __strong Response *_response;
    __strong NSArray *_rows;
    RethinkDbCursorValueBlock on_row;
    RethinkDbErrorBlock on_error;
    
    NSUInteger index;
}

- (instancetype)initWithClient:(RethinkDbClient*)aClient andToken:(int64_t)aToken
{
    self = [super init];
    if (self) {
        client = aClient;
        _token = aToken;
        index = 0;
    }
    return self;
}

#pragma mark -
#pragma mark RethinkDBCursor - Private

- (NSArray*) rows {
    return _rows;
}

- (void) setRows:(NSArray *)rows {
    _rows = rows;
}

- (Response*) response {
    return _response;
}

- (void) setResponse:(Response *)response {
    _response = response;
}

- (void) setOnError:(RethinkDbErrorBlock)anErrorBlock {
    on_error = anErrorBlock;
}

- (void) finished {
    // do nothing
}

- (BOOL) fetchNextBatch {
    if(self.response.type == Response_ResponseTypeSuccessPartial) {
        Query_Builder *qb = [Query_Builder new];
        qb.token = self.token;
        qb.type = Query_QueryTypeContinue;
        [client transmitAsync: qb];
        
        return YES;
    }
    
    return NO;
}

- (BOOL) processBatch:(NSArray*) to_process {
    BOOL continue_cursor = NO;
    
    if(on_row) {
        for (id value in to_process) {
            continue_cursor = on_row(value);
            
            if(!continue_cursor) {
                break;
            }
        }
    }
    
    return continue_cursor;
}

- (void) handleBatch {
    Response *resp = self.response;

    if(resp) {
        BOOL continue_cursor = [self processBatch: self.rows];
        
        if(continue_cursor) {
            if(![self fetchNextBatch]) {
                [self finished];
                [client removeCursor: self];
            }
        }
    } else {
        @throw [NSException exceptionWithName: @"rethinkdb" reason: @"Unexpected batch" userInfo: nil];
    }
}

#pragma mark -
#pragma mark RethinkDBCursor - Public interface

- (void) each:(RethinkDbCursorValueBlock)row fail:(RethinkDbErrorBlock) error {
    on_row = row;
    on_error = error;
    
    dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0), ^{
        [self handleBatch];
    });
}

- (void) close {
    Query_Builder *qb = [Query_Builder new];
    qb.token = self.token;
    qb.type = Query_QueryTypeStop;
    [client transmitAsync: qb];
    [client removeCursor: self];
}

@end

#pragma mark -
#pragma mark RethinkDBSequenceCursor

typedef BOOL (^BatchBlock)(NSArray *rows);


@implementation RethinkDBSequenceCursor {
    RethinkDbDoneBlock on_done;
    BatchBlock on_batch;
}

- (BOOL) processBatch:(NSArray*) to_process {
    if(on_batch) {
        return on_batch(to_process);
    }
    
    return [super processBatch: to_process];
}

- (void) finished {
    if(on_done) {
        on_done();
    }
}

- (void) each:(RethinkDbCursorValueBlock)row done:(RethinkDbDoneBlock) done fail:(RethinkDbErrorBlock) error {
    on_done = done;
    [self each: row fail: error];
}

- (void) next:(RethinkDbCursorValueBlock)success fail:(RethinkDbErrorBlock) error {
    if(error) {
        error([NSError errorWithDomain: @"rethinkdb" code: 0 userInfo: [NSDictionary dictionaryWithObject: @"Not yet implemented" forKey: NSLocalizedDescriptionKey]]);
    }
}

- (void) toArrayThen:(RethinkDbArrayBlock)success fail:(RethinkDbErrorBlock) error {
    __block NSMutableArray *result = [NSMutableArray new];
    
    on_batch = ^BOOL(NSArray* batch) {
        [result addObjectsFromArray: batch];
        return true;
    };
    
    if(success) {
        on_done = ^() {
            success([result copy]);
        };
    }
    
    [self setOnError: error];
    [self handleBatch];
    [self fetchNextBatch];
}

- (NSArray*) toArray:(NSError**)error {
    __block NSArray *result = nil;
    __block BOOL done = NO;
    
    [self toArrayThen:^(NSArray *array) {
        result = array;
        done = YES;
    } fail:^(NSError *an_error) {
        if(error) {
            *error = an_error;
        }
        
        done = YES;
    }];
    
    while(!done) {
        [[NSRunLoop currentRunLoop] runUntilDate: [NSDate date]];
    }
    
    return result;
}

@end

#pragma mark -
#pragma mark RethinkDBChangeFeed

@implementation RethinkDBChangeFeed



@end

