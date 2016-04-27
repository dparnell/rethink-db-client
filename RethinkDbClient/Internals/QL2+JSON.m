//
//  QL2+JSON.m
//  RethinkDbClient
//
//  Created by Daniel Parnell on 20/04/2016.
//  Copyright © 2016 Daniel Parnell. All rights reserved.
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

#import "QL2+JSON.h"

@implementation Datum (JSON)

- (void) toJSON:(NSMutableData *)data {
    char buf[32];
    
    switch ([self type]) {
        case Datum_DatumTypeRNull:
            [data appendBytes: "null" length: 4];
            break;
            
        case Datum_DatumTypeRBool:
            if([self rBool]) {
                [data appendBytes: "true" length: 4];
            } else {
                [data appendBytes: "false" length: 5];
            }
            break;
            
        case Datum_DatumTypeRNum:
            snprintf(buf, sizeof(buf), "%lf", [self rNum]);
            [data appendBytes: buf length: strlen(buf)];
            break;
            
/*          
            Datum_DatumTypeRStr = 4,
            Datum_DatumTypeRArray = 5,
            Datum_DatumTypeRObject = 6,
            Datum_DatumTypeRJson = 7,
 */

        default:
            @throw @"Unhandled datum type";
            break;
    }
}

@end

@implementation Term (JSON)

- (void) encodeArguments:(NSMutableData*) data {
    BOOL first = YES;
    
    [data appendBytes: ",[" length: 2];
    for (Term *arg in [self args]) {
        if(first) {
            first = NO;
        } else {
            [data appendBytes: "," length: 1];
        }
        [arg toJSON: data];
    }
    [data appendBytes: "]" length: 1];
}

- (void) encodeOptions:(NSMutableData*) data {
    
}

- (void) toJSON:(NSMutableData *)data {
    char buf[32];
    
    if(type == Term_TermTypeDatum) {
        [[self datum] toJSON: data];
    } else {
        [data appendBytes: "[" length: 1];
        snprintf(buf, sizeof(buf), "%d", type);
        [data appendBytes: buf length: strlen(buf)];
        [self encodeArguments: data];
        [self encodeOptions: data];
        [data appendBytes: "]" length: 1];
    }
}

@end

@implementation Query (JSON)

- (void) encodeOptionsAsJson:(NSMutableData*) data {
    BOOL first = YES;
    [data appendBytes: "{" length: 1];
    for (Query_AssocPair *pair in [self globalOptargs]) {
        if(first) {
            first = NO;
        } else {
            [data appendBytes: "," length: 1];
        }
        
        [data appendBytes: "\"" length: 1];
        [data appendData: [[pair key] dataUsingEncoding: NSUTF8StringEncoding]];
        [data appendBytes: "\":" length: 2];
        
    }
    [data appendBytes: "}" length: 1];
}

- (NSData*) toJSON {
    char buf[32];
    NSMutableData *data = [NSMutableData new];

    [data appendBytes: "[" length: 1];
    snprintf(buf, sizeof(buf), "%d,", query.type);
    [data appendBytes: buf length: strlen(buf)];
    [[self query] toJSON: data];
    [data appendBytes: "," length: 1];
    [self encodeOptionsAsJson: data];
    [data appendBytes: "]" length: 1];
    
    return data;
}

@end

