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

static void json_encode_string(NSString *string, NSMutableData *data) {
    char *buffer;
    NSUInteger length = [string length];
    unichar *ch = malloc(sizeof(unichar) * length);
    NSUInteger out_size = 0;
    NSUInteger out_pos = 0;
    
    [string getCharacters: ch];
    
    for(NSUInteger i = 0; i < length; i++) {
        unichar c = ch[i];
        
        if(c == '"' || c == '\\' || c == '\b' || c == '\f' || c == '\n' || c == '\r' || c == '\t') {
            out_size += 2;
        } else if(c < 32 || c > 127) {
            out_size += 6;
        } else {
            out_size++;
        }
    }
    
    buffer = malloc(out_size);
    out_pos = 0;
    for(NSUInteger i = 0; i < length; i++) {
        unichar c = ch[i];

        switch (c) {
            case '"':
                buffer[out_pos++] = '\\';
                buffer[out_pos++] = '"';
                break;
            case '\\':
                buffer[out_pos++] = '\\';
                buffer[out_pos++] = '\\';
                break;
            case '\b':
                buffer[out_pos++] = '\\';
                buffer[out_pos++] = 'b';
                break;
            case '\f':
                buffer[out_pos++] = '\\';
                buffer[out_pos++] = 'f';
                break;
            case '\n':
                buffer[out_pos++] = '\\';
                buffer[out_pos++] = 'n';
                break;
            case '\r':
                buffer[out_pos++] = '\\';
                buffer[out_pos++] = 'r';
                break;
            case '\t':
                buffer[out_pos++] = '\\';
                buffer[out_pos++] = 't';
                break;
                
            default:
                if(c < 32 || c > 127) {
                    sprintf(&buffer[out_pos], "\\u%04x", c);
                    out_pos += 6;
                } else {
                    buffer[out_pos++] = c;
                }
                break;
        }
    }
    free(ch);
    
    [data appendBytes: "\"" length: 1];
    [data appendBytes: buffer length: out_size];
    [data appendBytes: "\"" length: 1];
    
    free(buffer);
}

@implementation Datum (JSON)

+ (Datum*) datumFromNSObject:(id) object {
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

- (void) toJSON:(NSMutableData *)data {
    NSData *tmp;
    NSNumber *num;
    BOOL first = YES;
    
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
            num = [NSNumber numberWithDouble: [self rNum]];
            tmp = [[num stringValue] dataUsingEncoding: NSUTF8StringEncoding];
            [data appendData: tmp];
            break;

        case Datum_DatumTypeRStr:
            json_encode_string([self rStr], data);
            break;
            
        case Datum_DatumTypeRArray:
            // Term_TermTypeMakeArray
            [data appendBytes: "[2,[" length: 4];
            for (Term *arg in [self rArray]) {
                if(first) {
                    first = NO;
                } else {
                    [data appendBytes: "," length: 1];
                }
                
                [arg toJSON: data];
            }
            [data appendBytes: "]]" length: 2];
            break;
            
        case Datum_DatumTypeRObject:
            [data appendBytes: "{" length: 1];
            
            for (Datum_AssocPair *pair in [self rObject]) {
                if(first) {
                    first = NO;
                } else {
                    [data appendBytes: "," length: 1];
                }
                
                json_encode_string([pair key], data);
                [data appendBytes: ":" length: 1];
                [[pair val] toJSON: data];
            }
            
            [data appendBytes: "}" length: 1];
            break;
/*
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
        
        json_encode_string([pair key], data);
        [data appendBytes: ":" length: 1];
        [[pair val] toJSON: data];
    }
    [data appendBytes: "}" length: 1];
}

- (NSData*) toJSON {
    char buf[32];
    NSMutableData *data = [NSMutableData new];

    [data appendBytes: "[" length: 1];
    snprintf(buf, sizeof(buf), "%d,", type);
    [data appendBytes: buf length: strlen(buf)];
    [[self query] toJSON: data];
    [data appendBytes: "," length: 1];
    [self encodeOptionsAsJson: data];
    [data appendBytes: "]" length: 1];

    return data;
}

@end


@implementation Response (JSON)

+ (Response*) fromJSON:(NSData*)data withToken:(int64_t)token {
    Response_Builder *b = [Response_Builder new];
    NSDictionary *json = [NSJSONSerialization JSONObjectWithData: data options: 0 error: nil];
    
    Response_ResponseType type = [[json objectForKey: @"t"] intValue];
    
    [b setToken: token];
    [b setType: type];
    
    for (id obj in [json objectForKey: @"r"]) {
        [b addResponse: [Datum datumFromNSObject: obj]];
    }

    return [b build];
}

@end
