What is this thing?
===================

This is a simple client for RethinkDB written in Objective-C.

How do I use it?
================

    $> git clone https://github.com/dparnell/rethink-db-client.git
    $> git submodule init
    $> git submodule update

Then open the project in XCode and build.
You can now use the code in your own application via the generated framework.

What can I do with it?
======================

The following snippet shows how to use the client to get the list of tables.

    NSURL* url = [NSURL URLWithString: @"rethink://localhost"];
    NSError* error = nil;
    RethinkDbClient* r = [RethinkDbClient clientWithURL: url andError: &error];
    if(r) {
      NSArray* tables = [[r tableList] run: &error];
      if(tables) {
        NSLog(@"tables = %@", tables);
      } else {
        NSLog(@"tableList failed: %@, error);
      }
    } else {
      NSLog(@"Connection failed: %@", error);
    }


