import RethinkDbClient

var c = RethinkDbClient(URL: NSURL(string: "rethinkdb://localhost/"), andError: nil)
var databases:[String] = c.dbList().run(nil) as! [String]
c.db(databases[0]).tableList().run(nil)
