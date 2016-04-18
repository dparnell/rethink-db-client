import RethinkDbClient

var c = try RethinkDbClient(URL: NSURL(string: "rethinkdb://localhost/?version=3"))
var databases:[String] = try c.dbList().run() as! [String]
try c.db(databases[0]).tableList().run()
