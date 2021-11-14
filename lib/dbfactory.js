const dbcache = require("./dbcache")
const dbtable = require("./dbtable")


module.exports = function dbfactory(poolOrConnection) {
  
  let cache = dbcache(poolOrConnection)
  let tables = { }


  function getTable(name, schema) {
    let key = schema ? `${schema}.${name}` : name
    let table = tables[key]
    if (table) return table 

    table = dbtable({table: name, schema, cache, pool: poolOrConnection})
    tables[key] = table 
    return table
  }

  
  function getCache() {
    return cache
  }




  return Object.freeze({
    getTable, getCache, 
  })

}
