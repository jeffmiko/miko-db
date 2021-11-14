const dbpromise = require("./dbpromise")

module.exports = function dbcache(poolOrConnection) {

  const fieldCache = { }
  let expireMS = 60000*30

  poolOrConnection = dbpromise(poolOrConnection)

  function getExpires() {
    return expireMS/60000
  }

  function setExpires(minutes) {
    expireMS = minutes*60000
  }

  async function getCache(table, schema) {
    let key = schema ? `${schema}.${table}` : table
    let cache = fieldCache[key]
    if (!cache || cache.expires < Date.now()) {
      let params = [table]
      let sql = `SELECT COLUMN_NAME , DATA_TYPE 
          ,CASE WHEN COLUMN_KEY LIKE '%PRI%' THEN 1 ELSE 0 END IS_KEY
          ,CASE WHEN LOWER(EXTRA) like '%auto_increment%' THEN 1 ELSE 0 END IS_IDENTITY 
          ,CASE WHEN COLUMN_DEFAULT IS NOT NULL THEN 1 ELSE 0 END HAS_DEFAULT
          FROM information_schema.COLUMNS
          where TABLE_NAME = ? `

      if (schema) {
        sql += " AND TABLE_SCHEMA = ? "
        params.push(schema)
      } else {
        sql += " AND TABLE_SCHEMA = database() "
      }
      // fetch from database
      let fields = await poolOrConnection.query(sql, params)
      cache = { expires: Date.now()+expireMS, fields: [], identity: null, keys: [], byname: null }
      if (fields && fields.length) {
        for(let fld of fields) {
          if (fld.IS_IDENTITY) cache.identity = fld.COLUMN_NAME
          if (fld.IS_KEY) cache.keys.push(fld.COLUMN_NAME)
          cache.fields.push({ 
            name: fld.COLUMN_NAME, 
            primary: fld.IS_KEY,
            identity: fld.IS_IDENTITY,
            default: fld.HAS_DEFAULT,
            type: fld.DATA_TYPE.toLowerCase()})
        }
        cache.byname = Object.assign({}, ...cache.fields.map((x) => ({[x.name]: x})));

      }
      // update cache
      fieldCache[key] = cache
    }
    return cache
  }

  async function getFields(table, schema) {
    let cache = await getCache(table, schema)    
    return cache.fields
  }

  async function getFieldNames(table, schema) {
    let cache = await getCache(table, schema)  
    let names = []
    for(let f of cache.fields) {
      names.push(f.name)
    }  
    return names
  }  

  async function getFieldsByName(table, schema) {
    let cache = await getCache(table, schema)    
    return cache.byname
  }

  async function getPrimaryKeys(table, schema) {
    let cache = await getCache(table, schema)
    return cache.keys
  }

  async function getIdentity(table, schema) {
    let cache = await getCache(table, schema)
    return cache.identity
  }

  return Object.freeze({
    getFields, getFieldsByName, getFieldNames, 
    getPrimaryKeys, 
    getIdentity, getAutoIncrement: getIdentity,
    getExpires, setExpires, 
  })

}


