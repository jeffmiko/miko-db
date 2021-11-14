const dbcache = require("./dbcache")
const dbpromise = require("./dbpromise")


class DatabaseTable {

  #cache = null
  #connection = null 
  #datetype = /date|timestamp/i
  #limit = 200

  constructor({table, schema, cache, pool, connection, limit}) {
    if (!table) throw new TypeError("A table is required")
    if (!pool && !connection) throw new TypeError("A pool or connection is required")
    this.table = table 
    this.schema = schema
    this.#connection = dbpromise(pool || connection)
    if (cache) this.#cache = cache 
    else this.#cache = dbcache(this.#connection)
    if (limit) this.#limit = limit
  }

  async getFields() {
    return this.#cache.getFields(this.table, this.schema)
  }

  #parseDate(value) {
    let dt = new Date(value)
    if (isNaN(dt.getTime())) {  
      // date is not valid
      return value
    } else {
      // date is valid
      return dt
    }    
  }

  // add new record
  async add(values) {
    if (!values) throw new TypeError("A values object is required")
    let fieldNames = await this.#cache.getFieldsByName(this.table, this.schema)
    let ident = await this.#cache.getIdentity(this.table, this.schema)
    let fields = []
    let questions = []
    let params = []

    for(let key of Object.keys(values)) {
      let fld = fieldNames[key]
      if (!fld) continue //throw new TypeError(`The field ${key} not found in table ${this.table}`)
      if (fld.identity) throw new TypeError(`The field ${key} is auto incremented in table ${this.table}`)
      fields.push(this.#connection.escapeId(key))
      questions.push(this.#connection.paramId(questions.length+1))
      // parse dates if needed
      if (this.#datetype.test(fld.type) && typeof values[key] ==="string" ) {
        params.push(this.#parseDate(values[key]))
      } else {
        params.push(values[key])
      }
    }

    let sql = null 
    if (this.schema) {
      sql = `INSERT INTO ${this.#connection.escapeId(this.schema)}.${this.#connection.escapeId(this.table)} `
          + `(${fields.join(", ")}) VALUES (${questions.join(", ")})`
    } else {
      sql = `INSERT INTO ${this.#connection.escapeId(this.table)} (${fields.join(", ")}) `
          + `VALUES (${questions.join(", ")}) `
    }

    let data = await this.#connection.query(sql, params)
    if (ident) {
      if (data.insertId) {
        data[ident] = data.insertId
      }      
    }
    return data
  }

  // get one record by primary key(s)
  async get(values) {
    if (!values) throw new TypeError("A values object is required")
    let keys = await this.#cache.getPrimaryKeys(this.table, this.schema)
    let fields = []
    let params = []
    let paramIndex = 1 

    for(let key of keys) {
      if (key in values) {
        fields.push(`${this.#connection.escapeId(key)} = ${this.#connection.paramId(paramIndex)}`)
        paramIndex++
        params.push(values[key])
      } else {
        throw new TypeError(`The primary field ${key} is required`)
      }
    }

    let sql = null 
    if (this.schema) {
      sql = `SELECT * FROM ${this.#connection.escapeId(this.schema)}.${this.#connection.escapeId(this.table)} WHERE ${fields.join(" AND ")} `
    } else {
      sql = `SELECT * FROM ${this.#connection.escapeId(this.table)} WHERE ${fields.join(" AND ")} `
    }

    let results = await this.#connection.query(sql, params)
    if (results && results.length) {
      delete results.meta
      return results[0]
    } else {
      return null
    }

  }

  // save/modifyexisting record
  async save(values) {
    if (!values) throw new TypeError("A values object is required")
    let fieldNames = await this.#cache.getFieldsByName(this.table, this.schema)
    let pkeys = await this.#cache.getPrimaryKeys(this.table, this.schema)
    let fields = []
    let params = []
    let paramIndex=1

    for(let key of Object.keys(values)) {
      let fld = fieldNames[key]
      if (!fld) continue //throw new TypeError(`The field ${key} not found in table ${this.table}`)
      if (fld.primary || fld.identity) continue
      fields.push(this.#connection.escapeId(key)+` = ${this.#connection.paramId(paramIndex)} `)
      paramIndex++
      // parse dates if needed
      if (this.#datetype.test(fld.type) && typeof values[key] ==="string" ) {
        params.push(this.#parseDate(values[key]))
      } else {
        params.push(values[key])
      }
    }

    let where = []
    for(let key of pkeys) {
      where.push(this.#connection.escapeId(key)+` = ${this.#connection.paramId(paramIndex)} `)
      paramIndex++
      if (key in values) params.push(values[key])
      else throw new TypeError(`The field ${key} is required`)   
    }

    let sql = null 
    if (this.schema) {
      sql = `UPDATE ${this.#connection.escapeId(this.schema)}.${this.#connection.escapeId(this.table)} `
          + `SET ${fields.join(", ")} WHERE ${where.join(" AND ")} `
    } else {
      sql = `UPDATE ${this.#connection.escapeId(this.table)} SET ${fields.join(", ")} `
          + `WHERE ${where.join(" AND ")} `
    }
    return await this.#connection.query(sql, params)
  }

  // remove by priary key(s)
  async remove(values) {
    if (!values) throw new TypeError("A values object is required")
    let pkeys = await this.#cache.getPrimaryKeys(this.table, this.schema)
    let paramIndex=1
    let fields = []
    let params = []
    for(let key of pkeys) {
      fields.push(this.#connection.escapeId(key)+` = ${this.#connection.paramId(paramIndex)} `)
      paramIndex++
      if (key in values) params.push(values[key])
      else throw new TypeError(`The primary field ${key} is required`)
    }

    let sql = null 
    if (this.schema) {
      sql = `DELETE FROM ${this.#connection.escapeId(this.schema)}.${this.#connection.escapeId(this.table)} WHERE ${fields.join(" AND ")} `
    } else {
      sql = `DELETE FROM ${this.#connection.escapeId(this.table)} WHERE ${fields.join(" AND ")} `
    }

    return await this.#connection.query(sql, params)
    
  }

  // find by multiple field values
  async find(values, limit) {
    let fieldNames = await this.#cache.getFieldsByName(this.table, this.schema)
    if (!limit) limit = this.#limit
    let fields = []
    let params = []
    let paramIndex=1
    if (values) {
      for(let key of Object.keys(values)) {
        let fld = fieldNames[key]
        if (!fld) continue  //throw new TypeError(`The field ${key} not found in table ${this.table}`)
        fields.push(`${this.#connection.escapeId(key)} = ${this.#connection.paramId(paramIndex)} `)
        paramIndex++ 
        // parse dates if needed
        if (this.#datetype.test(fld.type) && typeof values[key] ==="string" ) {
          params.push(this.#parseDate(values[key]))
        } else {
          params.push(values[key])
        }        
      }
    }

    let where = ""
    if (fields.length) {
      where = `WHERE ${fields.join(" AND ")}`
    }

    let sql = null 
    if (this.schema) {
      sql = `SELECT * FROM ${this.#connection.escapeId(this.schema)}.${this.#connection.escapeId(this.table)} ${where} LIMIT ${limit} `
    } else {
      sql = `SELECT * FROM ${this.#connection.escapeId(this.table)} ${where} LIMIT ${limit} `
    }

    let results = await this.#connection.query(sql, params)
    if (results) {
      delete results.meta
      return results
    } else {
      return []
    }
    
  }


}  
  
module.exports = function dbtable({table, schema, cache, pool, connection}) {
  return new DatabaseTable({table, schema, cache, pool, connection})
}
