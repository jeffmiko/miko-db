
class DatabaseNativePromise {

  #connection = null

  constructor(poolOrConnection) {
    this.#connection = poolOrConnection
  }

  escapeId(value) {
    return this.#connection.escapeId(value)
  }

  paramId(index) {
    return '?'
  }

  async end() {
    return this.#connection.end()
  }

  async query(sql, values) {
    if (values) return this.#connection.query(sql, values)
    else return this.#connection.query(sql)
  }


}


class DatabasePromise {

  #connection = null

  constructor(poolOrConnection) {
    this.#connection = poolOrConnection
  }

  escapeId(value) {
    return this.#connection.escapeId(value)
  }

  paramId(index) {
    return '?'
  }

  async end() {
    return new Promise((resolve, reject) => {
      this.#connection.end(err => {
        if (err) reject(err)
        else resolve(true)
      })
    })
  }

  async query(sql, values) {
    return new Promise((resolve, reject) => {
      if (values) {
        this.#connection.query(sql, values, (err, rows)=> {
          if (err) {
            reject(err)
          } else {
            resolve(rows)
          }
        })
      } else {
        this.#connection.query(sql, (err, rows)=> {
          if (err) {
            reject(err)
          } else {
            delete rows.meta
            resolve(rows)
          }
        })
      }
    })
  }



  
}

class MySqlPromise extends DatabasePromise {

  constructor(poolOrConnection) {
    super(poolOrConnection)
  }
  
  get mysql() { return true }

}


class PgDatabasePromise extends DatabasePromise {

  constructor(poolOrConnection) {
    super(poolOrConnection)
  }

  get pg() { return true }

  escapeId(value) {
    return `"${value}"`
  }

  paramId(index) {
    return `$${index}`
  }

}



// takes a connection or pool from mysql, mysql2, or mariadb
// wraps in a promise if needed
module.exports = function dbpromise(poolOrConnection) {
  if (poolOrConnection.constructor.name == 'MySqlPromise') return poolOrConnection
  if (poolOrConnection.constructor.name == 'PgDatabasePromise') return poolOrConnection

  if (/^boundpool$/i.test(poolOrConnection.constructor.name)) {
    return new PgDatabasePromise(poolOrConnection)
  } 
  if (/^client$/i.test(poolOrConnection.constructor.name)) {
    return new PgDatabasePromise(poolOrConnection)
  } 
  if (/promise/i.test(poolOrConnection.constructor.name)) {
    return new DatabaseNativePromise(poolOrConnection)
  } 
  if (/callback/i.test(poolOrConnection.constructor.name)) {
    return new MySqlPromise(poolOrConnection)
  } 
  if (typeof poolOrConnection._queryPromise === "function") {
    return new MySqlPromise(poolOrConnection)
  } 
  return new MySqlPromise(poolOrConnection)
}
