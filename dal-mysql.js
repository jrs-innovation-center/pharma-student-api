const mysql = require('mysql');

const {
    map,
    filter,
    uniq,
    prop,
    omit,
    compose,
    drop,
    path,
    pathOr,
    view,
    lensIndex,
    set,
    lensPath,
    toString,
    lensProp
} = require('ramda')


var dal = {
    getMed: getMed,
    getPharmacy: getPharmacy,
    listMedsByLabel: listMedsByLabel
}

module.exports = dal

/////////////////////
//   medications
/////////////////////
function getMed(medId, cb) {
    // getDocByID('medWithIngredients', id, formatMed, function(err, res) {
    //     if (err) return callback(err)
    //     //callback(null, view(lensIndex(0), res))
    //     callback(null, res)
    // })
    if (!medId) return cb({
        error: 'missing_id',
        reason: 'missing_id',
        name: 'missing_id',
        status: 400,
        message: 'unable to retrieve medication due to missing id.'
    })

    const connection = createConnection()

    connection.query('SELECT * FROM medWithIngredients WHERE ID = ?', [medId], function(err, data) {
        if (err) return cb({
            error: 'unknown',
            reason: 'unknown',
            name: 'unknown',
            status: 500,
            message: err.message
        })
        if (data.length === 0) return cb({
            error: 'not_found',
            reason: 'missing',
            name: 'not_found',
            status: 404,
            message: 'You have attempted to retrieve a medication that is not in the database or has been deleted.'
        })
        cb(null, formatSingleMed(data))
    })
}


function formatSingleMed(medRows) {
    const mappedIngredients = compose(
        map(med => med.ingredient),
        filter(med => med.ingredient)
    )(medRows)

    return compose(
        omit(['ID', 'ingredient']),
        set(lensProp('ingredients'), mappedIngredients),
        set(lensProp('_id'), toString(prop('ID', medRows[0]))),
        set(lensProp('_rev'), ""),
        set(lensProp('type'), "medication")
    )(medRows[0])
}

function listMedsByLabel(startKey, limit, cb) {

    const connection = createConnection()
    const limitClause = limit ? '' : ' LIMIT ' + limit

    let sql = 'SELECT m.*, concat(m.label, m.ID) as startKey FROM medWithIngredients m '
    sql = startKey ? sql + ' WHERE concat(m.label, m.ID) > "' + startKey + '"' : sql
    sql = sql + ' ORDER BY startKey '
    sql = limit ? sql + ' LIMIT ' + limit : sql + ' LIMIT 10'
    console.log("sql: ", sql)

    connection.query(sql, function(err, data) {
        if (err) return cb({
            error: 'unknown',
            reason: 'unknown',
            name: 'unknown',
            status: 500,
            message: err.message
        })
        if (data.length === 0) return cb({
            error: 'not_found',
            reason: 'missing',
            name: 'not_found',
            status: 404,
            message: 'You have attempted to retrieve medications that are not in the database.'
        })
        cb(null, formatMultipleMeds(data))
    })
}

function formatMultipleMeds(meds) {

    // 1) map over the incoming meds and extract the ID column value
    // 2) use ramda's uniq() to create a unique list of primary key values
    const IDs = compose(
        uniq(),
        map(med => med.ID)
    )(meds)

    // 3) map over the unique list of IDs
    // 4) filter the incoming meds with the current id
    // 5) format each filtered med records using formatSingleMed()
    return map(id => compose(formatSingleMed,
                             filter(med => med.ID === id)
                            )(meds), IDs)
}


////////////////////////
//      Patients
///////////////////////

function getPatient(patientId, cb) {

}

////////////////////////
//      Pharmacy
///////////////////////
function getPharmacy(pharmacyId, cb) {

    if (!pharmacyId) return cb({
        error: 'missing_id',
        reason: 'missing_id',
        name: 'missing_id',
        status: 400,
        message: 'unable to retrieve data due to missing id.'
    })

    const connection = createConnection()

    connection.query('SELECT * FROM pharmacy WHERE ID = ?', [pharmacyId], function(err, data) {
        if (err) return cb({
            error: 'unknown',
            reason: 'unknown',
            name: 'unknown',
            status: 500,
            message: err.message
        })
        if (data.length === 0) return cb({
            error: 'not_found',
            reason: 'missing',
            name: 'not_found',
            status: 404,
            message: 'missing'
        })

        const typeLens = lensProp('type')
        const revLens = lensProp('_rev')
        const idLens = lensProp('_id')
        let idValue = prop('ID', data[0])
        idValue = toString(idValue)

        const theResult = compose(
            omit('ID'),
            set(idLens, idValue),
            set(revLens, ""),
            set(typeLens, "pharmacy")
        )(data[0])

        cb(null, theResult)
    })
}

/////////////////////
// helper functions
/////////////////////

function createConnection() {
    return mysql.createConnection({
        host: "0.0.0.0",
        user: "root",
        password: "mypassword",
        database: "pharmaStudent"
    });
}



function getDocByID(tablename, id, formatter, callback) {
    //  console.log("getDocByID", tablename, id)
    if (!id) return callback({
        error: 'missing_id',
        reason: 'missing_id',
        name: 'missing_id',
        status: 400,
        message: 'unable to retrieve data due to missing id.'
    })

    var connection = createConnection()

    connection.query('SELECT * FROM ' + connection.escapeId(tablename) + ' WHERE id = ?', [id], function(err, data) {
        if (err) return callback({
            error: 'unknown',
            reason: 'unknown',
            name: 'unknown',
            status: 500,
            message: err.message
        })
        if (data.length === 0) return callback({
            error: 'not_found',
            reason: 'missing',
            name: 'not_found',
            status: 404,
            message: 'missing'
        });

        if (data) {
            //console.log("query returned with data", formatter, formatter(data[0]))
            // grab the item sub [0] from the data.
            // take the data and run it through the formatter (convertPersonNoSQLFormat)
            // then take result of converting the person and parseToJSON
            return callback(null, formatter(data))
        }
    });
    connection.end(function(err) {
        if (err) return err;
    });
}
