const mysql = require('mysql')
const moment = require('moment')
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
    listMedsByLabel: listMedsByLabel,
    getPatient: getPatient,
    getPatients: getPatients
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



function listMedsByLabel(startKey, limit, cb) {
    const connection = createConnection()
    limit = limit ? limit : 4
    const whereClause = startKey ? " WHERE concat(m.label, m.ID) > '" + startKey + "'" : ""

    let sql = 'SELECT m.*, concat(m.label, m.ID) as startKey '
    sql += ' FROM medWithIngredients m '
    sql += ' INNER JOIN (SELECT DISTINCT ID '
    sql += ' FROM medWithIngredients m'
    sql += whereClause
    sql += ' LIMIT ' + limit + ') b '
    sql += ' ON m.ID = b.ID '
    sql += whereClause
    sql += ' ORDER BY startKey '

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




////////////////////////
//      Patients
///////////////////////

function getPatient(patientId, cb) {

    if (!patientId) return cb({
        error: 'missing_id',
        reason: 'missing_id',
        name: 'missing_id',
        status: 400,
        message: 'unable to retrieve data due to missing id.'
    })

    const connection = createConnection()

    connection.query('SELECT * FROM patientWithConditions WHERE ID = ?', [patientId], function(err, data) {
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


        cb(null, formatSinglePatient(data))
    })
}


// function getPatients(startKey, limit, cb) {
//
//     const connection = createConnection()
//
//     //PAGINATION FOR PATIENTS
//     limit = limit ? limit : 5
//     const whereClause = startKey ? " WHERE concat(p.lastName, p.ID) > '" + startKey + "'" : ""
//
//     let sql = 'SELECT p.*, concat(p.lastName, p.ID) as startKey '
//     sql += ' FROM patientWithConditions p '
//     sql += ' INNER JOIN (SELECT DISTINCT ID '
//     sql += ' FROM patientWithConditions p'
//     sql += whereClause
//     sql += ' LIMIT ' + limit + ') b '
//     sql += ' ON p.ID = b.ID '
//     sql += whereClause
//     sql += ' ORDER BY startKey'
//
//     console.log("pagination sql for patients: ", sql)
//
//     //PREPAGINATION
//     //let sql = 'SELECT * FROM pharmaStudent.patientWithConditions;'
//
//     connection.query(sql, function(err, data) {
//         if (err) return cb(errorMessage)
//
//         if (data.length === 0) return cb(noDataFound)
//
//         cb(null, formatMultiplePatients(data))
//     })
// }


function getPatients(cb) {

    const connection = createConnection()

    let sql = 'SELECT * FROM pharmaStudent.patientWithConditions;'

    connection.query(sql, function(err, data) {
        if (err) return cb(errorMessage)

        if (data.length === 0) return cb(noDataFound)

        cb(null, formatMultiplePatients(data))
    })

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

function formatMultipleMeds(meds) {
    // 1) map over the incoming meds and extract the ID column value
    // map(med => med.ID, meds)
    // 2) use ramda's uniq() to create a unique list of primary key values
    const IDs = compose(uniq, map(med => med.ID))(meds)

    // 3) map over the unique list of IDs
    // 4) compose and
    //    a) filter the incoming meds with the current id
    //    b) format each filtered med records using formatSingleMed()
    return map(id => compose(
        formatSingleMed,
        filter(med => med.ID === id)
    )(meds))(IDs)
}



function formatSinglePatient(patientRows) {
    const mappedConditions = compose(
        map(patient => patient.condition),
        filter(patient => patient.condition)
    )(patientRows)

    return compose(
        set(lensProp('birthdate'), moment(prop('birthdate', patientRows[0])).format("YYYY-MM-DD")),
        set(lensProp('conditions'), mappedConditions),
        omit(['ID', 'condition']),
        set(lensProp('_id'), toString(prop('ID', patientRows[0]))),
        set(lensProp('_rev'), ""),
        set(lensProp('type'), "patient")
    )(patientRows[0])
}

function formatMultiplePatients(patients) {
    const IDs = compose(
        uniq(),
        map(patient => patient.ID)
    )(patients)

    return map(id => compose(
        formatSinglePatient,
        filter(patient => patient.ID === id)
    )(patients))(IDs)
}



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
