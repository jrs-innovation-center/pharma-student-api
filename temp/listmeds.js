
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
