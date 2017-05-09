# Pharmacy API

## Getting Started

> The pharmacy API depends on [CouchDB](http://couchdb.apache.org/) running on locally on http://127.0.0.1:5984/.  You can adjust the port number within **dal.js** and **bulk-add.js**, if necessary.

0. Run the following terminal commands to clone the repo from GitHub, install the project dependencies, load the data into a database named `pharma-student` and start the API on port 8080:

    ```
    $ git clone https://github.com/jrs-innovation-center/pharma-student-api.git
    $ cd pharma-student-api
    $ npm install
    $ node bulk-add.js
    $ npm start
    ```

0. Open your browser to [http://localhost:8080/](http://localhost:8080/) to test the API.
0. Next, view all the meds via a `GET` to  [http://localhost:8080/medications](http://localhost:8080/medications).
0. Next, try a `GET` to [http://localhost:8080/pharmacies](http://localhost:8080/pharmacies).
0. And a `GET` to [http://localhost:8080/patients](http://localhost:8080/patients).

> Any errors will be logged to the console/terminal.

# Medications

## `GET /medications`

Returns a collection of medications default sorted by label.  You may optionally filter the medications by ingredient or form, such as patch, tablet, syrup. Attempting any other filter will result in an `200` response code with an empty array returned.

**Request URL**

```
http://localhost:8080/medications
```

**Examples**

Get medications that contain the ingredient Aspirin.

```
$ curl -X GET http://localhost:8080/medications?filter=ingredient:Aspirin
```

**Query Parameters**

 <table class="table table-striped table-hover">
      <thead>
        <tr>
          <th>Parameter</th>
          <th>Required</th>
          <th>Type</th>
          <th>Description</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td>filter</td>
          <td>false</td>
          <td>string</td>
          <td>Filters a collection of medications by specified ingredient or form, such as, (tablet, syrup, patch).
          <ul>
          <li>Filter By ingredient example:  GET /medications?filter=ingredient:aspirin.</li>
          <li>Filter by form example: GET /medications?filter=form:tablet.</li>
          </ul>
          </td>
        </tr>
      </tbody>
    </table>

**Response 200**

A successful response will include an array of medications.  

<pre><code>[
    {
        "_id": "medication_amlodipine_10mg_tablet",
        "_rev": "3-f071655fdafe4bd3d8cbbc22b11dacd0",
        "label": "Amlodipine 10mg tablet",
        "ingredients": [
            "Amlodipine",
            "Aspirin"
        ],
        "amount": "10",
        "unit": "mg",
        "form": "tablet",
        "type": "medication"
    },
    {
        "_id": "medication_spironolactone_100mg_tablet",
        "_rev": "3-01ca16f7eaede703edac68d60d6c0333",
        "label": "Lisinopril 100mg tablet",
        "ingredients": [
            "spironolactone",
            "tamezipam"
        ],
        "amount": "100",
        "unit": "mg",
        "form": "tablet",
        "type": "medication"
    },
    {
        "_id": "medication_lisinopril_20mg_tablet",
        "_rev": "5-28bab875b5388b6e99627b347c88e668",
        "label": "Lisinopril 20mg tablet",
        "ingredients": [
            "Lisinopril"
        ],
        "amount": "20",
        "unit": "mg",
        "form": "tablet",
        "type": "medication"
    }
]</code>
</pre>

**Response `404`**

Returned when the specified action is not found.

<pre><code>Cannot GET /medicat?filter=ingredient:Aspirin</code></pre>


## `GET /medications/{medicationId}`

Return a medication for a given medication id.

**Request URL**

<pre><code>http://localhost:8080/medications/{medicationId}</code></pre>

**Request Parameters**

<table class="table table-striped table-hover">
  <thead>
    <tr>
      <th>Parameter</th>
      <th>Required</th>
      <th>Type</th>
      <th>Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>medicationId</td>
      <td>true</td>
      <td>string</td>
      <td>Unique identifier for a medication.
      </td>
    </tr>
  </tbody>
</table>

**Examples**

Get a medication


<pre><code>$ curl -X GET -H "Content-Type: application/json" -H "Cache-Control: no-cache" -H "Postman-Token: 4c096411-c83d-98b5-dcf3-1bab3a02363b" "http://localhost:8080/medications/medication_nyquil_200%20ml%20syrup_syrup"</code></pre>


**Response `200`**

<pre><code>{
    "_id": "medication_nyquil_200 ml syrup_syrup",
    "_rev": "1-dc27efb54edd17daa58b52d30990d071",
    "label": "Nyquil 200 ml syrup",
    "ingredients": [
        "Ibuprofin"
    ],
    "amount": "200",
    "unit": "ml",
    "form": "syrup",
    "type": "medication"
}</code></pre>


## `POST /medications`

Adds a medication the collection of medications.  

**Request URL**

<pre><code>http://localhost:8080/medications</code></pre>

**Request Body**

<table class="table table-striped table-hover">
  <thead>
    <tr>
      <th>Property</th>
      <th>Required</th>
      <th>Type</th>
      <th>Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>label</td>
      <td>true</td>
      <td>string</td>
      <td>medication label including med name, amount, unit, and form.
      </td>
    </tr>
    <tr>
      <td>ingredients</td>
      <td>true</td>
      <td>array</td>
      <td>An array of strings. Each item in the array is the name of an ingredient.  
      </td>
    </tr>
    <tr>
      <td>amount</td>
      <td>true</td>
      <td>string</td>
      <td>The medication amount.
      </td>
    </tr>
    <tr>
      <td>form</td>
      <td>true</td>
      <td>string</td>
      <td>The form of the medication such as tablet, syrup, patch.
      </td>
    </tr>
  </tbody>
</table>


**Examples**

Creates a medication by providing a new medication as JSON in the request body:

<pre><code>curl -X POST -H "Content-Type: application/json" -H "Cache-Control: no-cache" -H "Postman-Token: 62e6431c-c7e5-b197-c12b-394f4d73d27e" -d '{
    "label": "Amlodipine 200mg syrup",
    "ingredients": [
      "Amlodipine",
      "Aspirin"
    ],
    "amount": "200",
    "unit": "mg",
    "form": "syrup"
  }' "http://localhost:8080/medications"</code></pre>

**Response `201`**

Returned when the operation successfully creates a medication. The response body contains the id of the new medication.


<pre><code>{
  "ok": true,
  "id": "medication_amlodipine_200mg_syrup",
  "rev": "1-4efe163301bb197b58f837b931558f9d"
}</code></pre>


**Response `409`**

Returned when an attempt is made to create a duplicate medication.

<pre><code>
{
  "name": "conflict",
  "status": 409,
  "message": "Document update conflict.",
  "reason": "Document update conflict.",
  "error": "conflict"
}
</code></pre>





## `PUT /medications/{medicationId}`

Updates a medication for a given medication id.

**Request URL**

<pre><code>http://localhost:8080/medications/{medicationId}</code></pre>

**Request Parameters**

<table class="table table-striped table-hover">
  <thead>
    <tr>
      <th>Parameter</th>
      <th>Required</th>
      <th>Type</th>
      <th>Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>medicationId</td>
      <td>true</td>
      <td>string</td>
      <td>Unique identifier for a medication.
      </td>
    </tr>
  </tbody>
</table>

**Examples**

Update a medication

<pre><code>$ curl -X PUT -H "Content-Type: application/json" -H "Cache-Control: no-cache" -H "Postman-Token: 7e7ee860-e153-7269-4117-9d5693e96cc4" -d '{
  "_id": "medication_spironolactone_100mg_patch",
  "_rev": "1-5ff9124953b429fc04080f38fbffaa18",
  "label": "Spironolactone 100mg patch",
  "ingredients": [
    "spironolactone",
    "sucrose"
  ],
  "amount": "100",
  "unit": "mg",
  "form": "patch",
  "type": "medication"
}' "http://localhost:8080/medications/medication_spironolactone_100mg_patch"</code></pre>

**Response `200`**

<pre><code>{
  "ok": true,
  "id": "medication_spironolactone_100mg_patch",
  "rev": "2-19929a6d15379c87e5e056a555b414b5"
}</code></pre>


**Response `409`**

Returned when an attempt is made to update a medication with an old revision number.

<pre><code>{
  "name": "conflict",
  "status": 409,
  "message": "Document update conflict.",
  "reason": "Document update conflict.",
  "error": "conflict"
}</code></pre>



## `DELETE /medications/{medicationId}`

Deletes a medication for a given medication id.

**Request URL**

<pre><code>http://localhost:8080/medications/{medicationId}</code></pre>

**Request Parameters**

<table class="table table-striped table-hover">
  <thead>
    <tr>
      <th>Parameter</th>
      <th>Required</th>
      <th>Type</th>
      <th>Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>medicationId</td>
      <td>true</td>
      <td>string</td>
      <td>Unique identifier for a medication.
      </td>
    </tr>
  </tbody>
</table>

**Examples**

Deletes a medication

<pre><code>$ curl -X DELETE -H "Content-Type: application/json" -H "Cache-Control: no-cache" -H "Postman-Token: 3b2f8365-26a8-7c67-424d-5eb09bbc9065" -d '' "http://localhost:8080/medications/medication_spironolactone_200mg_syrup"</code></pre>

**Response `200`**

<pre><code>{
  "ok": true,
  "id": "medication_spironolactone_200mg_syrup",
  "rev": "2-7b7aa943e265b55dc6776e977d890542"
}</code></pre>


**Response `404`**

Returned when an attempt is made to delete a medication with a bad or missing `medicationId` request parameter or when a medication has already been deleted.

<pre><code>{
  "name": "not_found",
  "status": 404,
  "message": "deleted",
  "reason": "deleted",
  "error": "not_found"
}</code></pre>



# Pharmacies

## `POST /pharmacies`

**Examples**

Creates a pharmacy by providing a new pharmacy as JSON in the request body:

<pre><code>curl -X POST -H "Content-Type: application/json" -H "Cache-Control: no-cache" -H "Postman-Token: 484fbe93-d990-428d-8942-ba247b4ce09d" -d '{
    "type": "pharmacy",
    "storeNumber": "1004",
    "storeChainName": "Walgreens",
    "storeName": "Hwy 17 S. Myrtle Beach",
    "streetAddress": "250 South Myrtle Beach Market Road",
    "phone": "843-777-1111",
    "city": "Myrtle Beach",
    "state": "SC",
    "zip": "29345"
  }' "http://localhost:8080/pharmacies"</code></pre>

**Response `201`**

<pre><code>
{
  "ok": true,
  "id": "pharmacy_Walgreens_Hwy_17_S._Myrtle_Beach_1004",
  "rev": "1-303c4dd05db46df6680146b971113328"
}
</code></pre>

# Patients

## `POST /patients`

**Examples**

Creates a patient by providing a new patient as JSON in the request body:

<pre><code>$ curl -X POST -H "Content-Type: application/json" -H "Cache-Control: no-cache" -H "Postman-Token: 0d5cbbeb-a77d-482c-e20b-fa18f159c52d" -d '{
    "patientNumber": 1001,
    "firstName": "Steve",
    "lastName": "Austin",
    "birthdate": "1940-09-01",
    "gender": "M",
    "ethnicity": "W",
    "last4SSN": 3033,
    "conditions": [
      "Depression",
      "Hypertension"
    ]
  }' "http://localhost:8080/patients"</code></pre>


**Response `201`**

<pre><code>{
  "ok": true,
  "id": "patient_austin_steve_3033_1001",
  "rev": "1-252282dad5d90055fdd2ff1667d5f666"
}</code></pre>
