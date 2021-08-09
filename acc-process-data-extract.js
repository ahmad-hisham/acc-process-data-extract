#!/usr/bin/env node

const fs = require('fs/promises');
const Papa = require('papaparse');
const axios = require('axios').default;
const config = require("./acc-config");

async function readCSVFiles() {
    // Files to process
    const inputFiles = [
        { name: "issues_issues.csv", 			projectProperty: "bim360_project_id", urnProperty: "linked_document_urn" },
        { name: "reviews_review_documents.csv", projectProperty: "bim360_project_id", urnProperty: "lineage_urn" }
    ];

    let documentUrns = [];
    for (const file of inputFiles) {
        // Read input file
        let csv = await fs.readFile(file.name, { encoding: "utf8" });

        // Parse results as CSV
        const results = Papa.parse(csv, { header: true });
        // Remove EOF
        results.data.pop();

        // Extract project_id and document_urn from input CSV
        const urnsList = results.data.map((entry) =>
            ({ "project_id": entry[file.projectProperty], "documet_urn": entry[file.urnProperty] })
        );

        documentUrns = documentUrns.concat(urnsList);
    }

    console.log(`Collected ${documentUrns.length} URNs from Data Extract`);

    // Group all documents for each project as a hash of projects (key is project_id and value is array of documents)
    const groupedDocuments = groupBy(documentUrns, "project_id", "documet_urn", true);

    return groupedDocuments;
}

async function readUrnsData(projects) {
    const accessToken = await getCredentials();

    return [];
}

async function getCredentials() {
    const client_id = config.credentials.client_id;
    const client_secret = config.credentials.client_secret;
    const scopes = config.scopes;

    let url = 'https://developer.api.autodesk.com/authentication/v1/authenticate';
    let opts = {
        headers: {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
    };
    let data = `client_id=${client_id}&client_secret=${client_secret}&grant_type=client_credentials&scope=${scopes.join('%20')}`;
    let response = await axios.post(url, data, opts);

    return response.data.access_token;
}

function groupBy(objectArray, keyProperty, valueProperty, uniqueOnly = true) {
    return objectArray.reduce((acc, obj) => {
        let key = obj[keyProperty]
        let value = obj[valueProperty];
        if (!acc[key])
            acc[key] = []
        if (value && (acc[key].indexOf(value) == -1 || !uniqueOnly))
            acc[key].push(value)
        return acc
    }, {})
}

// Main entry
async function run() {
    let documentsUrns = await readCSVFiles();
    let documentsData = await readUrnsData(documentsUrns);
}

run();