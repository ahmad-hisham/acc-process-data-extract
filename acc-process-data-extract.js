#!/usr/bin/env node

const fs = require('fs/promises');
const Papa = require('papaparse');
const axios = require('axios').default;
const config = require("./acc-config");
const forgeUrl = "https://developer.api.autodesk.com";

let accessToken = null;

async function readCSVFiles() {
    // Files to process
    const inputFiles = [
        { name: "issues_issues.csv", 			projectProperty: "bim360_project_id", urnProperty: "linked_document_urn" },
        { name: "reviews_review_documents.csv", projectProperty: "bim360_project_id", urnProperty: "versioned_urn" }
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
    let documentsDetails = [];

    for (const project in projects) {
        const projectId = project;
        const documentsUrns = projects[project];

        if (documentsUrns.length == 0)
            continue; // Array is empty, skip

        // Slice the document urns into chunks of 50 each
        const chunkSize = 50;
        let documentsUrnsChuncks = [];
        for (let i = 0; i < documentsUrns.length; i += chunkSize) {
            const chunk = documentsUrns.slice(i, i + chunkSize);
            documentsUrnsChuncks.push(chunk);
        }

        for (const [count, documentsUrnsChunck] of documentsUrnsChuncks.entries()) {
            try {
                const documentsCounter = count * chunkSize + documentsUrnsChunck.length;
                console.log(`Reading ${documentsCounter} / ${documentsUrns.length} documents for project ${projectId}`);
                const listItemsResults = await listItems(projectId, documentsUrnsChunck);
                const versionDetailsResults = await getVersionsDetails(projectId, documentsUrnsChunck);

                listItemsResults.forEach((item) => {
                    // Inject project_id to all items
                    item['project_id'] = projectId;
                    // Inject additional details from version results
                    const itemVersion = versionDetailsResults.find((version) => version.itemUrn === item.id);
                    if (itemVersion) {
                        item['storageSize'] = itemVersion.storageSize;
                        item['revisionNumber'] = itemVersion.revisionNumber;
                        item['customAttributes'] = itemVersion.customAttributes;
                        item['versionedUrn'] = itemVersion.urn;
                    }
                }); 

                documentsDetails = documentsDetails.concat(listItemsResults);
            } catch (err) {
                console.error("ERROR:", JSON.stringify(err.response.data.errors));
            }
        }
    }

    console.log(`Collected ${documentsDetails.length} documents details`);
    return documentsDetails;
}

async function writeDocumentsTable(documentsData) {
    const fileName = "documents_documents.csv";

    // Extract documents list for serialization
    let documentsTable = documentsData.map((document) => ({
            "id": document.id,
            "bim360_project_id": document.project_id,
            "name": document.meta.attributes.displayName,
            "path": document.meta.attributes.pathInProject,
            "version": document.revisionNumber,
            "created_at": document.meta.attributes.createTime,
            "created_by": document.meta.attributes.createUserId,
            "created_by_name": document.meta.attributes.createUserName,
            "updated_at": document.meta.attributes.lastModifiedTime,
            "updated_by": document.meta.attributes.lastModifiedUserId,
            "updated_by_name": document.meta.attributes.lastModifiedUserName,
            "storage_size": document.storageSize,
            "hidden": document.meta.attributes.hidden,
            "type": document.meta.attributes.extension.type,
            "versioned_urn": document.versionedUrn,
            "link": document.meta.links.webView ? document.meta.links.webView.href : '',
            "parent_id": document.meta.relationships.parent ? document.meta.relationships.parent.data.id : ''
        })
    );

    // Convert objects data to CSV string
    let csvData = Papa.unparse(documentsTable, { header: true });

    // Write all data read from all folders
    let csvContents = "\ufeff" + csvData + "\r\n";
    await fs.writeFile(fileName, csvContents, { encoding: "utf8" });

    return fileName;
}

async function writeCustomAttributesTable(documentsData) {
    const fileName = "documents_custom_attributes.csv";

    // Extract documents custom attributes for serialization
    let customAttributesTable = [];
    documentsData.forEach((document) => {
        if (document.customAttributes && document.customAttributes.length > 0) {
            const documentUrn = document.id;
            const documentProject = document.project_id;
            document.customAttributes.forEach((customAttribute) => {
                // Inject document_urn and project_id to all items
                customAttribute['document_urn'] = documentUrn;
                customAttribute['bim360_project_id'] = documentProject;
            });
            customAttributesTable.push(...document.customAttributes);
        }
    });

    // Convert objects data to CSV string
    let csvData = Papa.unparse(customAttributesTable, { header: true });

    // Write all data read from all folders
    let csvContents = "\ufeff" + csvData + "\r\n";
    await fs.writeFile(fileName, csvContents, { encoding: "utf8" });

    return fileName;
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

async function listItems(projectId, documentsUrns) {
    let requestConfig = {
        url: `/data/v1/projects/b.${projectId}/commands`,
        method: 'post',
        headers: {
            'Content-Type': 'application/vnd.api+json'
        },
        data: {
            "jsonapi": {
                "version": "1.0"
            },
            "data": {
                "type": "commands",
                "attributes": {
                    "extension": {
                        "type": "commands:autodesk.core:ListItems",
                        "version": "1.1.0",
                        "data": {
                            "includePathInProject": true
                        }
                    }
                },
                "relationships": {
                    "resources": {
                        "data": documentsUrns.map(documentsUrn => ({ "type": "items", "id": documentsUrn }))
                    }
                }
            }
        }
    };

    let response = await executeForgeCallWithRetry(requestConfig);
    return response.data.relationships.resources.data;
}

async function getVersionsDetails(projectId, documentsUrns) {
    let requestConfig = {
        url: `/bim360/docs/v1/projects/${projectId}/versions:batch-get`,
        method: 'post',
        headers: {
            'Content-Type': 'application/json'
        },
        data: {
            "urns": documentsUrns
        }
    }

    let response = await executeForgeCallWithRetry(requestConfig);
    return response.results;
}

async function executeForgeCallWithRetry(config) {
    // Set Forge BaseURL
    config.baseURL = forgeUrl;

    // Retrieve accessToken if not used
    if (!accessToken)
        accessToken = await getCredentials();
    // Add AccessTokens to headers
    if (!config.headers["Authorization"])
        config.headers["Authorization"] = `Bearer ${accessToken}`;

    try {
        let response = await axios(config);
        return response.data;
    } catch (err) {
        if (err.isAxiosError && err.response && err.response.status == 429 && err.response.headers["retry-after"]) {
            const retryAfter = parseInt(err.response.headers["retry-after"]);
            console.error(`RATE LIMIT: API Quota limit exceeded. Retrying after ${retryAfter} seconds`);
            await new Promise(resolve => setTimeout(resolve, ++retryAfter * 1000));
            return executeForgeCallWithRetry(config);
        } else {
            throw err;
        }
    }
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
    let filenameDocuments = await writeDocumentsTable(documentsData);
    let filenameAttributes = await writeCustomAttributesTable(documentsData);
    console.log(`Written ${filenameDocuments} ${filenameAttributes}`);
}

run();