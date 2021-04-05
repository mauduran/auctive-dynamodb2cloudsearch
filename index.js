const aws = require('aws-sdk');

let CloudSearch = new aws.CloudSearchDomain({
    region: process.env.REGION,
    endpoint: process.env.AWS_CLOUDSEARCH_ENDPOINT,
    apiVersion: '2013-01-01'
});

const create_cs_request = (id, record, operation) => {
    let request = {
        type: operation,
        id: id
    }
    if (operation == 'add') {
        let fields = {
            end_date: record.end_date.S,
            owner_email: record.owner_email.S,
            description: record.description.S,
            title: record.title.S,
            starting_price: record.starting_price.N,
            buy_now_price: record.buy_now_price.N,
            category: record.category.S,
            product_img_urls: record.product_img_urls.L.map(url=>url.S),
            start_date: record.start_date.S,
            status: record.status.S
        };

        if (record.tags) {
            tags = record.tags.L.map(el => el.S);
            fields['tags'] = tags;
        }

        request['fields'] = fields
    }

    return request;
}

function send_record_requests(requests) {
    var params = {
        contentType: 'application/json',
        documents: JSON.stringify(requests)
    }

    return new Promise((resolve, reject) => {
        CloudSearch.uploadDocuments(params, function (err, data) {
            if (err) {
                reject(err);
            } else {
                resolve(data);
            }
        });
    })
};


exports.handler = async (event, context) => {
    let requests = [];

    for (const record of event.Records) {
        try {
            let pk = record.dynamodb.Keys.PK.S;
            if (pk.includes("AUCTION#") && record.dynamodb.Keys.SK.S.includes("#AUCTION_USER#")) {
                if (record.eventName === "INSERT" || record.eventName === "MODIFY") {
                    requests.push(create_cs_request(pk, record.dynamodb.NewImage, "add"));
                } else if (record.eventName === "REMOVE") {
                    requests.push(create_cs_request(pk, record.dynamodb.OldImage, "delete"));
                }
            }

            if (requests.length) await send_record_requests(requests);
            return `Successfully processed ${requests.length} records.`;
        }
        catch (err) {
            console.log(err);
            return "Could not process records;"
        }
    }
}
