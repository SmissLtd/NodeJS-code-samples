"use strict";

var errorMaker = require("./error-maker");
var loader = require("./cs-wrappers").loader;
var pusher = require("./cs-wrappers").pusher;
var deleter = require("./cs-wrappers").deleter;
var diffChange = require("./difference").diffChange;
var diffConfigRequest = require("./difference").diffConfigRequest;
var extend = require("util")._extend;
var curry = require("ramda").curry;
var always = require("ramda").always;
var fs = require('fs');
var async = require('async');
var request = require('request');
var assert = require('assert');
var config = require('./config');
var MongoClient = require('mongodb').MongoClient;
var loadTypesToMigrate = require("./cs-wrappers").loader.loadTypesToMigrate;
var sendFiles = require('sftp-fileSender').sendFiles;
var notifyAutomaticApplianceService = require("./post-apply").notifyAutomaticApplianceService;
var checkExistenceOfEntity = require('./validation/change-modification-validators/index.js').checkExistenceOfEntity;
var clone = require('clone');
var promisify = require('es6-promisify');
var writeFile = promisify(fs.writeFile);

const CHANGES_TERM = "changes";
const CREATE_ACTION = "create";
const UPDATE_ACTION = "update";
const DELETE_ACTION = "delete";
const actions = [
    CREATE_ACTION,
    UPDATE_ACTION,
    DELETE_ACTION
];

const CONCURRENT_MODIFICATION = {
    CREATED: 'CREATED',
    UPDATED: 'UPDATED',
    DELETED: 'DELETED'
};
const ORDER_TEMPLATE_LOCATION = "./lib/resources/order.xml";

function makeConcurrentModificationError(modification) {
    return errorMaker("ConcurrentModification:" + modification, "The entity you try to change was already " + modification.toLowerCase(), 412);
}

function makeErrorResponse(action, client, id, next) {
    return err=>next({
        name: id,
        action: action,
        tenant: client,
        statusCode: err.status || (err.error ? err.error.status : 500),
        success: false,
        error: (err.error || err).message
    });
}

function makeSuccessResponse(action, client, id, resp) {
    return ()=>resp.json({
        name: id,
        action: action,
        tenant: client,
        statusCode: 200,
        success: true
    });
}

function addConfigRequest(req, resp, next) {
    const updater = req.query.updater;
    const client = req.query.client;
    const id = req.params.id;
    const entity = req.body;
    const migration = req.query.migration === "true";
    entity.name = id;
    pusher.saveConfigRequest(client, updater, entity, migration ? "POST" : "PUT")
        .then(notifyAutomaticApplianceService(id, client, CREATE_ACTION, entity.applyingDate))
        .then(makeSuccessResponse(CREATE_ACTION, client, id, resp))
        .catch(makeErrorResponse(CREATE_ACTION, client, id, next));
}


function checkForAlreadyApplied(configRequest) {
    if (configRequest.applied === true) {
        let error = new Error("Config request '" + configRequest.name + "' already applied and can't be modified");
        error.status = 412;
        error.name = "ConfigRequestAlreadyApplied";
        throw error;
    }
    return configRequest;
}
var isMigratable = configRequest => {
    if (!configRequest.changes || !configRequest.toMigrate) {
        return Promise.resolve(configRequest);
    }

    return loadTypesToMigrate().then(types=> {
        var nonMigratableTypes = Object.keys(configRequest.changes).filter(type =>!~types.indexOf(type));
        if (nonMigratableTypes.length) {
            let message = "Type '" + nonMigratableTypes.toString() + "'" + (nonMigratableTypes.length === 1 ? "is" : "are") + " not suitable to migrate";
            let err = new Error(message);
            err.status = 412;
            err.name = "TypeNotSuitable";
            throw err;
        }
        return configRequest;
    });
};

var saveConfigRequest = (client, updater, method)=>configRequest=> {
    return pusher.saveConfigRequest(client, updater, configRequest, method).then(always(configRequest));
};

var applyModificationsToConfigRequest = modifications=>configRequest=>Object
    .keys(modifications)
    .reduce((acc, key) => (acc[key] = modifications[key], acc), configRequest);

function updateConfigRequest(req, resp, next) {
    var client = req.query.client;
    var id = req.params.id;
    var updater = req.query.updater;
    var modifications = req.body;
    loader.loadConfigRequest(client, id)
        .then(checkForAlreadyApplied)
        .then(applyModificationsToConfigRequest(modifications))
        .then(isMigratable)
        .then(saveConfigRequest(client, updater, "POST"))
        .then(notifyAutomaticApplianceService(id, client, UPDATE_ACTION, modifications.applyingDate))
        .then(makeSuccessResponse(UPDATE_ACTION, client, id, resp))
        .catch(makeErrorResponse(UPDATE_ACTION, client, id, next));

}

function deleteConfigRequest(req, resp, next) {
    var id = req.params.id;
    var client = req.query.client;
    deleter(client, id)
        .then(notifyAutomaticApplianceService(id, client, DELETE_ACTION))
        .then(makeSuccessResponse(DELETE_ACTION, client, id, resp))
        .catch(makeErrorResponse(DELETE_ACTION, client, id, next));
}

function doesActionExist(configRequest, datatype, category, action) {
    return (configRequest[CHANGES_TERM] &&
    configRequest[CHANGES_TERM][datatype] &&
    configRequest[CHANGES_TERM][datatype][category] &&
    configRequest[CHANGES_TERM][datatype][category][action]);
}

function getEntity(configRequest, datatype, category, action, entityId) {
    if (!doesActionExist(configRequest, datatype, category, action)) {
        return null;
    }
    return configRequest[CHANGES_TERM][datatype][category][action].find(item => item.name === entityId);
}

/**
 * Get action performed on given entity in given CR.
 * Returns undefined if given entity was not changed in this CR
 */
function getActionForEntity(configRequest, datatype, category, entityId) {
    return actions.find(action => {
        return configRequest[CHANGES_TERM] && configRequest[CHANGES_TERM][datatype] &&
            configRequest[CHANGES_TERM][datatype][category] &&
            configRequest[CHANGES_TERM][datatype][category][action] &&
            configRequest[CHANGES_TERM][datatype][category][action].some(item => item.name === entityId)
    });
}

var deleteChangeFromConfigRequest = curry((configRequest, datatype, category, entityId, action) => {
    if (doesActionExist(configRequest, datatype, category, action)) {
        configRequest[CHANGES_TERM][datatype][category][action] = configRequest[CHANGES_TERM][datatype][category][action]
            .filter(isNameTheSame(entityId));
    }
});


function removeDuplicates(configRequest, datatype, category, action, entityId) {
    actions.filter((actionName) => action !== actionName)
        .forEach(deleteChangeFromConfigRequest(configRequest, datatype, category, entityId));
}
function isEmptyActionPart(changes, datatype, category) {
    return action =>changes[datatype] && changes[datatype][category][action] && !changes[datatype][category][action].length;
}

function isEmptyDataType(changes, datatype) {
    return !Object.keys(changes[datatype]).length;
}
function isCategoryEmpty(changes, datatype, category) {
    return !Object.keys(changes[datatype][category]).length;
}

function deleteTerm(obj) {
    var args = Array.prototype.slice.call(arguments, 1);
    var termToDelete = args.pop();
    var temp = obj;
    while (args.length) {
        temp = temp[args.shift()];
    }
    delete temp[termToDelete];
}
function cleanCategory(changes, datatype) {
    return category=> {
        actions
            .filter(isEmptyActionPart(changes, datatype, category))
            .forEach(actionName => deleteTerm(changes, datatype, category, actionName));
        if (isCategoryEmpty(changes, datatype, category)) {
            deleteTerm(changes, datatype, category);
        }
    };
}
function cleanDataType(changes) {
    return datatype => {
        Object.keys(changes[datatype]).forEach(cleanCategory(changes, datatype));
        if (isEmptyDataType(changes, datatype)) {
            deleteTerm(changes, datatype);
        }
    };
}
function cleanConfigRequest(configRequest) {
    var changes = configRequest[CHANGES_TERM];
    if (changes) {
        Object
            .keys(changes)
            .forEach(cleanDataType(changes));
        if (!Object.keys(changes).length) {
            deleteTerm(configRequest, CHANGES_TERM);
        }
    }
}

function buildUpdateChangeResult(datatype, isDeleteChange, timestamp) {
    return (configRequest)=> {
        var res = {};
        if (configRequest[CHANGES_TERM]) {
            res[datatype] = configRequest[CHANGES_TERM][datatype] || {};
        }
        if (!isDeleteChange) {
            res.lastUpdate = timestamp;
        }
        return res;
    };
}

function isNameTheSame(entityId) {
    return item=>item.name !== entityId;
}

function initChanges(configRequest, datatype, category, action) {
    if (!configRequest[CHANGES_TERM]) {
        configRequest[CHANGES_TERM] = {};
    }

    if (!configRequest[CHANGES_TERM][datatype]) {
        configRequest[CHANGES_TERM][datatype] = {};
    }
    if (!configRequest[CHANGES_TERM][datatype][category]) {
        configRequest[CHANGES_TERM][datatype][category] = {};
    }

    if (!configRequest[CHANGES_TERM][datatype][category][action]) {
        configRequest[CHANGES_TERM][datatype][category][action] = [];
    }
}

function addChangeToConfigRequest(configRequest, datatype, category, action, entity) {
    initChanges(configRequest, datatype, category, action);
    deleteChangeFromConfigRequest(configRequest, datatype, category, entity.name, action);
    configRequest[CHANGES_TERM][datatype][category][action].push(entity);
}

function checkLastUpdateOfEntity(dataType, category, action, entityId, lastUpdate, isDeleteChange) {
    return configRequest => {
        let lastAction = getActionForEntity(configRequest, dataType, category, entityId);
        if (lastAction) {
            if (isDeleteChange) {
                if (getEntity(configRequest, dataType, category, lastAction, entityId).timestamp != lastUpdate) {
                    throw makeConcurrentModificationError(CONCURRENT_MODIFICATION.UPDATED);
                }
            } else {
                switch (action) {
                    case CREATE_ACTION:
                    {
                        if (getEntity(configRequest, dataType, category, lastAction, entityId).timestamp != lastUpdate) {
                            throw makeConcurrentModificationError(CONCURRENT_MODIFICATION.CREATED);
                        }
                    }
                    case UPDATE_ACTION:
                    {
                        if (lastAction == DELETE_ACTION) {
                            throw makeConcurrentModificationError(CONCURRENT_MODIFICATION.DELETED);
                        } else if (getEntity(configRequest, dataType, category, lastAction, entityId).timestamp != lastUpdate) {
                            throw makeConcurrentModificationError(CONCURRENT_MODIFICATION.UPDATED);
                        }
                        break;
                    }
                    case DELETE_ACTION:
                    {
                        if (lastAction == DELETE_ACTION) {
                            throw makeConcurrentModificationError(CONCURRENT_MODIFICATION.DELETED);
                        } else if (getEntity(configRequest, dataType, category, lastAction, entityId).timestamp != lastUpdate) {
                            throw makeConcurrentModificationError(CONCURRENT_MODIFICATION.UPDATED);
                        }
                        break;
                    }
                }
            }
        }
        return configRequest;
    };
}

function handleNewChange(dataType, category, action, entity, isDeleteChange) {
    return configRequest => {
        var entityId = entity.name;
        if (isDeleteChange) {
            deleteChangeFromConfigRequest(configRequest, dataType, category, entityId, action);
        } else {
            addChangeToConfigRequest(configRequest, dataType, category, action, entity);
            removeDuplicates(configRequest, dataType, category, action, entityId);
        }
        cleanConfigRequest(configRequest);
        return configRequest;
    };
}

function makeEntityToChange(data, name, configRequestId, updater) {
    let commonFields = {
        name: name,
        configRequestId: configRequestId,
        timestamp: Date.now(),
        lastUpdater: updater
    };
    return extend(extend({}, data || {}), commonFields); // why not clone?
}

function getTypeData(type, category, client, fields) {
    return new Promise((resolve, reject)=> {
        if (!category) {
            reject({message: "Category should exist."});
            return;
        }
        fields = fields ? fields.join(",") : "";

        var url = config.TYPE_DATA_URL + type + "?category=" + category + "&fields=" + fields + "&client=" + client;

        var options = {
            url: url,
            rejectUnauthorized: false,
            checkServerIdentity: function (host, cert) {
                return true;
            }
        };

        request(options, function (error, response, body) {
            if (error) {
                reject(error);
                return;
            }
            resolve(JSON.parse(body));
        });
    });
}

function addChanges(configRequest, configRequestId, updater, data, dataType, category, action, ids, lastUpdate, isDeleteChange, client) {
    return Promise.resolve(configRequest)
        .then(function addChange(configRequest) {
            if (ids[0]) {
                if (!configRequest.changedAfterValidation)
                    configRequest.changedAfterValidation = true;
                let name = ids[0];
                let entity = makeEntityToChange(data, name, configRequestId, updater);
                ids.shift();

                return Promise.resolve(configRequest)
                    .then(checkLastUpdateOfEntity(dataType, category, action, name, lastUpdate, isDeleteChange))
                    .then(handleNewChange(dataType, category, action, entity, isDeleteChange))
                    .then(isMigratable)
                    .then(diffChange(client, data, dataType, name, category, action, isDeleteChange))
                    .then(addChange);
            }
            else {
                return configRequest;
            }
        });
}

function modifyChanges(req, resp, done) {
    var isDeleteChange = req.method.toLowerCase() === "delete";
    var dataType = req.params.type;
    var action = req.params.action;
    var updater = req.query.updater;
    var category = req.query.category || dataType;
    var entityId = req.params.id;
    var client = req.query.client;
    var lastUpdate = req.query.lastUpdate;
    var configRequestId = req.params.crId;
    var data = req.body;
    var ids = [entityId];

    var main = function () {
        loader.loadConfigRequest(client, configRequestId)
            .then(checkForAlreadyApplied)
            .then(cr=>addChanges(cr, configRequestId, updater, data, dataType, category, action, ids, lastUpdate, isDeleteChange, client))
            .then(saveConfigRequest(client, updater, "POST"))
            .then(buildUpdateChangeResult(dataType, isDeleteChange, Date.now()))
            .then(resp.json.bind(resp))
            .catch(done);
    };

    //delete_all route check
    if (entityId === undefined) {
        action = "delete";

        getTypeData(dataType, category, client, ["name"])
            .then(function (res) {
                ids = res.map(item=>item.name);
                main();
            })
            .catch(error=> {
                done(error);
            });
    } else {
        main();
    }
}

function addJSONConfigRequest(req, resp, done) {
    const FILE_KEY = 'config_request';
    let request = {
        params: req.params,
        query: req.query,
        method: 'put'
    };
    var isDeleteChange = req.method.toLowerCase() === "delete";
    var updater = req.query.updater;
    var client = req.query.client;
    var configRequestId = req.params.crId;
    let errors = [];

    function parseData(body) {
        return new Promise(function (resolve, reject) {
            var params = [];

            var parseEnd = function (clone, body, type, category, callback) {
                if (!body.name) {
                    errors.push({error: 'Error: name should exist!', body: clone});
                    callback();
                }
                else {
                    clone.params.id = body.name;

                    checkExistenceOfEntity(body.name, clone.query.client, type, category)
                        .then(exists=> {
                            if (exists)
                                clone.params.action = 'update';
                            else
                                clone.params.action = 'create';

                            params.push(clone);
                            callback();
                        });
                }
            };
            // forEachOfLimit because we send requests for check existence of entity.
            async.forEachOfLimit(body, 10,
                function (value, type, typesEnd) {
                    let clone = JSON.parse(JSON.stringify(request));
                    clone.params.type = type;

                    async.forEachOfLimit(value, 10,
                        function (body, id, idsEnd) {
                            if (type == 'helptable') {
                                async.forEachOfLimit(body.helptables, 10, function (body, num, cb) {
                                    let clone2 = JSON.parse(JSON.stringify(clone));
                                    clone2.body = body;
                                    clone2.query.category = id;
                                    parseEnd(clone2, body, type, id, cb);
                                }, idsEnd);
                            }
                            else {
                                let clone2 = JSON.parse(JSON.stringify(clone));
                                clone2.body = body;
                                parseEnd(clone2, body, type, type, idsEnd);
                            }
                        }, typesEnd);
                },
                function () {
                    resolve(params);
                });
        });
    }

    function changeChanges(params) {
        return loader.loadConfigRequest(client, configRequestId)
            .then(checkForAlreadyApplied)
            .then(function addChange(configRequest) {
                if (!configRequest.changedAfterValidation)
                    configRequest.changedAfterValidation = true;

                if (params[0]) {
                    let request = params[0];
                    var dataType = request.params.type;
                    var action = request.params.action;
                    var category = request.query.category || dataType;
                    var data = request.body;
                    var entityId = request.params.id;
                    var entity = makeEntityToChange(data, entityId, configRequestId, updater);
                    params.shift();

                    return Promise.resolve(configRequest)
                        .then(handleNewChange(dataType, category, action, entity, isDeleteChange))
                        .then(isMigratable) // catch for migration errors, call ?addChange?
                        .then(diffChange(client, data, dataType, entityId, category, action, isDeleteChange))
                        .then(addChange);
                }
                else
                    return configRequest;
            })
            .then(saveConfigRequest(client, updater, "POST"));
    }

    function main(){
        parseData(req.body)
            .then(changeChanges)
            .then((res)=> {
                res.errors = errors;
                resp.send(res);
            })
            .catch(done);
    }

    if (req.files[FILE_KEY]) {
        let path = req.files[FILE_KEY].path;
        fs.readFile(path, {encoding: 'utf-8'}, function loadFromFile(err, data) {
            if (!err) {
                try {
                    req.body = JSON.parse(data);
                    fs.unlink(path);
                } catch (e) {
                    fs.unlink(path);
                    done(e);
                    return;
                }
                main();
            } else {
                done(err);
            }
        });
    } else {
        main();
    }

}

function createImportProtocolPdfFromJson(json, targetPath) {
    return new Promise((resolve, reject)=> {
        try {
            assert(json);
            json = JSON.stringify(json);
            var requestJson = {
                "data": [
                    {
                        "type": "text",
                        "name": "cr",
                        "text": "" + json
                    }
                ],
                "template_name": config.PDF_GENERATOR_TEMPLATE_NAME,
                "lang":"de"
            };
            var options = {
                url: config.PDF_GENERATOR_ENDPOINT,
                headers: {'Content-type': 'application/json'},
                body: JSON.stringify(requestJson),
                rejectUnauthorized: false,
                checkServerIdentity: function (host, cert) {
                    return true;
                }
            };
            request.post(options).pipe(fs.createWriteStream(targetPath)).on('close', resolve);
        } catch (err) {
            return reject(err);
        }
    });
}

function createOrderXML(json, targetPath) {
    return new Promise((resolve, reject)=> {
            fs.readFile(ORDER_TEMPLATE_LOCATION, "utf8", function (error, data) {
                if (error)
                    reject(error);
                else {
                    data = data.replace('$description', json.description);
                    data = data.replace('$name', json.name);

                    fs.writeFile(targetPath, data, function (error) {
                        error ? reject(error) : resolve();
                    });
                }
            });
        }
    );
}

function sendFilesPromise(targetPath, pathToFiles) {
    return new Promise((resolve, reject)=> {
            sendFiles(config.sftpConfig, targetPath, pathToFiles, function (err) {
                if (err)
                    reject(err);
                else
                    resolve();
            });
        }
    );
}

function getExportFile(targetPath, client){
    return new Promise((resolve, reject)=>{
        var options = {
            url: config.REPORT_SERVICE_ENDPOINT + "/json/" + client,
            rejectUnauthorized: false,
            checkServerIdentity: function (host, cert) {
                return true;
            }
        };
        console.log(options.url);
        var stream = request.get(options).pipe(fs.createWriteStream(targetPath));
        stream.on('close', resolve);
        stream.on('error', reject);
    });
}

function createReport(req, resp, next) {

    var getChannel = function (db, callback) {
        db.collection('default.general').find({_id: ",general,configRequestReportChannel,"})
            .next((error, doc)=> {
                doc ? callback(error, doc.data.channel) : callback(error, false)
            });
    };

    var client = req.query.client;
    var configRequestId = req.params.crId;

    loader.loadConfigRequest(client, configRequestId)
        .then(function (json) {
            MongoClient.connect(config.MONGO_CONNECTION_STRING, function (err, db) {
                if (err) {
                    next(err);
                    return;
                }
                getChannel(db, function (error, channel) {

                    if (!channel || error) {
                        var message = error ? error : "Channel for CR reports not found.";
                        console.error(message);
                        resp.status(500).send({success: false, message: message});
                        return;
                    }

                    var options = {
                        url: config.CR_REPORT_CHANNEL_URL + channel + '?client=' + client,
                        rejectUnauthorized: false,
                        checkServerIdentity: function (host, cert) {
                            return true;
                        }
                    };

                    request(options, function (err, response, body) {
                        if (err) {
                            next(err);
                            return;
                        }
                        else if (!response || response.statusCode !== 200) {
                            console.error(new Error('Cannot get channel path.'));
                            resp.status(500).send({success: false, message: 'Cannot get channel path.'});
                            return;
                        }
                        var path = JSON.parse(body)['import.path'];

                        fs.mkdir(config.CR_REPORT_DIR_NAME, function () {
                            var jsonDir = config.CR_REPORT_DIR_NAME + '/' + json.name;
                            fs.mkdir(jsonDir, function () {
                                createImportProtocolPdfFromJson(json, jsonDir + '/report.pdf')
                                    .then(writeFile(jsonDir + '/config-request.json', JSON.stringify(json, null, 4)))
                                    .then(getExportFile.bind(this, jsonDir + "/configurationExportFile.json", client))
                                    .then(createOrderXML.bind(this, json, jsonDir + '/order.xml'))
                                    .then(sendFilesPromise.bind(this, jsonDir, path + '/' + json.name))
                                    .then(function () {
                                        resp.send({success: true});
                                    })
                                    .catch(error=> {
                                        console.warn(error);
                                        resp.status(500).send({success: false, message: error.message});
                                    });
                            });
                        });
                    });
                });
            });
        })
        .catch(function (err) {
            next(err);
        });
}

function getPendingChanges(req, resp, next) {
    let fields = ["applied", "invalid", "lastUpdater", "changes", "timestamp", "applyingDate", "toMigrate", "description", "name"];
    let client = req.query.client;
    let type = req.params.type;
    let id = req.params.id;
    let category = req.query.category || type;

    loader.loadConfigRequests(client, fields.join(","))
        .then(function (configRequests) {
            let notApplied = [];
            if (configRequests) {
                notApplied = configRequests.filter(function (cr) {
                    if (!cr.applied && cr.changes && cr.changes[type] && cr.changes[type][category]) {
                        for (let action in cr.changes[type][category]) {
                            if (action) {
                                for (let i in cr.changes[type][category][action]) {
                                    if (cr.changes[type][category][action][i] && cr.changes[type][category][action][i].name == id) {
                                        cr.action = action;
                                        delete cr.changes;
                                        return cr;
                                    }
                                }
                            }
                        }
                    }
                });
            }
            resp.send(notApplied);
        })
        .catch(next);
}

function getCrDifference(client, configRequestId) {
    return loader.loadConfigRequest(client, configRequestId)
        .then(configRequest=> {
            return configRequest.applied !== true ?
                diffConfigRequest(client)(configRequest)
                    .then(cr=>cr.diff) :
                configRequest.diff;
        });
}
exports.deleteConfigRequest = deleteConfigRequest;
exports.addConfigRequest = addConfigRequest;
exports.modifyChanges = modifyChanges;
exports.updateConfigRequest = updateConfigRequest;
exports.addJSONConfigRequest = addJSONConfigRequest;
exports.createReport = createReport;
exports.getPendingChanges = getPendingChanges;
exports.getCrDifference = getCrDifference;

